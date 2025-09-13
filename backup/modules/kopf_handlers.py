"""
Kopf handlers for Windows services management
"""

import kopf
import logging
import subprocess
import os
import yaml
from kubernetes import client
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)

# Resource definitions
RESOURCES = {
    'windowsvm': {
        'group': 'infra.example.com',
        'version': 'v1',
        'plural': 'windowsvms'
    },
    'mssqlserver': {
        'group': 'infra.example.com', 
        'version': 'v1',
        'plural': 'mssqlservers'
    },
    'otelcollector': {
        'group': 'infra.example.com',
        'version': 'v1', 
        'plural': 'otelcollectors'
    }
}

def setup_kopf_handlers():
    """Set up all Kopf handlers for different resource types"""
    logger.info("Setting up Kopf handlers for Windows services...")

# WindowsVM Handlers
@kopf.on.create('infra.example.com', 'v1', 'windowsvms')
@kopf.on.update('infra.example.com', 'v1', 'windowsvms')
def handle_windowsvm(body, meta, spec, status, namespace, **kwargs):
    """Handle WindowsVM resource changes"""
    name = meta.get('name')
    action = spec.get('action', 'install')
    vm_name = spec.get('vmName', name)
    
    logger.info(f"Processing WindowsVM {name}: action={action}, vm={vm_name}")
    
    try:
        # Update status to Processing
        kopf.info(body, reason='Processing', message=f'Starting {action} for VM {vm_name}')
        
        # Run the appropriate Ansible playbook
        playbook_path = "/root/kubernetes-installer/windows-server-controller.yaml"
        
        if action == 'install':
            result = run_ansible_playbook(playbook_path, {
                'action': 'install',
                'vm_name': vm_name,
                'iso_file': spec.get('isoFile', '/root/kubernetes-installer/win2025server.vhdx'),
                'disk_size': spec.get('diskSize', '100Gi'),
                'memory': spec.get('memory', '8Gi'),
                'cpu_cores': spec.get('cpuCores', 4)
            })
        elif action == 'uninstall':
            result = run_ansible_playbook(playbook_path, {
                'action': 'uninstall',
                'vm_name': vm_name
            })
        else:
            raise ValueError(f"Unknown action: {action}")
        
        if result['success']:
            kopf.info(body, reason='Success', message=f'Successfully {action}ed VM {vm_name}')
            return {'phase': 'Ready', 'message': f'VM {vm_name} {action} completed successfully'}
        else:
            kopf.warn(body, reason='Failed', message=f'Failed to {action} VM {vm_name}: {result["error"]}')
            return {'phase': 'Failed', 'message': f'Failed to {action} VM: {result["error"]}'}
            
    except Exception as e:
        error_msg = f"Error processing WindowsVM {name}: {e}"
        logger.error(error_msg)
        kopf.exception(body, reason='Error', message=error_msg)
        return {'phase': 'Failed', 'message': error_msg}

# MSSQLServer Handlers
@kopf.on.create('infra.example.com', 'v1', 'mssqlservers')
@kopf.on.update('infra.example.com', 'v1', 'mssqlservers')
def handle_mssqlserver(body, meta, spec, status, namespace, **kwargs):
    """Handle MSSQLServer resource changes"""
    name = meta.get('name')
    target_vm = spec['targetVM']['vmName']
    enabled = spec.get('enabled', True)
    
    logger.info(f"Processing MSSQLServer {name}: target_vm={target_vm}, enabled={enabled}")
    
    try:
        # Update status to Processing
        kopf.info(body, reason='Processing', message=f'Starting MSSQL installation on VM {target_vm}')
        
        if not enabled:
            kopf.info(body, reason='Disabled', message='MSSQL installation is disabled')
            return {'phase': 'Disabled', 'message': 'MSSQL installation is disabled'}
        
        # Check if target VM exists and is running
        vm_status = check_target_vm_status(target_vm, spec['targetVM'].get('namespace', 'kubevirt'))
        if not vm_status['ready']:
            error_msg = f"Target VM {target_vm} is not ready: {vm_status['message']}"
            kopf.warn(body, reason='VMNotReady', message=error_msg)
            return {'phase': 'Failed', 'message': error_msg}
        
        # Generate vault path based on VM name
        admin_vault_path = spec.get('credentials', {}).get('adminPasswordVaultPath', f"secret/data/{target_vm}/admin")
        sa_vault_path = spec.get('credentials', {}).get('saPasswordVaultPath', f"secret/data/{target_vm}/admin")
        
        # Run Windows automation playbook for MSSQL installation
        playbook_path = "/root/kubernetes-installer/windows-automation-controller.yaml"
        result = run_ansible_playbook(playbook_path, {
            'vm_name': target_vm,
            'install': 'mssql',
            'kubevirt_namespace': spec['targetVM'].get('namespace', 'kubevirt'),
            'admin_vault_path': admin_vault_path,
            'sa_vault_path': sa_vault_path
        })
        
        if result['success']:
            kopf.info(body, reason='Success', message=f'Successfully installed MSSQL on VM {target_vm}')
            return {
                'phase': 'Ready',
                'message': f'MSSQL Server {spec.get("version", "2025")} installed successfully',
                'serviceStatus': 'Running',
                'installedVersion': spec.get('version', '2025'),
                'installPath': spec.get('installPath', 'C:\\Data')
            }
        else:
            kopf.warn(body, reason='Failed', message=f'Failed to install MSSQL on VM {target_vm}: {result["error"]}')
            return {'phase': 'Failed', 'message': f'MSSQL installation failed: {result["error"]}'}
            
    except Exception as e:
        error_msg = f"Error processing MSSQLServer {name}: {e}"
        logger.error(error_msg)
        kopf.exception(body, reason='Error', message=error_msg)
        return {'phase': 'Failed', 'message': error_msg}

# OTelCollector Handlers
@kopf.on.create('infra.example.com', 'v1', 'otelcollectors')
@kopf.on.update('infra.example.com', 'v1', 'otelcollectors')
def handle_otelcollector(body, meta, spec, status, namespace, **kwargs):
    """Handle OTelCollector resource changes"""
    name = meta.get('name')
    target_vm = spec['targetVM']['vmName']
    enabled = spec.get('enabled', True)
    metrics_type = spec.get('metricsType', 'os')
    
    logger.info(f"Processing OTelCollector {name}: target_vm={target_vm}, metrics_type={metrics_type}, enabled={enabled}")
    
    try:
        # Update status to Processing
        kopf.info(body, reason='Processing', message=f'Starting OpenTelemetry Collector installation on VM {target_vm}')
        
        if not enabled:
            kopf.info(body, reason='Disabled', message='OpenTelemetry Collector installation is disabled')
            return {'phase': 'Disabled', 'message': 'OpenTelemetry Collector installation is disabled'}
        
        # Check if target VM exists and is running
        vm_status = check_target_vm_status(target_vm, spec['targetVM'].get('namespace', 'kubevirt'))
        if not vm_status['ready']:
            error_msg = f"Target VM {target_vm} is not ready: {vm_status['message']}"
            kopf.warn(body, reason='VMNotReady', message=error_msg)
            return {'phase': 'Failed', 'message': error_msg}
        
        # Check MSSQL prerequisite if collecting MSSQL metrics
        if 'mssql' in metrics_type and spec.get('prerequisites', {}).get('requireMSSQLForMetrics', True):
            mssql_status = check_mssql_availability(target_vm)
            if not mssql_status['available']:
                error_msg = f"MSSQL is required for metrics type '{metrics_type}' but not available on VM {target_vm}"
                kopf.warn(body, reason='MSSQLNotAvailable', message=error_msg)
                return {'phase': 'Failed', 'message': error_msg}
        
        # Generate vault path based on VM name
        admin_vault_path = spec.get('credentials', {}).get('adminPasswordVaultPath', f"secret/data/{target_vm}/admin")
        
        # Run Windows automation playbook for OpenTelemetry installation
        playbook_path = "/root/kubernetes-installer/windows-automation-controller.yaml"
        result = run_ansible_playbook(playbook_path, {
            'vm_name': target_vm,
            'otel_config': metrics_type,
            'otel_endpoint': spec['endpoint'],
            'otel_token': spec['token'],
            'kubevirt_namespace': spec['targetVM'].get('namespace', 'kubevirt'),
            'admin_vault_path': admin_vault_path
        })
        
        if result['success']:
            kopf.info(body, reason='Success', message=f'Successfully installed OpenTelemetry Collector on VM {target_vm}')
            return {
                'phase': 'Ready',
                'message': f'OpenTelemetry Collector installed successfully with {metrics_type} metrics',
                'collectorStatus': {'running': True, 'runningAs': 'service'},
                'installedVersion': spec.get('collectorVersion', '0.133.0'),
                'configType': metrics_type
            }
        else:
            kopf.warn(body, reason='Failed', message=f'Failed to install OpenTelemetry Collector on VM {target_vm}: {result["error"]}')
            return {'phase': 'Failed', 'message': f'OpenTelemetry Collector installation failed: {result["error"]}'}
            
    except Exception as e:
        error_msg = f"Error processing OTelCollector {name}: {e}"
        logger.error(error_msg)
        kopf.exception(body, reason='Error', message=error_msg)
        return {'phase': 'Failed', 'message': error_msg}

def run_ansible_playbook(playbook_path, variables):
    """Run Ansible playbook with given variables"""
    try:
        # Create temporary inventory
        inventory_content = "localhost ansible_connection=local\n"
        with open('/tmp/ansible_inventory', 'w') as f:
            f.write(inventory_content)
        
        # Build ansible-playbook command
        cmd = ['ansible-playbook', '-i', '/tmp/ansible_inventory', playbook_path]
        
        # Add extra variables
        for key, value in variables.items():
            cmd.extend(['--extra-vars', f'{key}={value}'])
        
        logger.info(f"Running command: {' '.join(cmd)}")
        
        # Run the playbook
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes timeout
        )
        
        if result.returncode == 0:
            logger.info("Ansible playbook completed successfully")
            return {'success': True, 'output': result.stdout}
        else:
            logger.error(f"Ansible playbook failed: {result.stderr}")
            return {'success': False, 'error': result.stderr, 'output': result.stdout}
            
    except subprocess.TimeoutExpired:
        error_msg = "Ansible playbook timed out after 30 minutes"
        logger.error(error_msg)
        return {'success': False, 'error': error_msg}
    except Exception as e:
        error_msg = f"Error running Ansible playbook: {e}"
        logger.error(error_msg)
        return {'success': False, 'error': error_msg}

def check_target_vm_status(vm_name, kubevirt_namespace):
    """Check if target VM is ready for service installation"""
    try:
        from utils.k8s_client import get_vm_status
        vm_status = get_vm_status(vm_name, kubevirt_namespace)
        
        if not vm_status['exists']:
            return {'ready': False, 'message': f'VM {vm_name} does not exist in namespace {kubevirt_namespace}'}
        
        if not vm_status['is_running']:
            return {'ready': False, 'message': f'VM {vm_name} is not running (phase: {vm_status["vmi_phase"]})'}
        
        return {'ready': True, 'message': f'VM {vm_name} is ready (phase: {vm_status["vmi_phase"]})'}
        
    except Exception as e:
        return {'ready': False, 'message': f'Error checking VM status: {e}'}

def check_mssql_availability(vm_name):
    """Check if MSSQL is available on the target VM"""
    try:
        # This is a placeholder - in reality, you would check if MSSQL service is running
        # For now, we'll assume it's available if we can't determine otherwise
        return {'available': True, 'message': 'MSSQL availability check not implemented'}
    except Exception as e:
        return {'available': False, 'message': f'Error checking MSSQL availability: {e}'}
