"""
Kopf handlers for Windows services management
"""


import kopf
import logging
import subprocess
import os
import yaml
from datetime import datetime
from kubernetes import client
from kubernetes.client.rest import ApiException
import queue

# Global log queue for TUI
try:
    from modules.tui_interface import log_queue
except ImportError:
    log_queue = None

logger = logging.getLogger(__name__)

# Set up operator file logger
operator_log_path = "/tmp/operator.log"
operator_file_handler = logging.FileHandler(operator_log_path, mode='a')
operator_file_handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
operator_file_handler.setLevel(logging.INFO)
logger.addHandler(operator_file_handler)

# Optionally suppress noisy Kopf inconsistency logs (set env KOPF_SUPPRESS_INCONSISTENCIES=1)
try:
    if os.getenv('KOPF_SUPPRESS_INCONSISTENCIES', '0') == '1':
        for name in ('kopf.objects', 'kopf.activities', 'kopf.clients.patching'):
            logging.getLogger(name).setLevel(logging.WARNING)
except Exception:
    pass

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


# Configure Kopf persistence to reduce status conflicts
@kopf.on.startup()
def configure_kopf(settings: kopf.OperatorSettings, **_):
    try:
        # Move Kopf's internal progress/diffbase storage to annotations to avoid touching .status
        settings.persistence.progress_storage = kopf.AnnotationsProgressStorage(prefix='kopf.windowsvm.dev')
        settings.persistence.diffbase_storage = kopf.AnnotationsDiffBaseStorage(prefix='kopf.windowsvm.dev')
        # Keep posting/info defaults; adjust if you want quieter logs
        logger.info("[OPERATOR] Kopf persistence configured to use annotations for progress/diffbase")
    except Exception as e:
        logger.warning(f"[OPERATOR] Failed to configure Kopf persistence: {e}")

# WindowsVM Handlers
@kopf.on.create('infra.example.com', 'v1', 'windowsvms')
@kopf.on.update('infra.example.com', 'v1', 'windowsvms')
def handle_windowsvm(body, meta, spec, status, namespace, diff, old, new, patch, **kwargs):
    # Guard: skip if already terminal phase
    terminal_phases = ['Ready', 'Failed', 'Skipped']
    if status and status.get('phase') in terminal_phases and status.get('observedGeneration') == meta.get('generation'):
        msg = f"[OPERATOR] Skipping execution for {meta.get('name')} (phase={status.get('phase')})"
        logger.info(msg)
        if log_queue:
            log_queue.put(msg)
        patch.status['phase'] = status.get('phase')
        patch.status['message'] = status.get('message', '')
        patch.status['observedGeneration'] = status.get('observedGeneration')
        return
    logger.info("[OPERATOR] handle_windowsvm triggered!")
    if log_queue:
        log_queue.put("[OPERATOR] handle_windowsvm triggered!")
    name = meta.get('name')
    action = spec.get('action', 'install')
    # Always log and run uninstall if action changed to uninstall
    if diff:
        for d in diff:
            if d[1] == ('spec', 'action'):
                logger.info(f"[OPERATOR] Detected spec.action change: {d}")
                if log_queue:
                    log_queue.put(f"[OPERATOR] Detected spec.action change: {d}")
    vm_name = spec.get('vmName', name)
    logger.info(f"[OPERATOR] CR received: name={name}, action={action}, vm_name={vm_name}")
    if log_queue:
        log_queue.put(f"[OPERATOR] CR received: name={name}, action={action}, vm_name={vm_name}")
    # Mark as InProgress at the beginning of processing
    try:
        patch.status['phase'] = 'InProgress'
        patch.status['message'] = f"{action.title()} in progress for VM {vm_name}"
        patch.status['reason'] = 'Processing'
        patch.status['observedGeneration'] = meta.get('generation')
        now = datetime.utcnow().isoformat() + 'Z'
        cond = {
            'type': 'Ready',
            'status': 'False',
            'reason': 'Processing',
            'message': f"{action.title()} in progress for VM {vm_name}",
            'lastTransitionTime': now,
        }
        existing = status.get('conditions', []) if status else []
        patch.status['conditions'] = [c for c in existing if c.get('type') != 'Ready'] + [cond]
    except Exception:
        pass
    import time
    max_retries = 5
    retry_delay = 1  # seconds
    try:
        logger.info(f"[OPERATOR] Deciding what to do for action={action} on VM {vm_name}")
        if log_queue:
            log_queue.put(f"[OPERATOR] Deciding what to do for action={action} on VM {vm_name}")
        kopf.info(body, reason='Processing', message=f'Starting {action} for VM {vm_name}')
        logger.info(f"[OPERATOR] Starting {action} for VM {vm_name}")
        if log_queue:
            log_queue.put(f"[OPERATOR] Starting {action} for VM {vm_name}")
        playbook_path = "/root/kubernetes-installer/windows-server-controller.yaml"
        # Collect all relevant variables from spec for playbook
        playbook_vars = {
            'action': action,
            'vm_name': vm_name,
            'windows_version': spec.get('windows_version', '2025'),
            'kubevirt_namespace': spec.get('kubevirt_namespace', namespace),
            'storage_dir': spec.get('storage_dir', '/var/lib/kubevirt'),
            'system_disk_size': spec.get('system_disk_size', '40Gi'),
            'vhdx_path': spec.get('vhdx_path', '/data/vms/win2025server.vhdx'),
            'virtio_iso_size': spec.get('virtio_iso_size', '500Mi'),
            'vm_cpu_cores': spec.get('vm_cpu_cores', 4),
            'vm_memory': spec.get('vm_memory', '8Gi'),
            'windows_admin_password': spec.get('windows_admin_password', 'Secret123%%'),
            'windows_product_key': spec.get('windows_product_key', ''),
            'image': spec.get('image', 'win2025server.vhdx'),
            'installer_disk_size': spec.get('installer_disk_size', '15Gi'),
        }
        if action == 'install':
            logger.info(f"[OPERATOR] Running Ansible playbook for install on VM {vm_name}")
            if log_queue:
                log_queue.put(f"[OPERATOR] Running Ansible playbook for install on VM {vm_name}")
            result = run_ansible_playbook(playbook_path, playbook_vars)
        elif action == 'uninstall':
            logger.info(f"[OPERATOR] Running Ansible playbook for uninstall on VM {vm_name}")
            if log_queue:
                log_queue.put(f"[OPERATOR] Running Ansible playbook for uninstall on VM {vm_name}")
            result = run_ansible_playbook(playbook_path, playbook_vars)
        else:
            logger.info(f"[OPERATOR] Unknown action: {action}, skipping.")
            if log_queue:
                log_queue.put(f"[OPERATOR] Unknown action: {action}, skipping.")
            return {'phase': 'Skipped', 'message': f'Unknown action: {action}'}

        # Kopf expects a dict with top-level status keys to patch .status
        if result['success']:
            logger.info(f"[OPERATOR] Playbook succeeded for {action} on VM {vm_name}")
            if log_queue:
                log_queue.put(f"[OPERATOR] Playbook succeeded for {action} on VM {vm_name}")
            if result.get('output'):
                logger.info(f"[OPERATOR] Playbook output:\n{result['output']}")
                if log_queue:
                    for line in result['output'].splitlines():
                        log_queue.put(f"[PLAYBOOK] {line}")
            patch.status['phase'] = 'Ready'
            patch.status['message'] = f"VM {vm_name} {action} completed successfully"
            patch.status['reason'] = 'Completed'
            patch.status['observedGeneration'] = meta.get('generation')
            now = datetime.utcnow().isoformat() + 'Z'
            cond = {
                'type': 'Ready',
                'status': 'True',
                'reason': 'Completed',
                'message': f"VM {vm_name} {action} completed successfully",
                'lastTransitionTime': now,
            }
            existing = status.get('conditions', []) if status else []
            patch.status['conditions'] = [c for c in existing if c.get('type') != 'Ready'] + [cond]
            return
        else:
            logger.info(f"[OPERATOR] Playbook failed for {action} on VM {vm_name}: {result['error']}")
            if log_queue:
                log_queue.put(f"[OPERATOR] Playbook failed for {action} on VM {vm_name}: {result['error']}")
            if result.get('output'):
                logger.info(f"[OPERATOR] Playbook output:\n{result['output']}")
                if log_queue:
                    for line in result['output'].splitlines():
                        log_queue.put(f"[PLAYBOOK] {line}")
            patch.status['phase'] = 'Failed'
            patch.status['message'] = f"Failed to {action} VM: {result['error']}"
            patch.status['reason'] = 'Error'
            patch.status['observedGeneration'] = meta.get('generation')
            now = datetime.utcnow().isoformat() + 'Z'
            cond = {
                'type': 'Ready',
                'status': 'False',
                'reason': 'Error',
                'message': f"Failed to {action} VM: {result['error']}",
                'lastTransitionTime': now,
            }
            existing = status.get('conditions', []) if status else []
            patch.status['conditions'] = [c for c in existing if c.get('type') != 'Ready'] + [cond]
            return
    except Exception as e:
        error_msg = f"[OPERATOR] Error processing WindowsVM {name}: {e}"
        logger.error(error_msg)
        if log_queue:
            log_queue.put(error_msg)
        try:
            kopf.exception(body, reason='Error', message=error_msg)
        except Exception as patch_err:
            logger.warning(f"[OPERATOR] Failed to patch CR status due to: {patch_err}")
            if log_queue:
                log_queue.put(f"[OPERATOR] Failed to patch CR status due to: {patch_err}")
        patch.status['phase'] = 'Failed'
        patch.status['message'] = error_msg
        patch.status['reason'] = 'Exception'
        patch.status['observedGeneration'] = meta.get('generation')
        return


# Resume handler to refresh status after operator restarts
@kopf.on.resume('infra.example.com', 'v1', 'windowsvms')
def resume_windowsvm(body, meta, spec, status, namespace, patch, **kwargs):
    name = meta.get('name')
    vm_name = spec.get('vmName', name)
    vm_ns = spec.get('kubevirt_namespace', namespace)
    try:
        st = check_target_vm_status(vm_name, vm_ns)
        now = datetime.utcnow().isoformat() + 'Z'
        if st['ready']:
            patch.status['phase'] = 'Ready'
            patch.status['message'] = f"VM {vm_name} is running ({st['message']})"
            patch.status['reason'] = 'Resumed'
            patch.status['observedGeneration'] = meta.get('generation')
            cond = {
                'type': 'Ready', 'status': 'True', 'reason': 'Resumed',
                'message': patch.status['message'], 'lastTransitionTime': now,
            }
        else:
            patch.status['phase'] = 'Pending'
            patch.status['message'] = st['message']
            patch.status['reason'] = 'Resumed'
            patch.status['observedGeneration'] = meta.get('generation')
            cond = {
                'type': 'Ready', 'status': 'False', 'reason': 'Resumed',
                'message': st['message'], 'lastTransitionTime': now,
            }
        existing = status.get('conditions', []) if status else []
        patch.status['conditions'] = [c for c in existing if c.get('type') != 'Ready'] + [cond]
    except Exception as e:
        patch.status['phase'] = 'Unknown'
        patch.status['message'] = f"Error on resume: {e}"
        patch.status['reason'] = 'Exception'
        patch.status['observedGeneration'] = meta.get('generation')


# Delete handler to mark terminating status
@kopf.on.delete('infra.example.com', 'v1', 'windowsvms')
def delete_windowsvm(body, meta, spec, status, namespace, patch, **kwargs):
    name = meta.get('name')
    vm_name = spec.get('vmName', name)
    patch.status['phase'] = 'Terminating'
    patch.status['message'] = f"Delete requested for VM {vm_name}"
    patch.status['reason'] = 'DeleteRequested'
    patch.status['observedGeneration'] = meta.get('generation')

    # Run uninstall playbook
    playbook_path = "/root/kubernetes-installer/windows-server-controller.yaml"
    logger.info(f"[OPERATOR] Running uninstall playbook for VM {vm_name}")
    if log_queue:
        log_queue.put(f"[OPERATOR] Running uninstall playbook for VM {vm_name}")
    result = run_ansible_playbook(playbook_path, {
        'action': 'uninstall',
        'vm_name': vm_name,
        'kubevirt_namespace': namespace
    })
    if result['success']:
        logger.info(f"[OPERATOR] Uninstall playbook completed for VM {vm_name}")
        if log_queue:
            log_queue.put(f"[OPERATOR] Uninstall playbook completed for VM {vm_name}")
    else:
        logger.error(f"[OPERATOR] Uninstall playbook failed for VM {vm_name}: {result.get('error')}")
        if log_queue:
            log_queue.put(f"[OPERATOR] Uninstall playbook failed for VM {vm_name}: {result.get('error')}")

# MSSQLServer Handlers
@kopf.on.create('infra.example.com', 'v1', 'mssqlservers')
@kopf.on.update('infra.example.com', 'v1', 'mssqlservers')
def handle_mssqlserver(body, meta, spec, status, namespace, **kwargs):
    """Handle MSSQLServer resource changes"""
    name = meta.get('name')
    target_vm = spec['targetVM']['vmName']
    enabled = spec.get('enabled', True)
    msg = f"Processing MSSQLServer {name}: target_vm={target_vm}, enabled={enabled}"
    logger.info(msg)
    try:
        kopf.info(body, reason='Processing', message=f'Starting MSSQL installation on VM {target_vm}')
        logger.info(f"Operator: Starting MSSQL installation on VM {target_vm}")
        if not enabled:
            logger.info(f"MSSQLServer {name} is disabled, skipping playbook run.")
            return
        # Use the namespace from the CR spec or fallback to the resource namespace
        vm_ns = spec['targetVM'].get('namespace', namespace)
        vm_status = check_target_vm_status(target_vm, vm_ns)
        if not vm_status['ready']:
            logger.info(f"Target VM {target_vm} is not ready: {vm_status['message']}. Skipping playbook run.")
            return
        # Run the appropriate Ansible playbook
        playbook_path = "/root/kubernetes-installer/windows-server-controller.yaml"
        logger.info(f"Operator: Running Ansible playbook for MSSQL install on VM {target_vm}")
        result = run_ansible_playbook(playbook_path, {
            'action': 'install',
            'vm_name': target_vm,
            'kubevirt_namespace': vm_ns
        })
        if result['success']:
            logger.info(f"Operator: Successfully installed MSSQL on VM {target_vm}")
            if result.get('output'):
                logger.info(f"Playbook output:\n{result['output']}")
            return {'phase': 'Ready', 'message': f'MSSQL install completed successfully on {target_vm}'}
        else:
            logger.info(f"Operator: Failed to install MSSQL on VM {target_vm}: {result['error']}")
            if result.get('output'):
                logger.info(f"Playbook output:\n{result['output']}")
            return {'phase': 'Failed', 'message': f"Failed to install MSSQL: {result['error']}"}
    except Exception as e:
        error_msg = f"Error processing MSSQLServer {name}: {e}"
        logger.error(error_msg)
        logger.info(error_msg)
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
    msg = f"Processing OTelCollector {name}: target_vm={target_vm}, metrics_type={metrics_type}, enabled={enabled}"
    logger.info(msg)
    try:
        kopf.info(body, reason='Processing', message=f'Starting OpenTelemetry Collector installation on VM {target_vm}')
        logger.info(f"Operator: Starting OpenTelemetry Collector installation on VM {target_vm}")
        if not enabled:
            logger.info(f"OTelCollector {name} is disabled, skipping playbook run.")
            return
        # Use the namespace from the CR spec or fallback to the resource namespace
        vm_ns = spec['targetVM'].get('namespace', namespace)
        vm_status = check_target_vm_status(target_vm, vm_ns)
        if not vm_status['ready']:
            logger.info(f"Target VM {target_vm} is not ready: {vm_status['message']}. Skipping playbook run.")
            return

        # Check MSSQL prerequisite if collecting MSSQL metrics
        if 'mssql' in metrics_type and spec.get('prerequisites', {}).get('requireMSSQLForMetrics', True):
            mssql_status = check_mssql_availability(target_vm)
            if not mssql_status['available']:
                logger.info(f"MSSQL is required for metrics type '{metrics_type}' but not available on VM {target_vm}. Skipping playbook run.")
                return
        
        # Run the appropriate Ansible playbook
        playbook_path = "/root/kubernetes-installer/windows-server-controller.yaml"
        logger.info(f"Operator: Running Ansible playbook for OTelCollector install on VM {target_vm}")
        result = run_ansible_playbook(playbook_path, {
            'action': 'install',
            'vm_name': target_vm,
            'kubevirt_namespace': vm_ns
        })
        if result['success']:
            logger.info(f"Operator: Successfully installed OTelCollector on VM {target_vm}")
            if result.get('output'):
                logger.info(f"Playbook output:\n{result['output']}")
            return {'phase': 'Ready', 'message': f'OTelCollector install completed successfully on {target_vm}'}
        else:
            logger.info(f"Operator: Failed to install OTelCollector on VM {target_vm}: {result['error']}")
            if result.get('output'):
                logger.info(f"Playbook output:\n{result['output']}")
            return {'phase': 'Failed', 'message': f"Failed to install OTelCollector: {result['error']}"}
    except Exception as e:
        error_msg = f"Error processing OTelCollector {name}: {e}"
        logger.error(error_msg)
        logger.info(error_msg)
        kopf.exception(body, reason='Error', message=error_msg)
        return {'phase': 'Failed', 'message': error_msg}

def run_ansible_playbook(playbook_path, variables):
    """Run Ansible playbook with given variables and stream output line by line"""
    import shlex
    try:
        # Use log_queue for streaming output if available
        # Create temporary inventory
        inventory_content = "localhost ansible_connection=local\n"
        with open('/tmp/ansible_inventory', 'w') as f:
            f.write(inventory_content)
        # Build ansible-playbook command
        cmd = ['ansible-playbook', '-i', '/tmp/ansible_inventory', playbook_path]
        for key, value in variables.items():
            cmd.extend(['--extra-vars', f'{key}={value}'])
        logger.info(f"[OPERATOR] Running command: {' '.join(shlex.quote(str(c)) for c in cmd)}")
        if log_queue:
            log_queue.put(f"[OPERATOR] Running command: {' '.join(shlex.quote(str(c)) for c in cmd)}")
        # Run the playbook and stream output
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        output_lines = []
        playbook_completed = False
        for line in process.stdout:
            line = line.rstrip()
            logger.info(f"[PLAYBOOK] {line}")  # Stream to console in real time
            if log_queue:
                log_queue.put(f"[PLAYBOOK] {line}")
            output_lines.append(line)
            # Detect playbook completion by looking for the final task and PLAY RECAP
            if 'TASK [Display completion message]' in line or 'PLAY RECAP' in line:
                playbook_completed = True
                if 'PLAY RECAP' in line:
                    logger.info("[PLAYBOOK] --- End of playbook execution detected ---")
                    if log_queue:
                        log_queue.put("[PLAYBOOK] --- End of playbook execution detected ---")
        if playbook_completed and log_queue:
            log_queue.put("[PLAYBOOK] Playbook execution has completed. Check above for summary.")
        process.wait()
        if process.returncode == 0:
            logger.info("[OPERATOR] Ansible playbook completed successfully")
            if log_queue:
                log_queue.put("[OPERATOR] Ansible playbook completed successfully")
            return {'success': True, 'output': '\n'.join(output_lines)}
        else:
            logger.error(f"[OPERATOR] Ansible playbook failed with code {process.returncode}")
            if log_queue:
                log_queue.put(f"[OPERATOR] Ansible playbook failed with code {process.returncode}")
            return {'success': False, 'error': f'Playbook failed with code {process.returncode}', 'output': '\n'.join(output_lines)}
    except subprocess.TimeoutExpired:
        error_msg = "[OPERATOR] Ansible playbook timed out after 30 minutes"
        logger.error(error_msg)
        if log_queue:
            log_queue.put(error_msg)
        return {'success': False, 'error': error_msg}
    except Exception as e:
        error_msg = f"[OPERATOR] Error running Ansible playbook: {e}"
        logger.error(error_msg)
        if log_queue:
            log_queue.put(error_msg)
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
