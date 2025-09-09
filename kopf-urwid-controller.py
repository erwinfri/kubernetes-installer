#!/usr/bin/env python3
"""
Hybrid Kopf + urwid TUI for WindowsVM Management
Combines the automatic Kopf operator with a visual TUI interface
"""

import os
import kopf
import yaml
import subprocess
import asyncio
import logging
import threading
import time
import queue
import signal
import urwid
from datetime import datetime
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# --- Configuration ---
CRD_GROUP = "infra.example.com"
CRD_VERSION = "v1"
CRD_PLURAL = "windowsvms"
CRD_KIND = "WindowsVM"
PLAYBOOK_PATH = "/root/kubernetes-installer/windows-server-controller.yaml"
MANIFEST_CONTROLLER_DIR = "/root/kubernetes-installer/manifest-controller"

# Global log queue for TUI
log_queue = queue.Queue()
tui_app = None

# --- Custom Log Handler ---
class TUILogHandler(logging.Handler):
    """Custom log handler that sends logs to the TUI"""
    def emit(self, record):
        try:
            msg = self.format(record)
            # Clean up verbose prefixes for better TUI display
            if ':' in msg:
                # Remove logger name prefix (everything before the first colon)
                msg = msg.split(':', 1)[1].strip()
            log_queue.put(msg)
        except Exception:
            pass

# Configure logging with TUI handler ONLY
# Remove all existing handlers first
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Set up our custom TUI handler
tui_handler = TUILogHandler()
tui_handler.setFormatter(logging.Formatter('%(name)s: %(message)s'))

# Configure root logger to use ONLY our handler - this will catch all log messages
logging.root.setLevel(logging.INFO)
logging.root.addHandler(tui_handler)

# Get logger instances but DON'T add handlers (they inherit from root)
logger = logging.getLogger(__name__)
kopf_logger = logging.getLogger('kopf')

# Suppress overly verbose loggers
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('kubernetes').setLevel(logging.WARNING)
logging.getLogger('kubernetes').setLevel(logging.WARNING)

# --- Utility Functions ---
def load_kube_config():
    """Load Kubernetes configuration"""
    try:
        config.load_incluster_config()
        logger.info("Loaded in-cluster Kubernetes config")
    except config.ConfigException:
        config.load_kube_config()
        logger.info("Loaded local Kubernetes config")

def vm_exists(vm_name, kubevirt_namespace="kubevirt"):
    """Check if a VirtualMachine exists in KubeVirt"""
    try:
        k8s_api = client.CustomObjectsApi()
        k8s_api.get_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=kubevirt_namespace,
            plural="virtualmachines",
            name=vm_name
        )
        return True
    except ApiException as e:
        if e.status == 404:
            return False
        logger.error(f"Error checking VM existence: {e}")
        raise

def get_comprehensive_status():
    """Get comprehensive status of all VMs, CRs (local and deployed), and their relationships"""
    status_report = {
        'local_crs': {},
        'deployed_crs': {},
        'running_vms': {},
        'orphaned_services': [],
        'scenarios': {}
    }
    
    try:
        # 1. Scan local CR files
        manifest_dir = "/root/kubernetes-installer/manifest-controller"
        if os.path.exists(manifest_dir):
            for file in os.listdir(manifest_dir):
                if file.endswith('-cr.yaml') or file.endswith('cr.yaml'):
                    file_path = os.path.join(manifest_dir, file)
                    try:
                        with open(file_path, 'r') as f:
                            cr_data = yaml.safe_load(f)
                            if cr_data and cr_data.get('kind') == 'WindowsVM':
                                name = cr_data['metadata']['name']
                                vm_name = cr_data['spec'].get('vmName', name)
                                enabled = cr_data['spec'].get('enabled', True)
                                # Keep action for backward compatibility but prefer enabled
                                action = cr_data['spec'].get('action', 'install' if enabled else 'uninstall')
                                status_report['local_crs'][name] = {
                                    'file': file,
                                    'vm_name': vm_name,
                                    'enabled': enabled,
                                    'action': action,  # For display compatibility
                                    'namespace': cr_data['metadata'].get('namespace', 'default')
                                }
                    except Exception as e:
                        logger.warning(f"Failed to parse CR file {file}: {e}")
        
        # 2. Get deployed CRs
        try:
            k8s_api = client.CustomObjectsApi()
            deployed_crs = k8s_api.list_cluster_custom_object(
                group=CRD_GROUP,
                version=CRD_VERSION,
                plural=CRD_PLURAL
            )
            
            for cr in deployed_crs.get('items', []):
                name = cr['metadata']['name']
                vm_name = cr['spec'].get('vmName', name)
                enabled = cr['spec'].get('enabled', True)
                # Keep action for backward compatibility but prefer enabled
                action = cr['spec'].get('action', 'install' if enabled else 'uninstall')
                status_report['deployed_crs'][name] = {
                    'vm_name': vm_name,
                    'enabled': enabled,
                    'action': action,  # For display compatibility
                    'namespace': cr['metadata'].get('namespace', 'default'),
                    'status': cr.get('status', {})
                }
        except Exception as e:
            logger.warning(f"Failed to get deployed CRs: {e}")
        
        # 3. Get running VMs
        try:
            k8s_api = client.CustomObjectsApi()
            vms = k8s_api.list_namespaced_custom_object(
                group="kubevirt.io",
                version="v1",
                namespace="kubevirt",
                plural="virtualmachines"
            )
            
            for vm in vms.get('items', []):
                name = vm['metadata']['name']
                vm_status = vm.get('status', {})
                status_report['running_vms'][name] = {
                    'ready': vm_status.get('ready', False),
                    'created': vm_status.get('created', False),
                    'printable_status': vm_status.get('printableStatus', 'Unknown'),
                    'conditions': vm_status.get('conditions', [])
                }
                
                # Get VMI status if exists
                try:
                    vmi = k8s_api.get_namespaced_custom_object(
                        group="kubevirt.io",
                        version="v1",
                        namespace="kubevirt",
                        plural="virtualmachineinstances",
                        name=name
                    )
                    status_report['running_vms'][name]['vmi_phase'] = vmi.get('status', {}).get('phase', 'Unknown')
                    status_report['running_vms'][name]['vmi_ready'] = vmi.get('status', {}).get('ready', False)
                except:
                    status_report['running_vms'][name]['vmi_phase'] = 'NotCreated'
                    status_report['running_vms'][name]['vmi_ready'] = False
                    
        except Exception as e:
            logger.warning(f"Failed to get running VMs: {e}")
        
        # 4. Analyze scenarios
        all_vm_names = set()
        all_vm_names.update(cr['vm_name'] for cr in status_report['local_crs'].values())
        all_vm_names.update(cr['vm_name'] for cr in status_report['deployed_crs'].values())
        all_vm_names.update(status_report['running_vms'].keys())
        
        for vm_name in all_vm_names:
            # Check local CR
            local_cr = None
            local_enabled = None
            for cr_name, cr_data in status_report['local_crs'].items():
                if cr_data['vm_name'] == vm_name:
                    local_cr = cr_name
                    local_enabled = cr_data.get('enabled', True)
                    break
            
            # Check deployed CR
            deployed_cr = None
            deployed_enabled = None
            for cr_name, cr_data in status_report['deployed_crs'].items():
                if cr_data['vm_name'] == vm_name:
                    deployed_cr = cr_name
                    deployed_enabled = cr_data.get('enabled', True)
                    break
            
            # Check running VM
            vm_running = vm_name in status_report['running_vms']
            vm_status = status_report['running_vms'].get(vm_name, {}).get('printable_status', 'NotExists')
            
            # Determine scenario based on enabled state vs reality
            scenario = "Unknown"
            issue_type = None
            
            if local_cr and not deployed_cr:
                if local_enabled and not vm_running:
                    scenario = "1A: Local CR (enabled) → No Instance"
                    issue_type = "missing_deployment"
                elif local_enabled and vm_running:
                    scenario = "1B: Local CR (enabled) → Instance Running (Unmanaged)"
                    issue_type = "unmanaged_vm"
                elif not local_enabled and not vm_running:
                    scenario = "1C: Local CR (disabled) → No Instance (Correct)"
                elif not local_enabled and vm_running:
                    scenario = "1D: Local CR (disabled) → Instance Running (Should Remove)"
                    issue_type = "unexpected_vm"
                    
            elif not local_cr and deployed_cr:
                if deployed_enabled and not vm_running:
                    scenario = "2A: Deployed CR (enabled) → No Instance"
                    issue_type = "vm_missing"
                elif deployed_enabled and vm_running:
                    scenario = "2B: Deployed CR (enabled) → Instance Running (Correct)"
                elif not deployed_enabled and not vm_running:
                    scenario = "2C: Deployed CR (disabled) → No Instance (Correct)"
                elif not deployed_enabled and vm_running:
                    scenario = "2D: Deployed CR (disabled) → Instance Running (Should Remove)"
                    issue_type = "vm_should_be_disabled"
                    
            elif local_cr and deployed_cr:
                local_state = "enabled" if local_enabled else "disabled"
                deployed_state = "enabled" if deployed_enabled else "disabled"
                vm_state = "running" if vm_running else "not running"
                
                if local_enabled == deployed_enabled:
                    if local_enabled and vm_running:
                        scenario = f"Mixed: Both CRs (enabled) → Instance Running (Correct)"
                    elif local_enabled and not vm_running:
                        scenario = f"Mixed: Both CRs (enabled) → No Instance"
                        issue_type = "vm_missing"
                    elif not local_enabled and not vm_running:
                        scenario = f"Mixed: Both CRs (disabled) → No Instance (Correct)"
                    elif not local_enabled and vm_running:
                        scenario = f"Mixed: Both CRs (disabled) → Instance Running (Should Remove)"
                        issue_type = "vm_should_be_disabled"
                else:
                    scenario = f"Mixed: Local CR ({local_state}) vs Deployed CR ({deployed_state}) → Instance {vm_state}"
                    issue_type = "cr_state_mismatch"
                    
            elif not local_cr and not deployed_cr and vm_running:
                scenario = "3A: No CR → Instance Running (Orphaned)"
                issue_type = "orphaned_vm"
            elif not local_cr and not deployed_cr and not vm_running:
                scenario = "3B: No CR → Instance Deleted (Clean)"
            
            status_report['scenarios'][vm_name] = {
                'scenario': scenario,
                'local_cr': local_cr,
                'deployed_cr': deployed_cr,
                'vm_running': vm_running,
                'vm_status': vm_status,
                'local_enabled': local_enabled,
                'deployed_enabled': deployed_enabled,
                'issue_type': issue_type,
                # Keep backward compatibility
                'local_cr_action': status_report['local_crs'].get(local_cr, {}).get('action', None) if local_cr else None,
                'deployed_cr_action': status_report['deployed_crs'].get(deployed_cr, {}).get('action', None) if deployed_cr else None
            }
        
        return status_report
        
    except Exception as e:
        logger.error(f"Error getting comprehensive status: {e}")
        return status_report

def get_vm_status(vm_name, kubevirt_namespace="kubevirt"):
    """Get detailed VM status including running state"""
    try:
        k8s_api = client.CustomObjectsApi()
        
        # Get VirtualMachine resource
        vm = k8s_api.get_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=kubevirt_namespace,
            plural="virtualmachines",
            name=vm_name
        )
        
        vm_status = {
            'exists': True,
            'vm_ready': vm.get('status', {}).get('ready', False),
            'vm_created': vm.get('status', {}).get('created', False),
            'conditions': vm.get('status', {}).get('conditions', [])
        }
        
        # Try to get VirtualMachineInstance status
        try:
            vmi = k8s_api.get_namespaced_custom_object(
                group="kubevirt.io",
                version="v1",
                namespace=kubevirt_namespace,
                plural="virtualmachineinstances",
                name=vm_name
            )
            
            vm_status.update({
                'vmi_exists': True,
                'vmi_phase': vmi.get('status', {}).get('phase', 'Unknown'),
                'vmi_conditions': vmi.get('status', {}).get('conditions', []),
                'vmi_interfaces': vmi.get('status', {}).get('interfaces', [])
            })
            
            # Check if VM is actually running and ready
            phase = vm_status['vmi_phase']
            vm_status['is_running'] = (phase == 'Running')
            vm_status['is_ready'] = vm_status['vm_ready'] and vm_status['is_running']
            
        except ApiException as vmi_e:
            if vmi_e.status == 404:
                vm_status.update({
                    'vmi_exists': False,
                    'vmi_phase': 'NotCreated',
                    'is_running': False,
                    'is_ready': False
                })
            else:
                logger.error(f"Error checking VMI status: {vmi_e}")
                vm_status.update({
                    'vmi_exists': None,
                    'vmi_phase': 'Error',
                    'is_running': False,
                    'is_ready': False
                })
        
        return vm_status
        
    except ApiException as e:
        if e.status == 404:
            return {
                'exists': False,
                'vm_ready': False,
                'is_running': False,
                'is_ready': False,
                'vmi_exists': False,
                'vmi_phase': 'NotExists'
            }
        logger.error(f"Error getting VM status: {e}")
        raise

async def force_cleanup_vm(vm_name, kubevirt_namespace="kubevirt"):
    """Force cleanup of VM resources when normal uninstall fails"""
    logger.info(f"🔧 Force cleanup initiated for VM: {vm_name}")
    
    try:
        k8s_api = client.CustomObjectsApi()
        cleanup_success = True
        
        # Try to delete VirtualMachineInstance first
        try:
            k8s_api.delete_namespaced_custom_object(
                group="kubevirt.io",
                version="v1",
                namespace=kubevirt_namespace,
                plural="virtualmachineinstances",
                name=vm_name
            )
            logger.info(f"🗑️ Deleted VirtualMachineInstance: {vm_name}")
        except ApiException as e:
            if e.status != 404:
                logger.warning(f"⚠️ Failed to delete VMI {vm_name}: {e}")
                cleanup_success = False
        
        # Try to delete VirtualMachine
        try:
            k8s_api.delete_namespaced_custom_object(
                group="kubevirt.io",
                version="v1",
                namespace=kubevirt_namespace,
                plural="virtualmachines",
                name=vm_name
            )
            logger.info(f"🗑️ Deleted VirtualMachine: {vm_name}")
        except ApiException as e:
            if e.status != 404:
                logger.warning(f"⚠️ Failed to delete VM {vm_name}: {e}")
                cleanup_success = False
        
        # Try to delete associated PVCs if they exist
        v1 = client.CoreV1Api()
        try:
            pvcs = v1.list_namespaced_persistent_volume_claim(namespace=kubevirt_namespace)
            for pvc in pvcs.items:
                if vm_name in pvc.metadata.name:
                    v1.delete_namespaced_persistent_volume_claim(
                        name=pvc.metadata.name,
                        namespace=kubevirt_namespace
                    )
                    logger.info(f"🗑️ Deleted PVC: {pvc.metadata.name}")
        except Exception as e:
            logger.warning(f"⚠️ Error cleaning up PVCs for {vm_name}: {e}")
        
        return cleanup_success
        
    except Exception as e:
        logger.error(f"💥 Force cleanup failed for VM {vm_name}: {e}")
        return False

async def run_ansible_playbook(spec, action="install"):
    """Run the Ansible playbook with given spec"""
    vm_name = spec.get('vmName', 'unknown')
    logger.info(f"Running Ansible playbook for VM: {vm_name}, action: {action}")
    
    # Prepare the spec with action - convert kopf.Spec to dict
    playbook_spec = dict(spec)
    playbook_spec['action'] = action
    
    # Build ansible-playbook command with non-interactive flags
    cmd = [
        "ansible-playbook", 
        PLAYBOOK_PATH,
        "-v"  # Verbose but not too verbose
    ]
    for k, v in playbook_spec.items():
        if isinstance(v, bool):
            v = str(v).lower()
        elif v is None:
            continue
        cmd.extend(["-e", f"{k}={v}"])
    
    logger.info(f"Executing: {' '.join(cmd)}")
    
    # Environment variables to ensure ansible doesn't interfere with TUI
    env = {
        **os.environ,
        'PYTHONUNBUFFERED': '1',
        'TERM': 'xterm-256color'  # Enable full color support
    }
    
    try:
        # Run the playbook asynchronously with controlled environment
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
            stdin=asyncio.subprocess.DEVNULL  # Prevent any input prompts
        )
        
        # Stream output line by line and ensure it goes only to our logger
        output_buffer = ""
        while True:
            try:
                line = await process.stdout.readline()
                if not line:
                    break
                
                line_text = line.decode('utf-8', errors='replace').strip()
                if line_text:
                    # Filter out ansible control characters and formatting
                    clean_line = ''.join(char for char in line_text if ord(char) >= 32 or char in '\n\t')
                    
                    # Clean up ansible output - remove verbose prefixes and timestamps
                    if clean_line.startswith('TASK ['):
                        # Simplify task headers
                        task_name = clean_line.replace('TASK [', '').replace(']', '').strip('*').strip()
                        logger.info(f"🔧 {task_name}")
                    elif 'ok:' in clean_line and '=>' in clean_line:
                        # Simplify ok status messages
                        if 'changed' in clean_line and 'false' in clean_line:
                            continue  # Skip unchanged status messages
                        elif 'item' in clean_line:
                            continue  # Skip verbose item details
                        else:
                            logger.info(f"✅ Task completed")
                    elif 'failed:' in clean_line:
                        logger.error(f"❌ {clean_line}")
                    elif 'fatal:' in clean_line:
                        logger.error(f"💥 {clean_line}")
                    elif 'changed:' in clean_line:
                        logger.info(f"🔄 {clean_line}")
                    elif clean_line.startswith('PLAY ['):
                        play_name = clean_line.replace('PLAY [', '').replace(']', '').strip('*').strip()
                        logger.info(f"🎭 {play_name}")
                    elif 'PLAY RECAP' in clean_line:
                        logger.info(f"📊 Playbook Summary")
                    elif any(keyword in clean_line.lower() for keyword in ['error', 'failed', 'exception']):
                        logger.error(f"⚠️ {clean_line}")
                    # Skip overly verbose lines
                    elif any(skip_phrase in clean_line for skip_phrase in [
                        'ansible_loop_var', 'duration', 'method', 'api_version', 'kind',
                        'namespaced', 'result', '"changed": false'
                    ]):
                        continue
                    else:
                        # Only log important lines
                        if len(clean_line) > 10 and not clean_line.startswith(' '):
                            logger.info(clean_line)
                    
            except Exception as e:
                logger.error(f"Error reading ansible output: {e}")
                break
        
        await process.wait()
        
        if process.returncode == 0:
            logger.info(f"✅ Playbook completed successfully for VM: {vm_name}")
            return True
        else:
            # Enhanced error reporting for different exit codes
            error_details = {
                1: "General errors (syntax errors, wrong parameters)",
                2: "Connection or unreachable host errors", 
                3: "Syntax errors in playbook or host file",
                4: "Bad or incomplete parameters",
                5: "Ansible configuration error",
                99: "User interrupted execution",
                250: "Unexpected error"
            }
            
            error_desc = error_details.get(process.returncode, f"Unknown error (exit code {process.returncode})")
            detailed_msg = f"❌ Playbook failed for VM {vm_name} - Exit Code {process.returncode}: {error_desc}"
            logger.error(detailed_msg)
            
            # For uninstall operations, provide additional guidance
            if action == 'uninstall':
                if process.returncode == 2:
                    logger.error(f"💡 VM {vm_name} uninstall failed - VM might already be deleted or unreachable")
                    logger.error(f"💡 This could be normal if the VM was already cleaned up manually")
            
            return False
            
    except Exception as e:
        logger.error(f"💥 Error running playbook for VM {vm_name}: {e}")
        return False

def update_cr_status(name, namespace, status, message=""):
    """Update the status of a WindowsVM CR"""
    try:
        load_kube_config()
        k8s_api = client.CustomObjectsApi()
        
        # Get current CR
        cr = k8s_api.get_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=namespace,
            plural=CRD_PLURAL,
            name=name
        )
        
        # Update status
        if 'status' not in cr:
            cr['status'] = {}
        
        cr['status']['phase'] = status
        cr['status']['message'] = message
        
        # Update the CR
        k8s_api.patch_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=namespace,
            plural=CRD_PLURAL,
            name=name,
            body=cr
        )
        
        logger.info(f"Updated CR {name} status to: {status}")
        
    except Exception as e:
        logger.error(f"Failed to update CR status: {e}")

# --- Kopf Event Handlers ---

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    """Configure the operator on startup"""
    settings.peering.priority = 100
    settings.peering.name = "windowsvm-operator"
    settings.watching.connect_timeout = 1 * 60
    settings.watching.server_timeout = 10 * 60
    logger.info("WindowsVM Operator starting up...")

@kopf.on.create(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
async def create_vm(spec, name, namespace, logger, **kwargs):
    """Handle WindowsVM CR creation - Use declarative enabled/disabled pattern"""
    logger.info(f"WindowsVM CR created: {name} in namespace {namespace}")
    
    vm_name = spec.get('vmName')
    enabled = spec.get('enabled', True)  # Default to enabled
    kubevirt_namespace = spec.get('kubevirt_namespace', 'kubevirt')
    
    if not vm_name:
        error_msg = "No vmName specified in spec"
        logger.error(error_msg)
        update_cr_status(name, namespace, "Failed", error_msg)
        return {"message": error_msg}
    
    # Update status to processing
    status_msg = f"VM should be {'enabled' if enabled else 'disabled'}"
    update_cr_status(name, namespace, "Processing", status_msg)
    
    try:
        if enabled:
            # VM should exist and be running
            vm_status = get_vm_status(vm_name, kubevirt_namespace)
            
            if vm_status['exists']:
                if vm_status['is_running']:
                    msg = f"VM {vm_name} already exists and is running - desired state achieved"
                    logger.info(msg)
                    update_cr_status(name, namespace, "Running", msg)
                    return {"message": msg}
                else:
                    msg = f"VM {vm_name} exists but not running (phase: {vm_status.get('vmi_phase', 'Unknown')})"
                    logger.info(msg)
                    update_cr_status(name, namespace, "Exists", msg)
                    return {"message": msg}
            
            # VM doesn't exist - install it
            logger.info(f"Installing VM {vm_name} to achieve enabled state")
            success = await run_ansible_playbook(spec, "install")
            
            if success:
                update_cr_status(name, namespace, "Completed", f"VM {vm_name} enabled and deployed successfully")
                return {"message": f"VM {vm_name} enabled and deployed successfully"}
            else:
                update_cr_status(name, namespace, "Failed", f"Failed to enable VM {vm_name}")
                raise kopf.PermanentError(f"Failed to enable VM {vm_name}")
                
        else:
            # VM should not exist (disabled state)
            logger.info(f"VM {vm_name} is disabled - ensuring it doesn't exist")
            vm_status = get_vm_status(vm_name, kubevirt_namespace)
            
            if vm_status['exists']:
                # VM exists but should be disabled - remove it
                logger.info(f"Removing existing VM {vm_name} to achieve disabled state")
                success = await run_ansible_playbook(spec, "uninstall")
                
                if success:
                    update_cr_status(name, namespace, "NotDeployed", f"VM {vm_name} disabled and removed successfully")
                    return {"message": f"VM {vm_name} disabled and removed successfully"}
                else:
                    update_cr_status(name, namespace, "Failed", f"Failed to disable VM {vm_name}")
                    raise kopf.PermanentError(f"Failed to disable VM {vm_name}")
            else:
                # VM doesn't exist and shouldn't exist - desired state achieved
                msg = f"VM {vm_name} is disabled and doesn't exist - desired state achieved"
                logger.info(msg)
                update_cr_status(name, namespace, "NotDeployed", msg)
                return {"message": msg}
            
    except Exception as e:
        error_msg = f"Error processing VM {vm_name}: {str(e)}"
        logger.error(error_msg)
        update_cr_status(name, namespace, "Failed", error_msg)
        raise kopf.TemporaryError(error_msg, delay=60)

@kopf.on.update(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
async def update_vm(spec, name, namespace, old, new, diff, logger, **kwargs):
    """Handle WindowsVM CR updates - Use declarative enabled/disabled pattern"""
    logger.info(f"WindowsVM CR updated: {name} in namespace {namespace}")
    
    vm_name = spec.get('vmName')
    enabled = spec.get('enabled', True)  # Default to enabled
    kubevirt_namespace = spec.get('kubevirt_namespace', 'kubevirt')
    
    if not vm_name:
        error_msg = "No vmName specified in spec"
        logger.error(error_msg)
        update_cr_status(name, namespace, "Failed", error_msg)
        return {"message": error_msg}
    
    # Log what changed
    for operation, field_path, old_value, new_value in diff:
        logger.info(f"Field {field_path} {operation}: {old_value} -> {new_value}")
    
    # Check if enabled state changed
    old_enabled = old.get('spec', {}).get('enabled', True)
    new_enabled = new.get('spec', {}).get('enabled', True)
    
    if old_enabled != new_enabled:
        logger.info(f"Enabled state changed from {old_enabled} to {new_enabled}")
        status_msg = f"Changing VM state to {'enabled' if new_enabled else 'disabled'}"
        update_cr_status(name, namespace, "Processing", status_msg)
    else:
        logger.info(f"Enabled state unchanged ({new_enabled}) - ensuring desired state")
        update_cr_status(name, namespace, "Processing", f"Ensuring VM {vm_name} is {'enabled' if enabled else 'disabled'}")
    
    try:
        if enabled:
            # VM should exist and be running
            vm_status = get_vm_status(vm_name, kubevirt_namespace)
            
            if vm_status['exists'] and vm_status['is_running']:
                msg = f"VM {vm_name} is already running - desired state achieved"
                logger.info(msg)
                update_cr_status(name, namespace, "Running", msg)
                return {"message": msg}
            
            # VM should be enabled but isn't running - install/start it
            logger.info(f"Enabling VM {vm_name}")
            success = await run_ansible_playbook(spec, "install")
            
            if success:
                update_cr_status(name, namespace, "Completed", f"VM {vm_name} enabled successfully")
                return {"message": f"VM {vm_name} enabled successfully"}
            else:
                update_cr_status(name, namespace, "Failed", f"Failed to enable VM {vm_name}")
                raise kopf.PermanentError(f"Failed to enable VM {vm_name}")
                
        else:
            # VM should not exist (disabled state)
            vm_status = get_vm_status(vm_name, kubevirt_namespace)
            
            if not vm_status['exists']:
                msg = f"VM {vm_name} is already disabled - desired state achieved"
                logger.info(msg)
                update_cr_status(name, namespace, "NotDeployed", msg)
                return {"message": msg}
            
            # VM exists but should be disabled - remove it
            logger.info(f"Disabling VM {vm_name}")
            success = await run_ansible_playbook(spec, "uninstall")
            
            if success:
                update_cr_status(name, namespace, "NotDeployed", f"VM {vm_name} disabled successfully")
                return {"message": f"VM {vm_name} disabled successfully"}
            else:
                update_cr_status(name, namespace, "Failed", f"Failed to disable VM {vm_name}")
                raise kopf.PermanentError(f"Failed to disable VM {vm_name}")
                
    except Exception as e:
        error_msg = f"Error updating VM {vm_name}: {str(e)}"
        logger.error(error_msg)
        update_cr_status(name, namespace, "Failed", error_msg)
        raise kopf.TemporaryError(error_msg, delay=60)
        update_cr_status(name, namespace, "Failed", error_msg)
        raise kopf.TemporaryError(error_msg, delay=60)

@kopf.on.delete(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
async def delete_vm(spec, name, namespace, logger, **kwargs):
    """Handle WindowsVM CR deletion"""
    logger.info(f"WindowsVM CR deleted: {name} in namespace {namespace}")
    
    vm_name = spec.get('vmName')
    kubevirt_namespace = spec.get('kubevirt_namespace', 'kubevirt')
    
    if not vm_name:
        logger.warning("No vmName specified in spec, skipping VM cleanup")
        return {"message": "No vmName specified, skipped cleanup"}
    
    try:
        # Check if VM exists before attempting deletion
        vm_status = get_vm_status(vm_name, kubevirt_namespace)
        
        if vm_status['exists']:
            logger.info(f"VM {vm_name} exists (running: {vm_status['is_running']}), attempting uninstall...")
            
            # Run uninstall playbook
            uninstall_spec = dict(spec)
            uninstall_spec['action'] = 'uninstall'
            success = await run_ansible_playbook(uninstall_spec, "uninstall")
            
            if success:
                logger.info(f"✅ VM {vm_name} uninstalled successfully")
                return {"message": f"VM {vm_name} uninstalled successfully"}
            else:
                # For uninstall failures, check if VM actually got removed despite the error
                post_uninstall_status = get_vm_status(vm_name, kubevirt_namespace)
                
                if not post_uninstall_status['exists']:
                    logger.info(f"🔄 VM {vm_name} was actually removed despite playbook error")
                    return {"message": f"VM {vm_name} removed (playbook reported error but VM is gone)"}
                elif not post_uninstall_status['is_running']:
                    logger.info(f"🔄 VM {vm_name} stopped but manifest still exists")
                    return {"message": f"VM {vm_name} stopped (manifest cleanup may be needed)"}
                else:
                    logger.error(f"❌ VM {vm_name} uninstall failed and VM is still running")
                    logger.info(f"🔧 Attempting force cleanup for VM {vm_name}")
                    
                    # Attempt force cleanup
                    force_success = await force_cleanup_vm(vm_name, kubevirt_namespace)
                    
                    if force_success:
                        logger.info(f"✅ Force cleanup successful for VM {vm_name}")
                        return {"message": f"VM {vm_name} forcibly removed after uninstall failure"}
                    else:
                        # Use PermanentError for true failures to avoid endless retries
                        raise kopf.PermanentError(f"Failed to uninstall VM {vm_name} - both normal and force cleanup failed")
        else:
            logger.info(f"✅ VM {vm_name} does not exist, cleanup not needed")
            return {"message": f"VM {vm_name} does not exist, cleanup not needed"}
            
    except Exception as e:
        error_msg = f"Error during VM cleanup for {vm_name}: {str(e)}"
        logger.error(error_msg)
        raise kopf.TemporaryError(error_msg, delay=60)

@kopf.timer(CRD_GROUP, CRD_VERSION, CRD_PLURAL, interval=30.0, idle=60.0)
async def monitor_vm_status(spec, name, namespace, logger, **kwargs):
    """Monitor VM status every 30 seconds and update CR accordingly"""
    vm_name = spec.get('vmName')
    kubevirt_namespace = spec.get('kubevirt_namespace', 'kubevirt')
    
    if not vm_name:
        return
    
    try:
        # Get detailed VM status
        vm_status = get_vm_status(vm_name, kubevirt_namespace)
        
        # Determine overall status
        if not vm_status['exists']:
            status_phase = "NotDeployed"
            status_msg = f"VM {vm_name} manifest not found in Kubernetes"
        elif vm_status['is_ready'] and vm_status['is_running']:
            status_phase = "Running"
            status_msg = f"VM {vm_name} is running and ready"
        elif vm_status['is_running']:
            status_phase = "Starting"
            status_msg = f"VM {vm_name} is running but not fully ready"
        elif vm_status['vmi_exists']:
            vmi_phase = vm_status.get('vmi_phase', 'Unknown')
            status_phase = f"VMI-{vmi_phase}"
            status_msg = f"VM {vm_name} VMI phase: {vmi_phase}"
        elif vm_status['vm_created']:
            status_phase = "Created"
            status_msg = f"VM {vm_name} created but VMI not started"
        else:
            status_phase = "Pending"
            status_msg = f"VM {vm_name} manifest exists but not created"
        
        # Update CR status with detailed information
        update_cr_status(name, namespace, status_phase, status_msg)
        
        # Log status for monitoring
        logger.debug(f"VM {vm_name} status: {status_phase} - {status_msg}")
        
    except Exception as e:
        logger.error(f"Error monitoring VM {vm_name} status: {e}")

# --- TUI Implementation ---

class WindowsVMTUI:
    def __init__(self):
        self.log_lines = []
        self.max_log_lines = 1000
        self.status_data = {}
        
        # Create palette
        self.palette = [
            ('header', 'white', 'dark blue'),
            ('menu', 'black', 'light gray'),
            ('menu_focus', 'white', 'dark red'),
            ('log_info', 'light green', 'black'),
            ('log_error', 'light red', 'black'),
            ('log_warning', 'yellow', 'black'),
            ('footer', 'white', 'dark blue'),
            ('button', 'black', 'light gray'),
            ('button_focus', 'white', 'dark red'),
            ('status_running', 'light green', 'black'),
            ('status_stopped', 'light red', 'black'),
            ('status_unknown', 'yellow', 'black'),
            ('cr_deployed', 'light cyan', 'black'),
            ('cr_local', 'light magenta', 'black'),
            ('cr_missing', 'dark gray', 'black'),
        ]
        
        self.setup_ui()
        
    def setup_ui(self):
        # Header
        header = urwid.Text(('header', 'WindowsVM Kopf Operator Console'), align='center')
        header = urwid.AttrMap(header, 'header')
        
        # Menu bar
        menu_items = [
            ('List VMs', self.list_vms),
            ('Status View', self.show_status),
            ('Fix Issues', self.fix_issues),
            ('Apply CRs', self.apply_manifests),
            ('Create VM', self.create_vm),
            ('Delete VM', self.delete_vm),
            ('Refresh', self.refresh_display),
            ('Clear Logs', self.clear_logs),
            ('Quit', self.quit_app)
        ]
        
        menu_buttons = []
        for label, callback in menu_items:
            btn = urwid.Button(label, on_press=callback)
            btn = urwid.AttrMap(btn, 'menu', 'menu_focus')
            menu_buttons.append(btn)
        
        menu_bar = urwid.Columns(menu_buttons, dividechars=1)
        menu_frame = urwid.AttrMap(menu_bar, 'menu')
        
        # Add separator line under menu
        separator = urwid.Divider('─')
        separator = urwid.AttrMap(separator, 'menu')
        
        # Status display panel
        self.status_walker = urwid.SimpleFocusListWalker([])
        self.status_listbox = urwid.ListBox(self.status_walker)
        self.status_frame = urwid.LineBox(self.status_listbox, title="VM/CR Status Overview")
        
        # Log display
        self.log_walker = urwid.SimpleFocusListWalker([])
        self.log_listbox = urwid.ListBox(self.log_walker)
        self.log_frame = urwid.LineBox(self.log_listbox, title="Kopf Operator Logs [FOCUSED]")
        self.auto_scroll = True  # Toggle for auto-scrolling behavior
        
        # Create horizontal split between status and logs with focus support
        self.content_columns = urwid.Columns([
            ('weight', 1, self.status_frame),
            ('weight', 2, self.log_frame)
        ], dividechars=1, focus_column=1)  # Start with logs focused
        
        # Footer with updated navigation instructions
        footer_text = "F2:Status F3:ListVMs F4:ClearLogs F5:Refresh F6:FixIssues F7:ApplyCRs F8:AutoScroll Tab:SwitchPanel Ctrl+C/Q:Quit"
        footer = urwid.Text(('footer', footer_text), align='center')
        footer = urwid.AttrMap(footer, 'footer')
        
        # Main layout with fixed header/menu and split content area
        top_section = urwid.Pile([
            ('pack', header),
            ('pack', urwid.Text("")),  # Spacer
            ('pack', menu_frame),
            ('pack', separator),
            ('pack', urwid.Text("")),  # Spacer
        ])
        
        main_pile = urwid.Pile([
            ('pack', top_section),
            ('weight', 1, self.content_columns),
            ('pack', footer)
        ])
        
        self.main_frame = main_pile
        
        # Start log update timer
        self.loop = None
        
    def add_log_line(self, text):
        """Add a log line to the display"""
        # Determine log level color
        if 'ERROR' in text.upper():
            attr = 'log_error'
        elif 'WARNING' in text.upper() or 'WARN' in text.upper():
            attr = 'log_warning'
        else:
            attr = 'log_info'
            
        log_widget = urwid.Text((attr, text))
        self.log_walker.append(log_widget)
        
        # Keep only recent logs
        if len(self.log_walker) > self.max_log_lines:
            self.log_walker.pop(0)
        
        # Only auto-scroll if enabled and user isn't manually scrolling
        if self.auto_scroll and self.log_walker:
            try:
                self.log_listbox.focus_position = len(self.log_walker) - 1
            except:
                pass  # Ignore focus errors
            
        # Don't force screen refresh - let urwid handle it naturally
    
    def update_status_display(self):
        """Update the status display with comprehensive VM/CR information"""
        try:
            status_report = get_comprehensive_status()
            
            # Clear existing status display
            self.status_walker.clear()
            
            # Add header
            header_text = urwid.Text(('header', f'=== VM/CR Status Report ({datetime.now().strftime("%H:%M:%S")}) ==='))
            self.status_walker.append(header_text)
            self.status_walker.append(urwid.Text(""))
            
            # Show scenarios
            if status_report['scenarios']:
                self.status_walker.append(urwid.Text(('cr_deployed', '📊 SCENARIO ANALYSIS:')))
                for vm_name, scenario_data in status_report['scenarios'].items():
                    scenario = scenario_data['scenario']
                    
                    # Color coding based on scenario
                    if 'Running' in scenario and 'Managed' in scenario:
                        color = 'status_running'
                        icon = '✅'
                    elif 'Running' in scenario and 'Orphaned' in scenario:
                        color = 'status_unknown'
                        icon = '⚠️ '
                    elif 'No Instance' in scenario:
                        color = 'status_stopped'
                        icon = '❌'
                    else:
                        color = 'status_unknown'
                        icon = '❓'
                    
                    status_line = f"{icon} {vm_name}: {scenario}"
                    self.status_walker.append(urwid.Text((color, status_line)))
                    
                    # Add details
                    if scenario_data['local_cr']:
                        self.status_walker.append(urwid.Text(('cr_local', f"   📁 Local CR: {scenario_data['local_cr']} (action: {scenario_data['local_cr_action']})")))
                    if scenario_data['deployed_cr']:
                        self.status_walker.append(urwid.Text(('cr_deployed', f"   ☸️  Deployed CR: {scenario_data['deployed_cr']} (action: {scenario_data['deployed_cr_action']})")))
                    if scenario_data['vm_running']:
                        self.status_walker.append(urwid.Text(('status_running', f"   🖥️  VM Status: {scenario_data['vm_status']}")))
                    
                    self.status_walker.append(urwid.Text(""))
            else:
                self.status_walker.append(urwid.Text(('status_unknown', '❓ No VMs or CRs found')))
            
            # Summary statistics
            self.status_walker.append(urwid.Text(('header', '📈 SUMMARY:')))
            self.status_walker.append(urwid.Text(f"Local CRs: {len(status_report['local_crs'])}"))
            self.status_walker.append(urwid.Text(f"Deployed CRs: {len(status_report['deployed_crs'])}"))
            self.status_walker.append(urwid.Text(f"Running VMs: {len(status_report['running_vms'])}"))
            
            # Store status data for other methods
            self.status_data = status_report
            
        except Exception as e:
            self.status_walker.clear()
            self.status_walker.append(urwid.Text(('log_error', f'Error updating status: {e}')))
    
    def show_status(self, button):
        """Show detailed status view"""
        self.update_status_display()
        self.add_log_line("🔄 Status display updated")
    
    def update_logs(self):
        """Update logs from the queue"""
        updated = False
        try:
            # Process multiple log entries at once to reduce update frequency
            for _ in range(5):  # Process up to 5 logs at once
                log_line = log_queue.get_nowait()
                self.add_log_line(log_line)
                updated = True
        except queue.Empty:
            pass
            
        # If we updated logs, force a controlled screen refresh
        if updated and self.loop:
            # Clear the screen in a controlled way
            self.loop.screen.clear()
            
        # Schedule next update (both logs and status)
        if self.loop and hasattr(self.loop, 'set_alarm_in'):
            self.loop.set_alarm_in(0.3, lambda loop, user_data: self.update_logs())
            # Update status display every 5 seconds
            self.loop.set_alarm_in(5.0, lambda loop, user_data: self.auto_refresh_status())
    
    def initial_startup(self):
        """Perform initial startup tasks"""
        try:
            # Load initial status
            self.update_status_display()
            self.add_log_line("📊 Initial status display loaded")
            
            # Show comprehensive status in logs
            status_report = get_comprehensive_status()
            
            self.add_log_line(f"📈 Found {len(status_report['local_crs'])} local CRs, {len(status_report['deployed_crs'])} deployed CRs, {len(status_report['running_vms'])} running VMs")
            
            # Log scenario summary
            scenario_counts = {}
            for vm_name, scenario_data in status_report['scenarios'].items():
                scenario_type = scenario_data['scenario'].split(':')[0]
                scenario_counts[scenario_type] = scenario_counts.get(scenario_type, 0) + 1
            
            for scenario_type, count in scenario_counts.items():
                self.add_log_line(f"📋 Scenario {scenario_type}: {count} instances")
                
        except Exception as e:
            self.add_log_line(f"⚠️ Error during startup: {e}")
    
    def auto_refresh_status(self):
        """Automatically refresh status display"""
        try:
            self.update_status_display()
            # Schedule next auto-refresh
            if self.loop and hasattr(self.loop, 'set_alarm_in'):
                self.loop.set_alarm_in(5.0, lambda loop, user_data: self.auto_refresh_status())
        except Exception as e:
            logger.warning(f"Auto-refresh failed: {e}")
    
    def list_vms(self, button):
        """List all WindowsVM CRs with detailed status"""
        try:
            load_kube_config()
            k8s_api = client.CustomObjectsApi()
            
            result = k8s_api.list_namespaced_custom_object(
                group=CRD_GROUP,
                version=CRD_VERSION,
                namespace='default',
                plural=CRD_PLURAL
            )
            
            crs = result.get('items', [])
            self.add_log_line(f"=== Found {len(crs)} WindowsVM CRs ===")
            
            for cr in crs:
                name = cr['metadata']['name']
                action = cr['spec'].get('action', 'unknown')
                status = cr.get('status', {}).get('phase', 'Unknown')
                status_msg = cr.get('status', {}).get('message', '')
                vm_name = cr['spec'].get('vmName', name)
                
                # Get real-time VM status
                try:
                    vm_status = get_vm_status(vm_name, cr['spec'].get('kubevirt_namespace', 'kubevirt'))
                    if vm_status['is_running']:
                        runtime_status = "🟢 RUNNING"
                    elif vm_status['exists']:
                        runtime_status = f"🟡 {vm_status.get('vmi_phase', 'STOPPED')}"
                    else:
                        runtime_status = "🔴 NOT_DEPLOYED"
                except Exception:
                    runtime_status = "❓ UNKNOWN"
                
                self.add_log_line(f"• {name}: vm={vm_name}, status={status}, runtime={runtime_status}")
                if status_msg:
                    self.add_log_line(f"  └─ {status_msg}")
                
        except Exception as e:
            self.add_log_line(f"ERROR: Failed to list VMs: {e}")
    
    def create_vm(self, button):
        """Show create VM dialog"""
        self.show_dialog("Create VM", "Enter VM name:", self.do_create_vm)
    
    def delete_vm(self, button):
        """Show delete VM dialog"""
        self.show_dialog("Delete VM", "Enter VM name to delete:", self.do_delete_vm)
    
    def do_create_vm(self, vm_name):
        """Create a new WindowsVM CR"""
        if not vm_name.strip():
            self.add_log_line("ERROR: VM name cannot be empty")
            return
            
        try:
            load_kube_config()
            k8s_api = client.CustomObjectsApi()
            
            # Create a simple WindowsVM CR
            vm_cr = {
                "apiVersion": f"{CRD_GROUP}/{CRD_VERSION}",
                "kind": "WindowsVM",
                "metadata": {
                    "name": vm_name.strip(),
                    "namespace": "default"
                },
                "spec": {
                    "vmName": vm_name.strip(),
                    "action": "install",
                    "windows_version": "2025",
                    "image": "win2025server.vhdx",
                    "installer_disk_size": "15Gi",
                    "kubevirt_namespace": "kubevirt",
                    "storage_dir": "/data/vms",
                    "system_disk_size": "40Gi",
                    "vhdx_path": "/data/vms/win2025server.vhdx",
                    "virtio_iso_size": "500Mi",
                    "vm_cpu_cores": 4,
                    "vm_memory": "8Gi",
                    "windows_admin_password": "Secret123%%",
                    "windows_product_key": "XXXXX-XXXXX-XXXXX-XXXXX-XXXXX"
                }
            }
            
            k8s_api.create_namespaced_custom_object(
                group=CRD_GROUP,
                version=CRD_VERSION,
                namespace='default',
                plural=CRD_PLURAL,
                body=vm_cr
            )
            
            self.add_log_line(f"✓ Created WindowsVM CR: {vm_name}")
            
        except Exception as e:
            if '409' in str(e):
                self.add_log_line(f"⚠ WindowsVM {vm_name} already exists")
            else:
                self.add_log_line(f"ERROR: Failed to create VM {vm_name}: {e}")
    
    def do_delete_vm(self, vm_name):
        """Delete a WindowsVM CR"""
        if not vm_name.strip():
            self.add_log_line("ERROR: VM name cannot be empty")
            return
            
        try:
            load_kube_config()
            k8s_api = client.CustomObjectsApi()
            
            k8s_api.delete_namespaced_custom_object(
                group=CRD_GROUP,
                version=CRD_VERSION,
                namespace='default',
                plural=CRD_PLURAL,
                name=vm_name.strip()
            )
            
            self.add_log_line(f"✓ Deleted WindowsVM CR: {vm_name}")
            
        except Exception as e:
            if '404' in str(e):
                self.add_log_line(f"⚠ WindowsVM {vm_name} not found")
            else:
                self.add_log_line(f"ERROR: Failed to delete VM {vm_name}: {e}")
    
    def fix_issues(self, button):
        """Interactive menu to fix identified issues"""
        try:
            status_report = get_comprehensive_status()
            issues = []
            
            # Identify fixable issues
            for vm_name, scenario_data in status_report['scenarios'].items():
                scenario = scenario_data['scenario']
                
                if '1A: Local CR → No Instance' in scenario:
                    issues.append({
                        'type': 'deploy_cr',
                        'vm_name': vm_name,
                        'cr_name': scenario_data['local_cr'],
                        'description': f"Deploy VM {vm_name} using CR {scenario_data['local_cr']}",
                        'action': 'Apply Local CR (install)',
                        'safe': True  # Safe to install since VM doesn't exist
                    })
                elif '3A: No CR → Instance Running (Orphaned)' in scenario:
                    issues.append({
                        'type': 'create_cr',
                        'vm_name': vm_name,
                        'description': f"Generate management CR for orphaned VM {vm_name}",
                        'action': 'Generate Management CR',
                        'safe': True  # Safe to generate CR
                    })
                elif '1B: Local CR → Instance Running (Unmanaged)' in scenario:
                    issues.append({
                        'type': 'apply_management_cr',
                        'vm_name': vm_name,
                        'cr_name': scenario_data['local_cr'],
                        'description': f"Apply management CR for running VM {vm_name}",
                        'action': 'Apply Management CR (careful)',
                        'safe': False  # Need to be careful with running VMs
                    })
            
            if not issues:
                self.add_log_line("✅ No fixable issues found!")
                return
                
            # Show interactive fix menu
            self.show_fix_menu(issues)
            
        except Exception as e:
            self.add_log_line(f"❌ Error analyzing issues: {e}")
    
    def show_fix_menu(self, issues):
        """Show interactive menu for fixing issues"""
        self.add_log_line(f"🔧 Found {len(issues)} fixable issues:")
        
        safe_issues = [i for i in issues if i['safe']]
        risky_issues = [i for i in issues if not i['safe']]
        
        for i, issue in enumerate(safe_issues, 1):
            self.add_log_line(f"✅ {i}. {issue['description']} (SAFE)")
        
        for i, issue in enumerate(risky_issues, len(safe_issues)+1):
            self.add_log_line(f"⚠️ {i}. {issue['description']} (REQUIRES CARE)")
        
        self.add_log_line("🚀 Auto-fixing safe issues...")
        
        # Store issues for reference
        self.current_issues = issues
        
        # Auto-fix safe issues only
        for issue in safe_issues:
            self.add_log_line(f"� Fixing: {issue['description']}")
            self.fix_single_issue(issue)
        
        # For risky issues, just generate files without applying
        for issue in risky_issues:
            if issue['type'] == 'apply_management_cr':
                self.add_log_line(f"⚠️ Risky: {issue['description']} - check CR file manually")
    
    def fix_single_issue(self, issue):
        """Fix a single identified issue"""
        try:
            if issue['type'] == 'deploy_cr' and issue['safe']:
                # VM not running, safe to apply install action
                self.apply_local_cr(issue['cr_name'])
            elif issue['type'] == 'create_cr':
                # Generate CR for orphaned VM
                self.generate_cr_for_vm(issue['vm_name'])
            elif issue['type'] == 'apply_management_cr':
                # This is risky - just log what should be done
                self.add_log_line(f"⚠️ Manual action needed for {issue['vm_name']}")
                self.add_log_line(f"   Check CR file {issue['cr_name']} and apply carefully")
                
        except Exception as e:
            self.add_log_line(f"❌ Error fixing issue: {e}")
    
    def apply_manifests(self, button):
        """Apply all local CR manifests to the cluster"""
        try:
            status_report = get_comprehensive_status()
            local_crs = status_report['local_crs']
            
            if not local_crs:
                self.add_log_line("❌ No local CR files found to apply")
                return
                
            self.add_log_line(f"🚀 Applying {len(local_crs)} local CR manifests...")
            
            success_count = 0
            for cr_name, cr_data in local_crs.items():
                try:
                    file_path = os.path.join("/root/kubernetes-installer/manifest-controller", cr_data['file'])
                    self.apply_local_cr_file(file_path)
                    success_count += 1
                except Exception as e:
                    self.add_log_line(f"❌ Failed to apply {cr_name}: {e}")
            
            self.add_log_line(f"✅ Successfully applied {success_count}/{len(local_crs)} CRs")
            
            # Refresh status after applying
            self.loop.set_alarm_in(2.0, lambda loop, user_data: self.update_status_display())
            
        except Exception as e:
            self.add_log_line(f"❌ Error applying manifests: {e}")
    
    def apply_local_cr(self, cr_name):
        """Apply a specific local CR by name"""
        try:
            # Find the CR file
            manifest_dir = "/root/kubernetes-installer/manifest-controller"
            for file in os.listdir(manifest_dir):
                if file.endswith('-cr.yaml') or file.endswith('cr.yaml'):
                    file_path = os.path.join(manifest_dir, file)
                    with open(file_path, 'r') as f:
                        cr_data = yaml.safe_load(f)
                        if cr_data and cr_data.get('metadata', {}).get('name') == cr_name:
                            self.apply_local_cr_file(file_path)
                            return
            
            raise Exception(f"CR file for {cr_name} not found")
            
        except Exception as e:
            self.add_log_line(f"❌ Error applying CR {cr_name}: {e}")
    
    def apply_local_cr_file(self, file_path):
        """Apply a CR file using kubectl"""
        try:
            import subprocess
            
            result = subprocess.run(
                ['kubectl', 'apply', '-f', file_path],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                self.add_log_line(f"✅ Applied manifest: {os.path.basename(file_path)}")
                self.add_log_line(f"   Output: {result.stdout.strip()}")
            else:
                self.add_log_line(f"❌ Failed to apply {os.path.basename(file_path)}")
                self.add_log_line(f"   Error: {result.stderr.strip()}")
                
        except subprocess.TimeoutExpired:
            self.add_log_line(f"⏰ Timeout applying {os.path.basename(file_path)}")
        except Exception as e:
            self.add_log_line(f"❌ Error applying {os.path.basename(file_path)}: {e}")
    
    def generate_cr_for_vm(self, vm_name):
        """Generate a WindowsVM CR for an orphaned VM"""
        try:
            # Get VM details from cluster
            k8s_api = client.CustomObjectsApi()
            vm = k8s_api.get_namespaced_custom_object(
                group="kubevirt.io",
                version="v1",
                namespace="kubevirt",
                plural="virtualmachines",
                name=vm_name
            )
            
            # Extract relevant information
            vm_spec = vm.get('spec', {})
            template_spec = vm_spec.get('template', {}).get('spec', {})
            
            # Generate WindowsVM CR with uninstall action (to manage without reinstalling)
            cr_manifest = {
                'apiVersion': f'{CRD_GROUP}/{CRD_VERSION}',
                'kind': CRD_KIND,
                'metadata': {
                    'name': f"{vm_name}-mgmt",  # Management CR
                    'namespace': 'default'
                },
                'spec': {
                    'vmName': vm_name,
                    'action': 'uninstall',  # Use uninstall to manage without reinstalling
                    'kubevirt_namespace': 'kubevirt',
                    'windows_version': '2025',  # Default assumption
                    'vm_cpu_cores': 4,  # Default values
                    'vm_memory': '8Gi',
                    'system_disk_size': '40Gi',
                    'installer_disk_size': '15Gi',
                    'virtio_iso_size': '500Mi',
                    'image': 'win2025server.vhdx',
                    'vhdx_path': '/data/vms/win2025server.vhdx',
                    'storage_dir': '/data/vms',
                    'windows_admin_password': 'Secret123%%',
                    'windows_product_key': 'XXXXX-XXXXX-XXXXX-XXXXX-XXXXX'
                }
            }
            
            # Save generated CR to file
            filename = f"{vm_name}-mgmt-cr.yaml"
            file_path = os.path.join("/root/kubernetes-installer/manifest-controller", filename)
            
            with open(file_path, 'w') as f:
                yaml.dump(cr_manifest, f, default_flow_style=False)
            
            self.add_log_line(f"📄 Generated management CR file: {filename}")
            self.add_log_line(f"   VM: {vm_name} → CR: {cr_manifest['metadata']['name']} (action: uninstall)")
            self.add_log_line(f"   💡 Use action 'uninstall' to manage existing VM without reinstalling")
            
            # Optionally apply immediately with user confirmation
            self.add_log_line(f"   📝 CR saved to file, apply manually when ready")
            
        except Exception as e:
            self.add_log_line(f"❌ Error generating CR for {vm_name}: {e}")
    
    def show_dialog(self, title, prompt, callback):
        """Show an input dialog"""
        edit_widget = urwid.Edit(prompt + "\n")
        
        def ok_pressed(button):
            callback(edit_widget.edit_text)
            self.close_dialog()
            
        def cancel_pressed(button):
            self.close_dialog()
        
        ok_btn = urwid.Button("OK", on_press=ok_pressed)
        cancel_btn = urwid.Button("Cancel", on_press=cancel_pressed)
        ok_btn = urwid.AttrMap(ok_btn, 'button', 'button_focus')
        cancel_btn = urwid.AttrMap(cancel_btn, 'button', 'button_focus')
        
        buttons = urwid.Columns([ok_btn, cancel_btn], dividechars=2)
        content = urwid.Pile([edit_widget, urwid.Text(""), buttons])
        dialog = urwid.LineBox(content, title=title)
        
        self.overlay = urwid.Overlay(
            dialog, self.main_frame,
            align='center', width=40,
            valign='middle', height=8
        )
        self.loop.widget = self.overlay
    
    def close_dialog(self):
        """Close the current dialog"""
        self.loop.widget = self.main_frame
        if hasattr(self, 'overlay'):
            delattr(self, 'overlay')
    
    def refresh_display(self, button):
        """Refresh the display"""
        self.add_log_line("=== Refreshing display ===")
        self.list_vms(button)
    
    def clear_logs(self, button):
        """Clear the log display"""
        self.log_walker.clear()
        self.add_log_line("=== Logs cleared ===")
    
    def quit_app(self, button):
        """Quit the application"""
        raise urwid.ExitMainLoop()
    
    def update_focus_indicators(self):
        """Update visual indicators to show which panel is focused"""
        try:
            current_focus = self.content_columns.focus_position
            
            if current_focus == 0:
                # Status panel focused
                self.status_frame.set_title("VM/CR Status Overview [FOCUSED]")
                self.log_frame.set_title("Kopf Operator Logs")
            else:
                # Log panel focused
                self.status_frame.set_title("VM/CR Status Overview")
                self.log_frame.set_title("Kopf Operator Logs [FOCUSED]")
                
            # Force screen update
            if self.loop:
                self.loop.draw_screen()
        except Exception as e:
            pass  # Ignore errors in focus update
    
    def input_filter(self, keys, raw):
        """Filter and process input keys"""
        # Convert raw input to proper key names for better handling
        filtered_keys = []
        for key in keys:
            if isinstance(key, str):
                # Handle special key combinations
                if key == '\x03':  # CTRL+C
                    filtered_keys.append('ctrl c')
                elif key == '\t':  # Tab
                    filtered_keys.append('tab')
                elif key == '\x1b[Z':  # Shift+Tab
                    filtered_keys.append('shift tab')
                else:
                    filtered_keys.append(key)
            else:
                filtered_keys.append(key)
        return filtered_keys
    
    def unhandled_input(self, key):
        if key in ('q', 'Q'):
            raise urwid.ExitMainLoop()
        elif key == 'ctrl c':
            # Handle CTRL+C properly
            self.add_log_line("🛑 CTRL+C pressed - Shutting down...")
            raise urwid.ExitMainLoop()
        elif key == 'tab':
            # Switch between status and log panels
            current_focus = self.content_columns.focus_position
            new_focus = 1 - current_focus  # Toggle between 0 and 1
            self.content_columns.focus_position = new_focus
            self.update_focus_indicators()
            
            # Update visual feedback
            if new_focus == 0:
                self.add_log_line("📊 Switched to Status Panel")
            else:
                self.add_log_line("📜 Switched to Log Panel")
        elif key == 'shift tab':
            # Reverse tab (same as tab for 2-panel layout)
            current_focus = self.content_columns.focus_position
            new_focus = 1 - current_focus
            self.content_columns.focus_position = new_focus
            self.update_focus_indicators()
            
            if new_focus == 0:
                self.add_log_line("📊 Switched to Status Panel")
            else:
                self.add_log_line("📜 Switched to Log Panel")
        elif key == 'f5':
            # F5 - Refresh everything
            self.update_status_display()
            self.add_log_line("🔄 Display refreshed (F5)")
        elif key == 'f2':
            # F2 - Show status view
            self.show_status(None)
        elif key == 'f3':
            # F3 - List VMs
            self.list_vms(None)
        elif key == 'f4':
            # F4 - Clear logs
            self.clear_logs(None)
        elif key == 'f6':
            # F6 - Fix Issues
            self.fix_issues(None)
        elif key == 'f7':
            # F7 - Apply CRs
            self.apply_manifests(None)
        elif key == 'f8':
            # F8 - Toggle auto-scroll
            self.auto_scroll = not self.auto_scroll
            status = "ON" if self.auto_scroll else "OFF"
            self.add_log_line(f"📜 Auto-scroll: {status} (F8)")
            if self.auto_scroll and self.log_walker:
                # If turning auto-scroll back on, scroll to bottom
                try:
                    self.log_listbox.focus_position = len(self.log_walker) - 1
                except:
                    pass
        elif key in ('left', 'right'):
            # Arrow keys for panel navigation
            if key == 'left':
                self.content_columns.focus_position = 0
                self.update_focus_indicators()
                self.add_log_line("📊 Moved to Status Panel (←)")
            else:
                self.content_columns.focus_position = 1
                self.update_focus_indicators()
                self.add_log_line("📜 Moved to Log Panel (→)")
        elif key in ('up', 'down', 'page up', 'page down'):
            # Handle scrolling in logs panel - disable auto-scroll when manually scrolling
            if self.content_columns.focus_position == 1:  # Logs panel is focused
                self.auto_scroll = False  # Disable auto-scroll when user manually scrolls
            # Let these keys pass through to focused panel for scrolling
            return key
        elif key == 'enter':
            # Handle enter key in focused panel
            return key
        else:
            # Let other keys pass through
            return key
    
    def run(self):
        """Run the TUI"""
        import signal
        
        def handle_sigint(signum, frame):
            # Graceful shutdown
            self.add_log_line("🛑 Received SIGINT - Shutting down gracefully...")
            raise urwid.ExitMainLoop()
        
        def handle_sigterm(signum, frame):
            self.add_log_line("🛑 Received SIGTERM - Shutting down gracefully...")
            raise urwid.ExitMainLoop()
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, handle_sigint)
        signal.signal(signal.SIGTERM, handle_sigterm)
        
        self.loop = urwid.MainLoop(
            self.main_frame,
            self.palette,
            unhandled_input=self.unhandled_input,
            handle_mouse=True,  # Enable mouse for better interaction
            input_filter=self.input_filter  # Add input filter for better key handling
        )
        
        # Welcome message and initial status
        self.add_log_line("=== WindowsVM Kopf Operator Console Started ===")
        self.add_log_line("Monitoring Kubernetes WindowsVM Custom Resources...")
        
        # Load initial status display
        self.loop.set_alarm_in(0.2, lambda loop, user_data: self.initial_startup())
        
        # Start log updates after the loop is fully initialized
        self.loop.set_alarm_in(0.5, lambda loop, user_data: self.update_logs())
        
        # Start a timer to periodically refresh the entire screen to prevent corruption
        def refresh_screen(loop, user_data):
            try:
                # Force a complete redraw to fix any screen corruption
                loop.screen.clear()
                loop.draw_screen()
                # Schedule next refresh
                loop.set_alarm_in(2.0, refresh_screen)  # Increased interval to reduce flicker
            except:
                pass
        
        self.loop.set_alarm_in(2.0, refresh_screen)
        
        try:
            self.loop.run()
        except KeyboardInterrupt:
            pass
        finally:
            # Ensure we reset the terminal properly
            if hasattr(self.loop.screen, 'stop'):
                self.loop.screen.stop()

# --- Main Function ---
def run_kopf_operator():
    """Run the Kopf operator in a separate thread"""
    try:
        load_kube_config()
        logger.info("Starting Kopf operator thread...")
        
        # Run Kopf operator
        kopf.run(
            clusterwide=False,
            namespace=os.getenv('WATCH_NAMESPACE', 'default'),
            standalone=True,
        )
    except Exception as e:
        logger.error(f"Kopf operator error: {e}")

def main():
    """Main entry point"""
    global tui_app
    
    # Start Kopf operator in background thread
    kopf_thread = threading.Thread(target=run_kopf_operator, daemon=True)
    kopf_thread.start()
    
    # Give Kopf a moment to start
    time.sleep(2)
    
    # Start TUI
    tui_app = WindowsVMTUI()
    tui_app.run()

if __name__ == "__main__":
    main()
