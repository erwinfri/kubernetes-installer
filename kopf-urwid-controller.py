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
            timestamp = datetime.now().strftime("%H:%M:%S")
            log_queue.put(f"[{timestamp}] {msg}")
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
                    logger.info(f"[ANSIBLE] {clean_line}")
                    
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
    """Handle WindowsVM CR creation"""
    logger.info(f"WindowsVM CR created: {name} in namespace {namespace}")
    
    vm_name = spec.get('vmName')
    action = spec.get('action', 'install')
    kubevirt_namespace = spec.get('kubevirt_namespace', 'kubevirt')
    
    if not vm_name:
        error_msg = "No vmName specified in spec"
        logger.error(error_msg)
        update_cr_status(name, namespace, "Failed", error_msg)
        return {"message": error_msg}
    
    # Update status to processing
    update_cr_status(name, namespace, "Processing", f"Starting {action} for VM {vm_name}")
    
    try:
        if action == 'install':
            # Check detailed VM status instead of just existence
            vm_status = get_vm_status(vm_name, kubevirt_namespace)
            
            if vm_status['exists']:
                if vm_status['is_running']:
                    msg = f"VM {vm_name} already exists and is running in namespace {kubevirt_namespace}"
                    logger.info(msg)
                    update_cr_status(name, namespace, "Running", msg)
                    return {"message": msg}
                else:
                    msg = f"VM {vm_name} exists but not running (phase: {vm_status.get('vmi_phase', 'Unknown')})"
                    logger.info(msg)
                    update_cr_status(name, namespace, "Exists", msg)
                    return {"message": msg}
            
            # Run installation playbook
            success = await run_ansible_playbook(spec, "install")
            
            if success:
                update_cr_status(name, namespace, "Completed", f"VM {vm_name} deployed successfully")
                return {"message": f"VM {vm_name} deployed successfully"}
            else:
                update_cr_status(name, namespace, "Failed", f"Failed to deploy VM {vm_name}")
                raise kopf.PermanentError(f"Failed to deploy VM {vm_name}")
                
        elif action == 'uninstall':
            # Run uninstall playbook
            success = await run_ansible_playbook(spec, "uninstall")
            
            if success:
                update_cr_status(name, namespace, "Completed", f"VM {vm_name} uninstalled successfully")
                return {"message": f"VM {vm_name} uninstalled successfully"}
            else:
                update_cr_status(name, namespace, "Failed", f"Failed to uninstall VM {vm_name}")
                raise kopf.PermanentError(f"Failed to uninstall VM {vm_name}")
        else:
            error_msg = f"Unknown action: {action}. Supported actions: install, uninstall"
            logger.error(error_msg)
            update_cr_status(name, namespace, "Failed", error_msg)
            raise kopf.PermanentError(error_msg)
            
    except Exception as e:
        error_msg = f"Error processing VM {vm_name}: {str(e)}"
        logger.error(error_msg)
        update_cr_status(name, namespace, "Failed", error_msg)
        raise kopf.TemporaryError(error_msg, delay=60)

@kopf.on.update(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
async def update_vm(spec, name, namespace, old, new, diff, logger, **kwargs):
    """Handle WindowsVM CR updates"""
    logger.info(f"WindowsVM CR updated: {name} in namespace {namespace}")
    
    vm_name = spec.get('vmName')
    action = spec.get('action', 'install')
    
    if not vm_name:
        error_msg = "No vmName specified in spec"
        logger.error(error_msg)
        update_cr_status(name, namespace, "Failed", error_msg)
        return {"message": error_msg}
    
    # Log what changed
    for operation, field_path, old_value, new_value in diff:
        logger.info(f"Field {field_path} {operation}: {old_value} -> {new_value}")
    
    # Update status to processing
    update_cr_status(name, namespace, "Processing", f"Updating VM {vm_name} with action {action}")
    
    try:
        # Run playbook with updated spec
        success = await run_ansible_playbook(spec, action)
        
        if success:
            update_cr_status(name, namespace, "Completed", f"VM {vm_name} updated successfully")
            return {"message": f"VM {vm_name} updated successfully"}
        else:
            update_cr_status(name, namespace, "Failed", f"Failed to update VM {vm_name}")
            raise kopf.PermanentError(f"Failed to update VM {vm_name}")
            
    except Exception as e:
        error_msg = f"Error updating VM {vm_name}: {str(e)}"
        logger.error(error_msg)
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
        ]
        
        self.setup_ui()
        
    def setup_ui(self):
        # Header
        header = urwid.Text(('header', 'WindowsVM Kopf Operator Console'), align='center')
        header = urwid.AttrMap(header, 'header')
        
        # Menu bar
        menu_items = [
            ('List VMs', self.list_vms),
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
        
        # Log display
        self.log_walker = urwid.SimpleFocusListWalker([])
        self.log_listbox = urwid.ListBox(self.log_walker)
        log_frame = urwid.LineBox(self.log_listbox, title="Kopf Operator Logs")
        
        # Footer
        footer_text = "Navigate: Tab/Shift+Tab | Select: Enter | Q: Quit | F5: Refresh | Kopf Operator Active"
        footer = urwid.Text(('footer', footer_text), align='center')
        footer = urwid.AttrMap(footer, 'footer')
        
        # Main layout with fixed header/menu and scrollable log area
        top_section = urwid.Pile([
            ('pack', header),
            ('pack', urwid.Text("")),  # Spacer
            ('pack', menu_frame),
            ('pack', separator),
            ('pack', urwid.Text("")),  # Spacer
        ])
        
        main_pile = urwid.Pile([
            ('pack', top_section),
            ('weight', 1, log_frame),
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
        
        # Auto-scroll to bottom
        if self.log_walker:
            self.log_listbox.focus_position = len(self.log_walker) - 1
            
        # Don't force screen refresh - let urwid handle it naturally
    
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
            
        # Schedule next update
        if self.loop and hasattr(self.loop, 'set_alarm_in'):
            self.loop.set_alarm_in(0.3, lambda loop, user_data: self.update_logs())
    
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
    
    def unhandled_input(self, key):
        if key in ('q', 'Q'):
            raise urwid.ExitMainLoop()
        elif key == 'f5':
            self.refresh_display(None)
    
    def run(self):
        """Run the TUI"""
        import signal
        
        def handle_sigint(signum, frame):
            raise urwid.ExitMainLoop()
        
        signal.signal(signal.SIGINT, handle_sigint)
        
        self.loop = urwid.MainLoop(
            self.main_frame,
            self.palette,
            unhandled_input=self.unhandled_input,
            handle_mouse=False  # Disable mouse to avoid conflicts
        )
        
        # Welcome message
        self.add_log_line("=== WindowsVM Kopf Operator Console Started ===")
        self.add_log_line("Monitoring Kubernetes WindowsVM Custom Resources...")
        
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
