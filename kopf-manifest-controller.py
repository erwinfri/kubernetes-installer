#!/usr/bin/env python3
"""
Kopf-based Kubernetes Operator for WindowsVM Custom Resources
This operator automatically manages Windows VM deployments using KubeVirt
when WindowsVM CRs are created, updated, or deleted.
"""

import os
import kopf
import yaml
import subprocess
import asyncio
import logging
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# --- Configuration ---
CRD_GROUP = "infra.example.com"
CRD_VERSION = "v1"
CRD_PLURAL = "windowsvms"
CRD_KIND = "WindowsVM"
PLAYBOOK_PATH = "/root/kubernetes-installer/windows-server-controller.yaml"
MANIFEST_CONTROLLER_DIR = "/root/kubernetes-installer/manifest-controller"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

async def run_ansible_playbook(spec, action="install"):
    """Run the Ansible playbook with given spec"""
    vm_name = spec.get('vmName', 'unknown')
    logger.info(f"Running Ansible playbook for VM: {vm_name}, action: {action}")
    
    # Prepare the spec with action - convert kopf.Spec to dict
    playbook_spec = dict(spec)
    playbook_spec['action'] = action
    
    # Build ansible-playbook command
    cmd = ["ansible-playbook", PLAYBOOK_PATH]
    for k, v in playbook_spec.items():
        if isinstance(v, bool):
            v = str(v).lower()
        elif v is None:
            continue
        cmd.extend(["-e", f"{k}={v}"])
    
    logger.info(f"Executing: {' '.join(cmd)}")
    
    try:
        # Run the playbook asynchronously
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        
        # Stream output
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            logger.info(f"Playbook output: {line.decode().strip()}")
        
        await process.wait()
        
        if process.returncode == 0:
            logger.info(f"Playbook completed successfully for VM: {vm_name}")
            return True
        else:
            logger.error(f"Playbook failed with exit code {process.returncode} for VM: {vm_name}")
            return False
            
    except Exception as e:
        logger.error(f"Error running playbook for VM {vm_name}: {e}")
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
            # Check if VM already exists
            if vm_exists(vm_name, kubevirt_namespace):
                msg = f"VirtualMachine {vm_name} already exists in namespace {kubevirt_namespace}"
                logger.info(msg)
                update_cr_status(name, namespace, "Completed", msg)
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
        if vm_exists(vm_name, kubevirt_namespace):
            logger.info(f"VM {vm_name} exists, running uninstall playbook")
            
            # Run uninstall playbook
            uninstall_spec = dict(spec)
            uninstall_spec['action'] = 'uninstall'
            success = await run_ansible_playbook(uninstall_spec, "uninstall")
            
            if success:
                logger.info(f"VM {vm_name} uninstalled successfully")
                return {"message": f"VM {vm_name} uninstalled successfully"}
            else:
                logger.error(f"Failed to uninstall VM {vm_name}")
                raise kopf.PermanentError(f"Failed to uninstall VM {vm_name}")
        else:
            logger.info(f"VM {vm_name} does not exist, cleanup not needed")
            return {"message": f"VM {vm_name} does not exist, cleanup not needed"}
            
    except Exception as e:
        error_msg = f"Error during VM cleanup for {vm_name}: {str(e)}"
        logger.error(error_msg)
        raise kopf.TemporaryError(error_msg, delay=60)

@kopf.on.field(CRD_GROUP, CRD_VERSION, CRD_PLURAL, field='spec.action')
async def action_changed(old, new, spec, name, namespace, logger, **kwargs):
    """Handle when the action field changes"""
    if old != new:
        logger.info(f"Action changed for {name}: {old} -> {new}")
        vm_name = spec.get('vmName')
        
        if vm_name:
            update_cr_status(name, namespace, "Processing", f"Action changed to {new} for VM {vm_name}")

# --- Health and Status Monitoring ---

@kopf.timer(CRD_GROUP, CRD_VERSION, CRD_PLURAL, interval=300)  # Every 5 minutes
async def monitor_vm_status(spec, name, namespace, logger, **kwargs):
    """Monitor VM status and update CR status accordingly"""
    vm_name = spec.get('vmName')
    kubevirt_namespace = spec.get('kubevirt_namespace', 'kubevirt')
    
    if not vm_name:
        return
    
    try:
        load_kube_config()
        vm_deployed = vm_exists(vm_name, kubevirt_namespace)
        
        # Get current CR to check status
        k8s_api = client.CustomObjectsApi()
        cr = k8s_api.get_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=namespace,
            plural=CRD_PLURAL,
            name=name
        )
        
        current_status = cr.get('status', {}).get('phase', 'Unknown')
        expected_action = spec.get('action', 'install')
        
        # Update status based on actual VM state
        if expected_action == 'install':
            if vm_deployed and current_status != 'Completed':
                update_cr_status(name, namespace, "Completed", f"VM {vm_name} is running")
                logger.info(f"VM {vm_name} detected as running, updated status")
            elif not vm_deployed and current_status == 'Completed':
                update_cr_status(name, namespace, "Failed", f"VM {vm_name} is not running but should be")
                logger.warning(f"VM {vm_name} should be running but is not found")
        
        elif expected_action == 'uninstall':
            if not vm_deployed and current_status != 'Completed':
                update_cr_status(name, namespace, "Completed", f"VM {vm_name} has been removed")
                logger.info(f"VM {vm_name} detected as removed, updated status")
    
    except Exception as e:
        logger.error(f"Error monitoring VM {vm_name}: {e}")

# --- Event Filtering ---

@kopf.on.event(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
def log_events(event, logger, **kwargs):
    """Log all WindowsVM events for debugging"""
    event_type = event.get('type', 'UNKNOWN')
    obj = event.get('object', {})
    name = obj.get('metadata', {}).get('name', 'unknown')
    namespace = obj.get('metadata', {}).get('namespace', 'unknown')
    
    logger.debug(f"Event {event_type} for WindowsVM {namespace}/{name}")

# --- Main Entry Point ---
if __name__ == "__main__":
    # Load Kubernetes config
    load_kube_config()
    
    # Start the operator
    logger.info("Starting WindowsVM Kopf Operator...")
    kopf.run(
        clusterwide=False,  # Run namespace-scoped
        namespace=os.getenv('WATCH_NAMESPACE', 'default'),  # Watch specific namespace
        standalone=True,    # Run as standalone process
    )
