"""
Kubernetes client utilities
"""

import logging
from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)

def load_kube_config():
    """Load Kubernetes configuration"""
    try:
        config.load_incluster_config()
        logger.info("Loaded in-cluster Kubernetes config")
    except config.ConfigException:
        config.load_kube_config()
        logger.info("Loaded local Kubernetes config")

def get_k8s_client():
    """Get Kubernetes API client"""
    return client.CustomObjectsApi()

def vm_exists(vm_name, kubevirt_namespace="kubevirt"):
    """Check if a VirtualMachine exists in KubeVirt"""
    try:
        k8s_api = get_k8s_client()
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
    """Get detailed VM status from KubeVirt"""
    try:
        k8s_api = get_k8s_client()
        
        # Get VM status
        vm_status = {
            'exists': False,
            'is_running': False,
            'vm_phase': 'Unknown',
            'vmi_phase': 'Unknown',
            'ready': False,
            'printable_status': 'Unknown'
        }
        
        try:
            vm = k8s_api.get_namespaced_custom_object(
                group="kubevirt.io",
                version="v1",
                namespace=kubevirt_namespace,
                plural="virtualmachines",
                name=vm_name
            )
            vm_status['exists'] = True
            vm_status['ready'] = vm.get('status', {}).get('ready', False)
            vm_status['printable_status'] = vm.get('status', {}).get('printableStatus', 'Unknown')
        except ApiException as e:
            if e.status != 404:
                raise
        
        # Get VMI status if exists
        try:
            vmi = k8s_api.get_namespaced_custom_object(
                group="kubevirt.io",
                version="v1",
                namespace=kubevirt_namespace,
                plural="virtualmachineinstances",
                name=vm_name
            )
            vm_status['vmi_phase'] = vmi.get('status', {}).get('phase', 'Unknown')
            vm_status['is_running'] = vmi.get('status', {}).get('phase') == 'Running'
        except ApiException as e:
            if e.status != 404:
                raise
        
        return vm_status
        
    except Exception as e:
        logger.error(f"Error getting VM status for {vm_name}: {e}")
        raise
