#!/usr/bin/env python3
"""
Test script for the Kopf WindowsVM Operator
This script demonstrates how to create, update, and delete WindowsVM CRs
that will be automatically processed by the Kopf operator.
"""

import yaml
import time
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# Configuration
CRD_GROUP = "infra.example.com"
CRD_VERSION = "v1"
CRD_PLURAL = "windowsvms"
NAMESPACE = "default"

def load_kube_config():
    """Load Kubernetes configuration"""
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

def create_test_vm_cr(vm_name, action="install"):
    """Create a test WindowsVM Custom Resource"""
    load_kube_config()
    k8s_api = client.CustomObjectsApi()
    
    # Define the WindowsVM CR
    vm_cr = {
        "apiVersion": f"{CRD_GROUP}/{CRD_VERSION}",
        "kind": "WindowsVM",
        "metadata": {
            "name": vm_name,
            "namespace": NAMESPACE
        },
        "spec": {
            "vmName": vm_name,
            "action": action,
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
    
    try:
        # Create the CR
        result = k8s_api.create_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=NAMESPACE,
            plural=CRD_PLURAL,
            body=vm_cr
        )
        print(f"✓ Created WindowsVM CR: {vm_name}")
        return result
    except ApiException as e:
        if e.status == 409:
            print(f"⚠ WindowsVM CR {vm_name} already exists")
        else:
            print(f"✗ Error creating WindowsVM CR {vm_name}: {e}")
        return None

def update_vm_cr(vm_name, new_action):
    """Update a WindowsVM CR action"""
    load_kube_config()
    k8s_api = client.CustomObjectsApi()
    
    try:
        # Get current CR
        cr = k8s_api.get_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=NAMESPACE,
            plural=CRD_PLURAL,
            name=vm_name
        )
        
        # Update the action
        cr['spec']['action'] = new_action
        
        # Update the CR
        result = k8s_api.patch_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=NAMESPACE,
            plural=CRD_PLURAL,
            name=vm_name,
            body=cr
        )
        print(f"✓ Updated WindowsVM CR {vm_name} action to: {new_action}")
        return result
    except ApiException as e:
        print(f"✗ Error updating WindowsVM CR {vm_name}: {e}")
        return None

def delete_vm_cr(vm_name):
    """Delete a WindowsVM CR"""
    load_kube_config()
    k8s_api = client.CustomObjectsApi()
    
    try:
        k8s_api.delete_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=NAMESPACE,
            plural=CRD_PLURAL,
            name=vm_name
        )
        print(f"✓ Deleted WindowsVM CR: {vm_name}")
        return True
    except ApiException as e:
        if e.status == 404:
            print(f"⚠ WindowsVM CR {vm_name} not found")
        else:
            print(f"✗ Error deleting WindowsVM CR {vm_name}: {e}")
        return False

def get_vm_cr_status(vm_name):
    """Get the status of a WindowsVM CR"""
    load_kube_config()
    k8s_api = client.CustomObjectsApi()
    
    try:
        cr = k8s_api.get_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=NAMESPACE,
            plural=CRD_PLURAL,
            name=vm_name
        )
        
        status = cr.get('status', {})
        phase = status.get('phase', 'Unknown')
        message = status.get('message', 'No status message')
        
        print(f"📊 VM {vm_name} status: {phase}")
        print(f"   Message: {message}")
        return status
    except ApiException as e:
        if e.status == 404:
            print(f"⚠ WindowsVM CR {vm_name} not found")
        else:
            print(f"✗ Error getting WindowsVM CR {vm_name} status: {e}")
        return None

def list_vm_crs():
    """List all WindowsVM CRs"""
    load_kube_config()
    k8s_api = client.CustomObjectsApi()
    
    try:
        result = k8s_api.list_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=NAMESPACE,
            plural=CRD_PLURAL
        )
        
        crs = result.get('items', [])
        print(f"📋 Found {len(crs)} WindowsVM CRs:")
        
        for cr in crs:
            name = cr['metadata']['name']
            action = cr['spec'].get('action', 'unknown')
            status = cr.get('status', {}).get('phase', 'Unknown')
            print(f"   • {name}: action={action}, status={status}")
            
        return crs
    except ApiException as e:
        print(f"✗ Error listing WindowsVM CRs: {e}")
        return []

def main():
    """Demonstrate Kopf operator functionality"""
    print("🚀 WindowsVM Kopf Operator Test Script")
    print("=" * 50)
    
    # Test VM name
    test_vm_name = "kopf-test-vm"
    
    print("\n1. Listing existing WindowsVM CRs:")
    list_vm_crs()
    
    print(f"\n2. Creating test WindowsVM CR: {test_vm_name}")
    create_test_vm_cr(test_vm_name, "install")
    
    print("\n3. Waiting for operator to process... (10 seconds)")
    time.sleep(10)
    
    print(f"\n4. Checking status of {test_vm_name}:")
    get_vm_cr_status(test_vm_name)
    
    print(f"\n5. Updating {test_vm_name} action to 'uninstall':")
    update_vm_cr(test_vm_name, "uninstall")
    
    print("\n6. Waiting for operator to process update... (10 seconds)")
    time.sleep(10)
    
    print(f"\n7. Checking updated status of {test_vm_name}:")
    get_vm_cr_status(test_vm_name)
    
    print(f"\n8. Deleting test WindowsVM CR: {test_vm_name}")
    delete_vm_cr(test_vm_name)
    
    print("\n9. Final listing of WindowsVM CRs:")
    list_vm_crs()
    
    print("\n✅ Test completed!")
    print("\nNote: If the Kopf operator is running, you should see")
    print("status updates and corresponding Ansible playbook executions.")

if __name__ == "__main__":
    main()
