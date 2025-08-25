#!/bin/bash
# Manual cleanup script for stuck Windows VM resources

VM_NAME="win2019server"
NAMESPACE="kubevirt"

echo "🧹 Manual Windows VM Cleanup Script"
echo "VM: $VM_NAME in namespace: $NAMESPACE"
echo

# Function to remove finalizers and force delete
force_delete_resource() {
    local resource_type=$1
    local resource_name=$2
    local namespace_flag=$3
    
    echo "Checking $resource_type: $resource_name..."
    if kubectl get $resource_type $resource_name $namespace_flag &>/dev/null; then
        echo "  Found $resource_type, removing finalizers..."
        kubectl patch $resource_type $resource_name $namespace_flag --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
        echo "  Force deleting $resource_type..."
        kubectl delete $resource_type $resource_name $namespace_flag --force --grace-period=0 2>/dev/null || true
        echo "  ✅ $resource_type cleanup attempted"
    else
        echo "  ❌ $resource_type not found"
    fi
    echo
}

# Clean up VMI first
force_delete_resource "vmi" "$VM_NAME" "-n $NAMESPACE"

# Clean up VM
force_delete_resource "vm" "$VM_NAME" "-n $NAMESPACE"

# Clean up any stuck pods
echo "Checking for stuck launcher pods..."
PODS=$(kubectl get pods -n $NAMESPACE -l kubevirt.io/vm=$VM_NAME -o name 2>/dev/null)
if [ -n "$PODS" ]; then
    echo "Found launcher pods, force deleting..."
    echo "$PODS" | xargs -r kubectl delete -n $NAMESPACE --force --grace-period=0
else
    echo "No launcher pods found"
fi
echo

# Clean up PVCs
echo "Cleaning up PVCs..."
for pvc in "win2019server-installer-pvc" "win2019server-system-pvc" "win2019server-virtio-iso-pvc"; do
    force_delete_resource "pvc" "$pvc" "-n $NAMESPACE"
done

# Clean up PVs
echo "Cleaning up PVs..."
for pv in "win2019server-installer-pv" "win2019server-system-pv" "win2019server-virtio-iso-pv"; do
    force_delete_resource "pv" "$pv" ""
done

# Clean up services
echo "Cleaning up services..."
for svc in "win2019server-vnc" "win2019server-rdp"; do
    force_delete_resource "service" "$svc" "-n $NAMESPACE"
done

# Clean up secrets
force_delete_resource "secret" "win2019server-sysprep" "-n $NAMESPACE"

echo "🏁 Cleanup completed!"
echo "Wait a few seconds, then check status with:"
echo "kubectl get vm,vmi,pods -n $NAMESPACE"
