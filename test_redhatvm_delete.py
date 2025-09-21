#!/usr/bin/env python3

import sys
import subprocess
import os
sys.path.append('/root/kubernetes-installer')

print("=== TESTING DELETE CR CALLBACK FOR REDHATVM ===")

file_name = "rhel9-redhatvm-cr.yaml"
file_path = "/root/kubernetes-installer/manifest-controller/rhel9-redhatvm-cr.yaml"

print(f"File: {file_name}")
print(f"Path: {file_path}")
print(f"File exists: {os.path.exists(file_path)}")

print("\n=== TESTING KUBECTL DELETE ===")
try:
    result = subprocess.run(['kubectl', 'delete', '-f', file_path], 
                          capture_output=True, text=True, timeout=10)
    print(f"Return code: {result.returncode}")
    print(f"Stdout: {result.stdout}")
    print(f"Stderr: {result.stderr}")
except subprocess.TimeoutExpired:
    print("❌ kubectl delete command timed out!")
except Exception as e:
    print(f"❌ Error running kubectl delete: {e}")

print("\n=== TESTING VM CLEANUP CHECK ===")
try:
    result = subprocess.run(['kubectl', 'get', 'vmi', '-o', 'json'], 
                          capture_output=True, text=True, timeout=5)
    print(f"VMI check return code: {result.returncode}")
    if result.returncode == 0:
        import json
        vmis = json.loads(result.stdout)
        vm_count = len(vmis.get('items', []))
        print(f"Found {vm_count} running VMs")
    else:
        print(f"VMI check failed: {result.stderr}")
except subprocess.TimeoutExpired:
    print("❌ kubectl get vmi command timed out!")
except Exception as e:
    print(f"❌ Error checking VMs: {e}")

print("\n=== TEST COMPLETED ===")