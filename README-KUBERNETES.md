# Kubernetes / KubeVirt / Windows Server Controllers (Quick Commands)

Minimal copy/paste cheat sheet for lifecycle actions.

## Kubernetes
Controller file: k8s-redhat-kubernetes-controller.yaml

Install:
ansible-playbook k8s-redhat-kubernetes-controller.yaml -e k8s_action=install

Status:
ansible-playbook k8s-redhat-kubernetes-controller.yaml -e k8s_action=status

Uninstall:
ansible-playbook k8s-redhat-kubernetes-controller.yaml -e k8s_action=uninstall

## KubeVirt
Controller file: k8s-redhat-kubevirt-controller.yaml

Install:
ansible-playbook k8s-redhat-kubevirt-controller.yaml -e kubevirt_action=install

Status:
ansible-playbook k8s-redhat-kubevirt-controller.yaml -e kubevirt_action=status

Uninstall:
ansible-playbook k8s-redhat-kubevirt-controller.yaml -e kubevirt_action=uninstall

## Windows Server VM (KubeVirt)

### Option 1: Windows Server 2019 Only
Controller file: windows-server-controller.yaml

Install:
ansible-playbook windows-server-controller.yaml -e action=install

Status:
ansible-playbook windows-server-controller.yaml -e action=status

Uninstall:
ansible-playbook windows-server-controller.yaml -e action=uninstall

Reinstall:
ansible-playbook windows-server-controller.yaml -e action=reinstall

### Option 2: Unified Controller (Windows 2019 & 2025)
Controller file: windows-server-unified-controller.yaml

Install Windows 2019:
ansible-playbook windows-server-unified-controller.yaml -e action=install -e windows_version=2019

Install Windows 2025:
ansible-playbook windows-server-unified-controller.yaml -e action=install -e windows_version=2025

Status Windows 2019:
ansible-playbook windows-server-unified-controller.yaml -e action=status -e windows_version=2019

Status Windows 2025:
ansible-playbook windows-server-unified-controller.yaml -e action=status -e windows_version=2025

Uninstall Windows 2019:
ansible-playbook windows-server-unified-controller.yaml -e action=uninstall -e windows_version=2019

Uninstall Windows 2025:
ansible-playbook windows-server-unified-controller.yaml -e action=uninstall -e windows_version=2025

Reinstall Windows 2019:
ansible-playbook windows-server-unified-controller.yaml -e action=reinstall -e windows_version=2019

Reinstall Windows 2025:
ansible-playbook windows-server-unified-controller.yaml -e action=reinstall -e windows_version=2025

---
Variables (examples):
- Set hostname for Kubernetes install: -e k8s_hostname=myhost.example.com
- Override KubeVirt namespace: -e kubevirt_namespace=kubevirt
- Windows password: -e admin_password='StrongP@ss1'
- Windows product key: -e product_key='XXXXX-XXXXX-XXXXX-XXXXX-XXXXX'
- Custom VHD URL (Windows 2019): -e vhd_url='https://custom-server.com/win2019.vhd'
- Custom VHDX URL (Windows 2025): -e vhd_url='https://custom-server.com/win2025.vhdx'

## Complete Deployment Examples

### Full Stack Deployment (Kubernetes + KubeVirt + Windows):
```bash
# 1. Install Kubernetes
ansible-playbook k8s-redhat-kubernetes-controller.yaml -e k8s_action=install

# 2. Install KubeVirt
ansible-playbook k8s-redhat-kubevirt-controller.yaml -e kubevirt_action=install

# 3. Deploy Windows Server 2025
ansible-playbook windows-server-unified-controller.yaml -e action=install -e windows_version=2025
```

### Windows Version Differences:
- **Windows 2019**: Uses .vhd files, 8Gi RAM, 4 CPU cores, VirtIO drivers (2k19/amd64)
- **Windows 2025**: Uses .vhdx files, 12Gi RAM, 6 CPU cores, VirtIO drivers (w11/amd64, 2k22/amd64)

### Download URLs (Auto-downloaded if files don't exist):
- **Windows 2019**: Microsoft evaluation VHD (~6GB)
- **Windows 2025**: Microsoft evaluation VHDX (~8GB)

## File Structure

### Controllers:
- `k8s-redhat-kubernetes-controller.yaml` - Kubernetes cluster management
- `k8s-redhat-kubevirt-controller.yaml` - KubeVirt virtualization platform
- `windows-server-controller.yaml` - Windows Server 2019 only
- `windows-server-unified-controller.yaml` - Windows Server 2019 & 2025

### Task Files (windows-server/ directory):
- `windows-server-2019-install.yaml` - Windows 2019 installation
- `windows-server-2019-status.yaml` - Windows 2019 status checks  
- `windows-server-2019-uninstall.yaml` - Windows 2019 removal
- `windows-server-2025-install.yaml` - Windows 2025 installation
- `windows-server-2025-status.yaml` - Windows 2025 status checks
- `windows-server-2025-uninstall.yaml` - Windows 2025 removal

## Troubleshooting

### Kopf status patch warnings (inconsistencies)
If you see messages like "Patching failed with inconsistencies" from Kopf when updating CR status, ensure your CRDs:
- Enable the status subresource
- Define a `status` schema

Example CRD snippet:
```yaml
versions:
	- name: v1
		served: true
		storage: true
		subresources:
			status: {}
		schema:
			openAPIV3Schema:
				type: object
				properties:
					spec:
						type: object
						# ...
					status:
						type: object
						properties:
							phase:
								type: string
							message:
								type: string
							reason:
								type: string
							observedGeneration:
								type: integer
							conditions:
								type: array
								items:
									type: object
									properties:
										type:
											type: string
										status:
											type: string
											enum: ["True", "False", "Unknown"]
										lastTransitionTime:
											type: string
											format: date-time
										reason:
											type: string
										message:
											type: string
```

Then re-apply the CRD:
```bash
kubectl apply -f <crd-file>.yaml
```

### Stuck VM Deletion:
If VMs get stuck during uninstall, use the manual cleanup script:
```bash
./cleanup-stuck-vm.sh
```

### Check VM Status:
```bash
kubectl get vm,vmi,pods -n kubevirt
```

### Access VM Console:
```bash
# VNC access
virtctl vnc win2019server -n kubevirt
virtctl vnc win2025server -n kubevirt

# Serial console
virtctl console win2019server -n kubevirt
virtctl console win2025server -n kubevirt
```
