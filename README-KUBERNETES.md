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
Controller file: windows-server-controller.yaml

Install:
ansible-playbook windows-server-controller.yaml -e action=install

Status:
ansible-playbook windows-server-controller.yaml -e action=status

Uninstall:
ansible-playbook windows-server-controller.yaml -e action=uninstall

Reinstall:
ansible-playbook windows-server-controller.yaml -e action=reinstall

---
Variables (examples):
- Set hostname for Kubernetes install: -e k8s_hostname=myhost.example.com
- Override KubeVirt namespace: -e kubevirt_namespace=kubevirt
- Windows password: -e admin_password='StrongP@ss1'
- Windows product key: -e product_key='XXXXX-XXXXX-XXXXX-XXXXX-XXXXX'
