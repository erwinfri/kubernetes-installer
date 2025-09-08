


# --- Utility Functions ---
def load_kube_config():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

def ensure_crd_exists(api_ext):
    try:
        api_ext.read_custom_resource_definition(CRD_NAME)
        print(f"\033[92m✓ CRD {CRD_NAME} already exists.\033[0m")
    except ApiException as e:
        if e.status == 404:
            print(f"\033[93mCRD {CRD_NAME} not found. Creating...\033[0m")
            api_ext.create_custom_resource_definition(CRD_TEMPLATE)
            print(f"\033[92m✓ CRD {CRD_NAME} created.\033[0m")
        else:
            raise

def run_playbook(spec):
    print(f"\033[94m→ Running playbook for VM: {spec.get('vmName', 'unknown')}\033[0m")
    print(f"\033[93m[DEBUG] Parameters passed to playbook:\n{yaml.dump(spec)}\033[0m")
    # Check if the VirtualMachine already exists
    vm_name = spec.get('vmName')
    kubevirt_namespace = spec.get('kubevirt_namespace', 'kubevirt')
    if not vm_name:
        print("\033[91mNo vmName specified in spec, skipping playbook.\033[0m")
        return
    try:
        load_kube_config()
        k8s_api = client.CustomObjectsApi()
        vm = k8s_api.get_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=kubevirt_namespace,
            plural="virtualmachines",
            name=vm_name
        )
        print(f"\033[93m[INFO] VirtualMachine {vm_name} already exists in namespace {kubevirt_namespace}. Skipping playbook.\033[0m")
        return
    except ApiException as e:
        if e.status != 404:
            print(f"\033[91mError checking for existing VM: {e}\033[0m")
            return
        # If 404, VM does not exist, proceed
    # Run playbook if VM does not exist
    cmd = ["ansible-playbook", PLAYBOOK_PATH]
    for k, v in spec.items():
        # Convert booleans and numbers to strings for shell
        if isinstance(v, bool):
            v = str(v).lower()
        elif v is None:
            continue
        cmd.extend(["-e", f"{k}={v}"])
    print(f"\033[93m[DEBUG] ansible-playbook command: {' '.join(cmd)}\033[0m")
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            print(f"\033[92m{line.rstrip()}\033[0m")
        process.wait()
        if process.returncode != 0:
            print(f"\033[91mPlaybook failed with exit code {process.returncode}\033[0m")
    except Exception as e:
        print(f"\033[91mPlaybook execution error: {e}\033[0m")

# --- Imports ---
import os
import time
import yaml
import subprocess
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from deepdiff import DeepDiff  # For config drift detection

# --- Configuration ---
CRD_GROUP = "infra.example.com"
CRD_VERSION = "v1"
CRD_PLURAL = "windowsvms"
CRD_KIND = "WindowsVM"
NAMESPACE = "default"
CRD_NAME = f"{CRD_PLURAL}.{CRD_GROUP}"
CRD_TEMPLATE = {
    "apiVersion": "apiextensions.k8s.io/v1",
    "kind": "CustomResourceDefinition",
    "metadata": {"name": CRD_NAME},
    "spec": {
        "group": CRD_GROUP,
        "names": {
            "kind": CRD_KIND,
            "plural": CRD_PLURAL,
            "singular": "windowsvm",
            "listKind": "WindowsVMList"
        },
        "scope": "Namespaced",
        "versions": [
            {
                "name": CRD_VERSION,
                "served": True,
                "storage": True,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "image": {"type": "string"},
                                    "installer_disk_size": {"type": "string"},
                                    "kubevirt_namespace": {"type": "string"},
                                    "storage_dir": {"type": "string"},
                                    "system_disk_size": {"type": "string"},
                                    "vhdx_download_url": {"type": "string"},
                                    "vhdx_path": {"type": "string"},
                                    "virtio_iso_size": {"type": "string"},
                                    "virtio_iso_url": {"type": "string"},
                                    "vm_cpu_cores": {"type": "integer"},
                                    "vm_memory": {"type": "string"},
                                    "vmName": {"type": "string"},
                                    "windows_admin_password": {"type": "string"},
                                    "windows_product_key": {"type": "string"},
                                    "action": {"type": "string"},
                                    "windows_version": {"type": "string"}
                                }
                            }
                        }
                    }
                },
                "subresources": {"status": {}}
            }
        ]
    }
}
PLAYBOOK_PATH = "/root/kubernetes-installer/windows-server-controller.yaml"

# --- Utility Functions ---
def load_kube_config():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

def ensure_crd_exists(api_ext):
    try:
        api_ext.read_custom_resource_definition(CRD_NAME)
        print(f"\033[92m✓ CRD {CRD_NAME} already exists.\033[0m")
    except ApiException as e:
        if e.status == 404:
            print(f"\033[93mCRD {CRD_NAME} not found. Creating...\033[0m")
            api_ext.create_custom_resource_definition(CRD_TEMPLATE)
            print(f"\033[92m✓ CRD {CRD_NAME} created.\033[0m")
        else:
            raise

def run_playbook(spec):
    print(f"\033[94m→ Running playbook for VM: {spec.get('vmName', 'unknown')}\033[0m")
    print(f"\033[93m[DEBUG] Parameters passed to playbook:\n{yaml.dump(spec)}\033[0m")
    # Check if the VirtualMachine already exists
    vm_name = spec.get('vmName')
    kubevirt_namespace = spec.get('kubevirt_namespace', 'kubevirt')
    if not vm_name:
        print("\033[91mNo vmName specified in spec, skipping playbook.\033[0m")
        return
    try:
        load_kube_config()
        k8s_api = client.CustomObjectsApi()
        vm = k8s_api.get_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=kubevirt_namespace,
            plural="virtualmachines",
            name=vm_name
        )
        print(f"\033[93m[INFO] VirtualMachine {vm_name} already exists in namespace {kubevirt_namespace}. Skipping playbook.\033[0m")
        return
    except ApiException as e:
        if e.status != 404:
            print(f"\033[91mError checking for existing VM: {e}\033[0m")
            return
        # If 404, VM does not exist, proceed
    # Run playbook if VM does not exist
    cmd = ["ansible-playbook", PLAYBOOK_PATH]
    for k, v in spec.items():
        # Convert booleans and numbers to strings for shell
        if isinstance(v, bool):
            v = str(v).lower()
        elif v is None:
            continue
        cmd.extend(["-e", f"{k}={v}"])
    print(f"\033[93m[DEBUG] ansible-playbook command: {' '.join(cmd)}\033[0m")
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            print(f"\033[92m{line.rstrip()}\033[0m")
        process.wait()
        if process.returncode != 0:
            print(f"\033[91mPlaybook failed with exit code {process.returncode}\033[0m")
    except Exception as e:
        print(f"\033[91mPlaybook execution error: {e}\033[0m")

# --- Drift Detection ---

def read_yaml_file(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def get_live_resource(api, group, version, namespace, plural, name):
    return api.get_namespaced_custom_object(group, version, namespace, plural, name)

def check_drift(manifest_path, api, group, version, namespace, plural, name):
    import json
    try:
        desired = read_yaml_file(manifest_path)
        live = get_live_resource(api, group, version, namespace, plural, name)
        # --- Layer 1: Manifest vs. Live CR (spec only) ---
        desired_spec = desired.get('spec', {})
        live_spec = live.get('spec', {})
        diff_spec = DeepDiff(desired_spec, live_spec, ignore_order=True)
        if diff_spec:
            pretty_diff = json.dumps(diff_spec, indent=2, default=str)
            print(f"\033[91m[DRIFT][CRD] Spec drift detected between manifest and live CR for {name}:\n{pretty_diff}\033[0m")
            print(f"\033[93mTo fix: kubectl apply -f {manifest_path}\033[0m")
        else:
            print(f"\033[92m[DRIFT][CRD] No spec drift between manifest and live CR for {name}.\033[0m")
    except Exception as e:
        print(f"\033[91m[DRIFT][CRD] Error checking manifest vs. live CR for {name}: {e}\033[0m")

# --- Main Execution ---
def main():
    load_kube_config()
    api_ext = client.ApiextensionsV1Api()
    custom_api = client.CustomObjectsApi()

    ensure_crd_exists(api_ext)

    w = watch.Watch()
    print("\033[96mWatching for WindowsVM resources...\033[0m")
    for event in w.stream(
        custom_api.list_namespaced_custom_object,
        group=CRD_GROUP,
        version=CRD_VERSION,
        namespace=NAMESPACE,
        plural=CRD_PLURAL,
        timeout_seconds=0
    ):
        obj = event['object']
        event_type = event['type']
        name = obj['metadata']['name']
        spec = obj.get('spec', {})
        print(f"\033[95mEvent: {event_type} {name}\033[0m")

        if event_type in ["ADDED", "MODIFIED"]:
            run_playbook(spec)

            # --- Layer 2: Live CR vs. Actual VM resources ---
            try:
                kubevirt_namespace = spec.get('kubevirt_namespace', 'kubevirt')
                vm_name = spec.get('vmName')
                from kubernetes import config as k8s_config, client as k8s_client
                try:
                    k8s_config.load_kube_config()
                except Exception:
                    k8s_config.load_incluster_config()
                core_api = k8s_client.CoreV1Api()
                vmi_api = k8s_client.CustomObjectsApi()
                vmi = vmi_api.get_namespaced_custom_object(
                    group="kubevirt.io",
                    version="v1",
                    namespace=kubevirt_namespace,
                    plural="virtualmachineinstances",
                    name=vm_name
                )
                # Map volume name to PVC claimName
                pvc_map = {}
                for vol in vmi.get('spec', {}).get('volumes', []):
                    if 'persistentVolumeClaim' in vol:
                        pvc_map[vol['name']] = vol['persistentVolumeClaim']['claimName']
                # Get actual PVC sizes
                actual_sizes = {}
                for vol_name, pvc_name in pvc_map.items():
                    try:
                        pvc = core_api.read_namespaced_persistent_volume_claim(pvc_name, kubevirt_namespace)
                        size = pvc.spec.resources.requests.get('storage')
                        actual_sizes[vol_name] = size
                    except Exception as e:
                        print(f"\033[91m[DRIFT][VM] Error reading PVC {pvc_name}: {e}\033[0m")
                print(f"\033[94m[DEBUG][VM] Actual PVC sizes attached to VMI: {actual_sizes}\033[0m")
                # Compare CR spec to actual PVC sizes
                drift = False
                # Map CR keys to expected volume names
                cr_to_vol = {
                    'system_disk_size': 'disk0',
                    'virtio_iso_size': 'virtio-iso',
                    # Add more mappings if needed
                }
                for cr_key, vol_name in cr_to_vol.items():
                    cr_size = spec.get(cr_key)
                    actual_size = actual_sizes.get(vol_name)
                    if cr_size and actual_size:
                        if cr_size != actual_size:
                            print(f"\033[91m[DRIFT][VM] {cr_key} drift: CR={cr_size}, Actual={actual_size} (volume: {vol_name})\033[0m")
                            drift = True
                        else:
                            print(f"\033[92m[DRIFT][VM] {cr_key} matches: {cr_size} (volume: {vol_name})\033[0m")
                    elif cr_size and not actual_size:
                        print(f"\033[93m[DRIFT][VM] No actual PVC found for {cr_key} (expected volume: {vol_name})\033[0m")
                if not drift:
                    print(f"\033[92m[DRIFT][VM] No disk size drift detected between CR and actual VM resources.\033[0m")
            except Exception as e:
                print(f"\033[91m[DRIFT][VM] Error during disk drift check: {e}\033[0m")

            # --- Drift detection: also check manifest-controller/ subdir ---
            manifest_candidates = [
                f"./{name}.yaml",
                f"./{name}-cr.yaml",
                f"/root/kubernetes-installer/{name}.yaml",
                f"/root/kubernetes-installer/{name}-cr.yaml",
                f"/root/kubernetes-installer/manifest-controller/{name}.yaml",
                f"/root/kubernetes-installer/manifest-controller/{name}-cr.yaml"
            ]
            for manifest_candidate in manifest_candidates:
                if os.path.exists(manifest_candidate):
                    check_drift(manifest_candidate, custom_api, CRD_GROUP, CRD_VERSION, NAMESPACE, CRD_PLURAL, name)
                    break
            else:
                print(f"\033[93m[DRIFT] No manifest file found for {name} to check drift.\033[0m")
        elif event_type == "DELETED":
            print(f"\033[93mResource {name} deleted.\033[0m")

# --- Entrypoint ---
if __name__ == "__main__":
    main()
