# --- CRD/KubeVirt Constants ---
CRD_GROUP = "infra.example.com"
CRD_VERSION = "v1"
CRD_PLURAL = "windowsvms"
CRD_KIND = "WindowsVM"
NAMESPACE = "default"
CRD_NAME = f"{CRD_PLURAL}.{CRD_GROUP}"

import os
import time
import yaml
import subprocess
import signal
import urwid
import glob
from kubernetes import client, config
from kubernetes.client.rest import ApiException



# --- Utility Functions ---
def load_kube_config():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

def ensure_crd_exists(api_ext):
    """Ensure CRD exists, preferring file-based deployment"""
    return deploy_crd_from_file(api_ext, MANIFEST_CONTROLLER_DIR)

# --- CRD Template ---
CRD_TEMPLATE = {
    "metadata": {"name": "windowsvms.infra.example.com"},
    "spec": {
        "group": "infra.example.com",
        "names": {
            "kind": "WindowsVM",
            "plural": "windowsvms",
            "singular": "windowsvm",
            "listKind": "WindowsVMList"
        },
        "scope": "Namespaced",
        "versions": [
            {
                "name": "v1",
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
MANIFEST_CONTROLLER_DIR = "/root/kubernetes-installer/manifest-controller"

# --- Utility Functions ---
def load_kube_config():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

def load_cr_files_from_directory(directory_path):
    """Load Custom Resource YAML files from a directory"""
    cr_files = []
    if not os.path.exists(directory_path):
        print(f"\033[93mDirectory {directory_path} does not exist\033[0m")
        return cr_files
    
    # Look for CR files (not CRD files)
    yaml_files = glob.glob(os.path.join(directory_path, "*-cr.yaml"))
    for file_path in yaml_files:
        try:
            with open(file_path, 'r') as f:
                cr_data = yaml.safe_load(f)
                if cr_data and cr_data.get('kind') == 'WindowsVM':
                    cr_files.append({
                        'data': cr_data,
                        'file_path': file_path,
                        'source': 'file'
                    })
                    print(f"\033[92m✓ Loaded CR from {file_path}\033[0m")
        except Exception as e:
            print(f"\033[91mError loading CR from {file_path}: {e}\033[0m")
    
    return cr_files

def deploy_crd_from_file(api_ext, directory_path):
    """Deploy CRD from file if it doesn't exist"""
    crd_file_path = os.path.join(directory_path, "win2025-vm-crd.yaml")
    
    try:
        # Check if CRD already exists
        api_ext.read_custom_resource_definition(CRD_NAME)
        print(f"\033[92m✓ CRD {CRD_NAME} already exists.\033[0m")
        return True
    except ApiException as e:
        if e.status == 404:
            print(f"\033[93mCRD {CRD_NAME} not found. Attempting to create from file...\033[0m")
            
            # Try to load CRD from file
            if os.path.exists(crd_file_path):
                try:
                    with open(crd_file_path, 'r') as f:
                        crd_data = yaml.safe_load(f)
                        api_ext.create_custom_resource_definition(body=crd_data)
                        print(f"\033[92m✓ CRD {CRD_NAME} created from file {crd_file_path}.\033[0m")
                        return True
                except Exception as file_error:
                    print(f"\033[91mError creating CRD from file: {file_error}\033[0m")
                    print(f"\033[93mFalling back to template CRD...\033[0m")
            
            # Fallback to template CRD
            try:
                api_ext.create_custom_resource_definition(CRD_TEMPLATE)
                print(f"\033[92m✓ CRD {CRD_NAME} created from template.\033[0m")
                return True
            except Exception as template_error:
                print(f"\033[91mError creating CRD from template: {template_error}\033[0m")
                return False
        else:
            print(f"\033[91mError checking CRD: {e}\033[0m")
            return False

def deploy_cr_to_cluster(custom_api, cr_data, namespace=NAMESPACE):
    """Deploy a Custom Resource to the cluster"""
    try:
        # Extract metadata
        cr_name = cr_data['metadata']['name']
        
        # Check if CR already exists
        try:
            existing_cr = custom_api.get_namespaced_custom_object(
                group=CRD_GROUP,
                version=CRD_VERSION,
                namespace=namespace,
                plural=CRD_PLURAL,
                name=cr_name
            )
            print(f"\033[93mCR {cr_name} already exists in cluster.\033[0m")
            return existing_cr
        except ApiException as e:
            if e.status == 404:
                # CR doesn't exist, create it
                cr_data['metadata']['namespace'] = namespace
                created_cr = custom_api.create_namespaced_custom_object(
                    group=CRD_GROUP,
                    version=CRD_VERSION,
                    namespace=namespace,
                    plural=CRD_PLURAL,
                    body=cr_data
                )
                print(f"\033[92m✓ CR {cr_name} deployed to cluster.\033[0m")
                return created_cr
            else:
                raise
                
    except Exception as e:
        print(f"\033[91mError deploying CR to cluster: {e}\033[0m")
        return None

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


# --- Agent Mode: Ensure all WindowsVM CRs are deployed as VMs ---
def vm_exists(vm_name, kubevirt_namespace="kubevirt"):
    k8s_api = client.CustomObjectsApi()
    try:
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
        raise

def deploy_vm(spec):
    cmd = ["ansible-playbook", PLAYBOOK_PATH]
    for k, v in spec.items():
        if isinstance(v, bool):
            v = str(v).lower()
        elif v is None:
            continue
        cmd.extend(["-e", f"{k}={v}"])
    print(f"→ Deploying VM: {spec.get('vmName', 'unknown')}")
    subprocess.run(cmd)

# --- Main Execution ---
def main():
    load_kube_config()
    api_ext = client.ApiextensionsV1Api()
    custom_api = client.CustomObjectsApi()

    ensure_crd_exists(api_ext)


        # Prepare data for TUI
    vm_entries = []
    for cr in crs:
        spec = cr.get('spec', {})
        vm_name = spec.get('vmName')
        kubevirt_namespace = spec.get('kubevirt_namespace', 'kubevirt')
        if not vm_name:
            vm_entries.append({
                'label': "No vmName in CR, skipping.",
                'status': 'error',
                'spec': spec,
                'kubevirt_namespace': kubevirt_namespace
            })
            continue
        deployed = vm_exists(vm_name, kubevirt_namespace)
        label = f"{'✓' if deployed else '✗'} {vm_name}: {'Deployed' if deployed else 'Not deployed'}"
        vm_entries.append({
            'label': label,
            'status': 'deployed' if deployed else 'not_deployed',
            'spec': spec,
            'vm_name': vm_name,
            'kubevirt_namespace': kubevirt_namespace
        })

    run_tui(vm_entries)


# --- Urwid TUI Implementation ---
class AutoSelectListBox(urwid.ListBox):
    """Custom ListBox that automatically updates selection on focus change"""
    def __init__(self, walker, app):
        super().__init__(walker)
        self.app = app
        self._last_focus_position = None
        # Set up connection to detect focus changes
        urwid.connect_signal(walker, 'modified', self._on_walker_modified)
    
    def _on_walker_modified(self):
        """Called when the walker is modified - check for focus changes"""
        if hasattr(self, 'focus_position') and self.focus_position != self._last_focus_position:
            self._last_focus_position = self.focus_position
            if hasattr(self.app, 'current_selection'):
                self.app.current_selection = self.focus_position
                self.app.update_details(self.focus_position)
    
    def keypress(self, size, key):
        # Handle the keypress first
        result = super().keypress(size, key)
        
        # Check if focus position changed and update selection
        if hasattr(self, 'focus_position') and self.focus_position != self._last_focus_position:
            self._last_focus_position = self.focus_position
            if hasattr(self.app, 'current_selection'):
                self.app.current_selection = self.focus_position
                self.app.update_details(self.focus_position)
        
        return result
    
    def mouse_event(self, size, event, button, col, row, focus):
        """Handle mouse events and update selection"""
        result = super().mouse_event(size, event, button, col, row, focus)
        
        # Check if focus position changed and update selection
        if hasattr(self, 'focus_position') and self.focus_position != self._last_focus_position:
            self._last_focus_position = self.focus_position
            if hasattr(self.app, 'current_selection'):
                self.app.current_selection = self.focus_position
                self.app.update_details(self.focus_position)
        
        return result

class VMListWidget(urwid.ListWalker):
    def __init__(self, vm_entries):
        self.vm_entries = vm_entries
        self.focus = 0
        
    def get_focus(self):
        return self._get_at_pos(self.focus)
        
    def set_focus(self, focus):
        self.focus = focus
        
    def get_next(self, start_from):
        return self._get_at_pos(start_from + 1)
        
    def get_prev(self, start_from):
        return self._get_at_pos(start_from - 1)
        
    def _get_at_pos(self, pos):
        if pos < 0 or pos >= len(self.vm_entries):
            return None, None
        return self._make_vm_widget(pos), pos
        
    def _make_vm_widget(self, pos):
        entry = self.vm_entries[pos]
        label = entry.get('label', f'VM {pos+1}')
        status = entry.get('status', 'unknown')
        
        if status == 'deployed':
            attr = 'deployed'
        elif status == 'not_deployed':
            attr = 'not_deployed'
        elif status == 'available':
            attr = 'available'
        else:
            attr = 'error'
            
        return urwid.AttrMap(urwid.Text(label), attr, 'selected')

class VMApp:
    def __init__(self, vm_entries):
        self.vm_entries = vm_entries
        self.current_selection = 0
        
        # Create palette
        self.palette = [
            ('header', 'white', 'dark blue'),
            ('deployed', 'light green', 'black'),
            ('not_deployed', 'light red', 'black'),
            ('available', 'light blue', 'black'),
            ('error', 'yellow', 'black'),
            ('selected', 'black', 'light gray'),
            ('details', 'light cyan', 'black'),
            ('button', 'black', 'light gray'),
            ('button_focus', 'white', 'dark red'),
        ]
        
        self.setup_ui()
        
    def setup_ui(self):
        # Header
        header = urwid.Text(('header', 'WindowsVM Deployment Status'), align='center')
        header = urwid.AttrMap(header, 'header')
        
        # VM List
        if self.vm_entries:
            vm_list_items = []
            for i, entry in enumerate(self.vm_entries):
                label = entry.get('label', f'VM {i+1}')
                status = entry.get('status', 'unknown')
                
                if status == 'deployed':
                    attr = 'deployed'
                elif status == 'not_deployed':
                    attr = 'not_deployed'
                else:
                    attr = 'error'
                    
                button = urwid.Button(label, on_press=self.vm_selected, user_data=i)
                button = urwid.AttrMap(button, attr, 'selected')
                vm_list_items.append(button)
        else:
            vm_list_items = [urwid.Text('No VMs found')]
            
        self.vm_listbox = AutoSelectListBox(urwid.SimpleFocusListWalker(vm_list_items), self)
        vm_list_frame = urwid.LineBox(self.vm_listbox, title="VM List")
        
        # Details panel
        self.details_text = urwid.Text("Select a VM to see details")
        details_filler = urwid.Filler(self.details_text, valign='top')
        self.details_frame = urwid.LineBox(details_filler, title="Manifest Details")
        
        # Redeploy and Edit buttons
        self.redeploy_btn = urwid.Button("Redeploy Selected VM", on_press=self.redeploy_vm)
        redeploy_btn_attr = urwid.AttrMap(self.redeploy_btn, 'button', 'button_focus')
        
        self.edit_btn = urwid.Button("Edit Manifest", on_press=lambda x: self.edit_manifest())
        edit_btn_attr = urwid.AttrMap(self.edit_btn, 'button', 'button_focus')
        
        buttons_column = urwid.Columns([redeploy_btn_attr, edit_btn_attr], dividechars=1)
        
        # Details column
        details_pile = urwid.Pile([
            ('weight', 4, self.details_frame),
            ('pack', buttons_column)
        ])
        
        # Main layout
        columns = urwid.Columns([
            ('weight', 1, vm_list_frame),
            ('weight', 1, details_pile)
        ], dividechars=1)
        
        # Footer
        footer = urwid.Text("Navigate: ↑/↓  Select: Enter  Edit: E  Redeploy: R  Uninstall: U  Refresh: F5  Quit: Q")
        footer = urwid.AttrMap(footer, 'header')
        
        # Main frame
        self.main_frame = urwid.Frame(
            body=columns,
            header=header,
            footer=footer
        )
        
        # Update details for first item
        if self.vm_entries:
            self.update_details(0)
    
    def sync_selection_from_focus(self):
        """Sync current_selection with the actual listbox focus position"""
        if hasattr(self, 'vm_listbox') and hasattr(self.vm_listbox, 'focus_position'):
            if 0 <= self.vm_listbox.focus_position < len(self.vm_entries):
                old_selection = self.current_selection
                self.current_selection = self.vm_listbox.focus_position
                # Update details if selection changed
                if old_selection != self.current_selection:
                    self.update_details(self.current_selection)
                return True
        return False

    def vm_selected(self, button, user_data):
        """Handle VM selection from the list"""
        if user_data is not None and 0 <= user_data < len(self.vm_entries):
            self.current_selection = user_data
            # Also sync the listbox focus position
            if hasattr(self, 'vm_listbox'):
                self.vm_listbox.focus_position = user_data
            self.update_details(user_data)
        else:
            self.current_selection = 0
            if hasattr(self, 'vm_listbox'):
                self.vm_listbox.focus_position = 0
            if self.vm_entries:
                self.update_details(0)
    
    def update_details(self, vm_index):
        if 0 <= vm_index < len(self.vm_entries):
            entry = self.vm_entries[vm_index]
            spec = entry.get('spec', {})
            vm_name = entry.get('vm_name', f'VM {vm_index + 1}')
            status = entry.get('status', 'unknown')
            
            # Add header showing which VM is selected
            details_lines = [
                f"🔷 SELECTED: {vm_name} (#{vm_index + 1})",
                f"Status: {status}",
                "=" * 40
            ]
            
            if spec:
                for key, value in spec.items():
                    line = f"{key}: {value}"
                    if len(line) > 50:
                        line = line[:47] + "..."
                    details_lines.append(line)
                details_text = '\n'.join(details_lines)
            else:
                details_lines.append("No spec available")
                details_text = '\n'.join(details_lines)
                
            self.details_text.set_text(('details', details_text))
        else:
            self.details_text.set_text("Select a VM to see details")
    
    def redeploy_vm(self, button):
        # Sync selection from current focus position first
        self.sync_selection_from_focus()
        
        if 0 <= self.current_selection < len(self.vm_entries):
            entry = self.vm_entries[self.current_selection]
            spec = entry.get('spec')
            vm_name = entry.get('vm_name', 'Unknown VM')
            
            if spec:
                # Show confirmation dialog
                self.show_redeploy_dialog(vm_name, spec, entry)
    
    def edit_manifest(self):
        """Open manifest editor for the selected VM"""
        try:
            # Sync selection from current focus position first
            self.sync_selection_from_focus()
            
            if 0 <= self.current_selection < len(self.vm_entries):
                entry = self.vm_entries[self.current_selection]
                spec = entry.get('spec', {})
                vm_name = entry.get('vm_name', 'Unknown VM')
                
                if spec:
                    self.show_edit_dialog(vm_name, spec)
                else:
                    self.show_error_dialog("No specification found for this VM")
            else:
                self.show_error_dialog(f"No VM selected (current: {self.current_selection}, total: {len(self.vm_entries)})")
        except Exception as e:
            self.show_error_dialog(f"Error opening edit dialog: {str(e)}")
    
    def show_edit_dialog(self, vm_name, original_spec):
        """Show dialog to edit manifest parameters"""
        try:
            # Store the original spec for reference
            self.original_spec = original_spec.copy()
            
            # Create edit fields for key parameters
            edit_fields = []
            self.edit_widgets = {}
            
            # Define editable parameters with their descriptions
            editable_params = [
                ('vmName', 'VM Name'),
                ('vm_memory', 'Memory (e.g., 4Gi)'),
                ('vm_cpu_cores', 'CPU Cores'),
                ('windows_version', 'Windows Version'),
                ('system_disk_size', 'System Disk Size'),
                ('windows_admin_password', 'Admin Password'),
                ('kubevirt_namespace', 'KubeVirt Namespace'),
                ('storage_dir', 'Storage Directory'),
                ('vhdx_path', 'VHDX Path'),
                ('windows_product_key', 'Product Key')
            ]
            
            for param, description in editable_params:
                current_value = str(original_spec.get(param, ''))
                edit_widget = urwid.Edit(f"{description}: ", current_value)
                self.edit_widgets[param] = edit_widget
                edit_fields.append(urwid.AttrMap(edit_widget, 'details'))
                # No dividers between fields to avoid sizing issues
            
            # Create buttons
            def save_pressed(button):
                try:
                    self.save_edited_manifest()
                except Exception as e:
                    self.show_error_dialog(f"Error saving manifest: {str(e)}")
                    
            def cancel_pressed(button):
                self.close_dialog()
            
            save_btn = urwid.Button("Save & Redeploy", on_press=save_pressed)
            cancel_btn = urwid.Button("Cancel", on_press=cancel_pressed)
            save_btn = urwid.AttrMap(save_btn, 'button', 'button_focus')
            cancel_btn = urwid.AttrMap(cancel_btn, 'button', 'button_focus')
            
            buttons = urwid.Columns([save_btn, cancel_btn], dividechars=1)
            
            # Create scrollable list for edit fields
            if edit_fields:
                edit_listbox = urwid.ListBox(urwid.SimpleFocusListWalker(edit_fields))
            else:
                edit_listbox = urwid.ListBox(urwid.SimpleFocusListWalker([urwid.Text("No editable fields")]))
            
            # Create the main content with simple structure
            content = urwid.Pile([
                ('weight', 1, edit_listbox),
                ('pack', urwid.Text("")),  # Simple spacer instead of Divider
                ('pack', buttons)
            ])
            
            dialog = urwid.LineBox(content, title=f"Edit Manifest: {vm_name}")
            
            # Create custom dialog handler for proper input handling
            class EditDialogHandler(urwid.WidgetWrap):
                def __init__(self, widget, app):
                    super().__init__(widget)
                    self.app = app
                
                def keypress(self, size, key):
                    # Handle escape to cancel
                    if key == 'esc':
                        self.app.close_dialog()
                        return None
                    # Handle all arrow keys and navigation within the dialog
                    elif key in ('left', 'right', 'up', 'down', 'home', 'end', 'page up', 'page down'):
                        # Let the dialog handle navigation keys
                        return super().keypress(size, key)
                    # Handle other special keys that should work in edit fields
                    elif key in ('backspace', 'delete', 'enter', 'tab', 'shift tab'):
                        return super().keypress(size, key)
                    # Handle printable characters
                    elif len(key) == 1 or key.startswith('ctrl '):
                        return super().keypress(size, key)
                    # Block other keys from propagating to main interface
                    else:
                        return None
            
            dialog_handler = EditDialogHandler(dialog, self)
            
            self.overlay = urwid.Overlay(
                dialog_handler, self.main_frame,
                align='center', width=('relative', 90),
                valign='middle', height=('relative', 70)
            )
            self.loop.widget = self.overlay
            
        except Exception as e:
            self.show_error_dialog(f"Error opening edit dialog: {str(e)}")

    def uninstall_vm(self, button):
        """Uninstall the selected VM"""
        # Sync selection from current focus position first
        self.sync_selection_from_focus()
        
        if 0 <= self.current_selection < len(self.vm_entries):
            entry = self.vm_entries[self.current_selection]
            spec = entry.get('spec')
            vm_name = entry.get('vm_name', 'Unknown VM')
            
            if spec:
                # Show confirmation dialog
                self.show_uninstall_dialog(vm_name, spec, entry)

    def show_uninstall_dialog(self, vm_name, spec, entry=None):
        """Show confirmation dialog for uninstalling VM"""
        message = f"⚠️ UNINSTALL VM '{vm_name}'?\n\nThis will permanently remove the VM and all its data.\n\nContinue?"
        
        def yes_pressed(button):
            self.close_dialog()
            # Start uninstall process
            spec_copy = spec.copy()
            spec_copy['action'] = 'uninstall'
            # Ensure vm_name is in the spec for ansible
            spec_copy['vm_name'] = vm_name
            spec_copy['vmName'] = vm_name  # Some playbooks might use vmName instead
            self.show_deployment_window(spec_copy, vm_name, is_edit=False, entry=entry)
            
        def no_pressed(button):
            self.close_dialog()
        
        yes_btn = urwid.Button("Uninstall", on_press=yes_pressed)
        no_btn = urwid.Button("Cancel", on_press=no_pressed)
        yes_btn = urwid.AttrMap(yes_btn, 'button', 'button_focus')
        no_btn = urwid.AttrMap(no_btn, 'button', 'button_focus')
        
        buttons = urwid.Columns([
            ('pack', yes_btn),
            ('pack', urwid.Text("  ")),
            ('pack', no_btn)
        ])
        
        content = urwid.Pile([
            ('pack', urwid.Text(message)),
            ('pack', urwid.Text("")),  # Spacer
            ('pack', buttons)
        ])
        
        dialog = urwid.LineBox(content, title="⚠️ Confirm Uninstall")
        
        self.overlay = urwid.Overlay(
            dialog, self.main_frame,
            align='center', width=50,
            valign='middle', height=15
        )
        self.loop.widget = self.overlay

    def show_info_dialog(self, message):
        """Show an information dialog"""
        info_text = urwid.Text(message)
        ok_btn = urwid.Button("OK", on_press=lambda x: self.close_dialog())
        ok_btn = urwid.AttrMap(ok_btn, 'button', 'button_focus')
        
        content = urwid.Pile([
            ('pack', info_text),
            ('pack', urwid.Text("")),
            ('pack', ok_btn)
        ])
        
        info_dialog = urwid.LineBox(content, title="Information")
        
        self.overlay = urwid.Overlay(
            info_dialog, self.main_frame,
            align='center', width=50,
            valign='middle', height=10
        )
        self.loop.widget = self.overlay

    def close_dialog(self):
        """Close any open dialog and return to main interface"""
        if hasattr(self, 'overlay'):
            self.loop.widget = self.main_frame
            del self.overlay

    def show_error_dialog(self, error_message):
        """Show an error dialog"""
        error_text = urwid.Text(error_message)
        ok_btn = urwid.Button("OK", on_press=lambda x: self.close_dialog())
        ok_btn = urwid.AttrMap(ok_btn, 'button', 'button_focus')
        
        content = urwid.Pile([
            ('pack', error_text),
            ('pack', urwid.Text("")),  # Simple spacer instead of Divider
            ('pack', ok_btn)
        ])
        
        error_dialog = urwid.LineBox(content, title="Error")
        
        self.overlay = urwid.Overlay(
            error_dialog, self.main_frame,
            align='center', width=60,
            valign='middle', height=8
        )
        self.loop.widget = self.overlay
    
    def save_edited_manifest(self):
        """Save the edited manifest and trigger redeploy"""
        # Check if edit_widgets exists
        if not hasattr(self, 'edit_widgets'):
            self.show_error_dialog("No edit data available")
            return
            
        # Get the original spec and create a copy for editing
        if 0 <= self.current_selection < len(self.vm_entries):
            entry = self.vm_entries[self.current_selection]
            original_spec = entry.get('spec', {})
            edited_spec = original_spec.copy()
        else:
            self.show_error_dialog("No VM selected")
            return
        
        # Update spec with edited values
        for param, widget in self.edit_widgets.items():
            value = widget.edit_text.strip()
            if value:
                # Convert numeric values
                if param == 'vm_cpu_cores':
                    try:
                        value = int(value)
                    except ValueError:
                        pass
                edited_spec[param] = value
            elif param in edited_spec:
                # Remove empty values
                del edited_spec[param]
        
        vm_name = edited_spec.get('vmName', 'Unknown VM')
        
        # Close the edit dialog and show deployment window
        self.close_dialog()
        self.show_deployment_window(edited_spec, vm_name, is_edit=True, entry=entry)
    
    def show_redeploy_dialog(self, vm_name, spec, entry=None):
        message = f"Redeploy VM '{vm_name}'?"
        
        def yes_pressed(button):
            self.close_dialog()
            self.start_redeploy(spec, vm_name, entry)
            
        def no_pressed(button):
            self.close_dialog()
        
        yes_btn = urwid.Button("Yes", on_press=yes_pressed)
        no_btn = urwid.Button("No", on_press=no_pressed)
        yes_btn = urwid.AttrMap(yes_btn, 'button', 'button_focus')
        no_btn = urwid.AttrMap(no_btn, 'button', 'button_focus')
        
        buttons = urwid.Columns([yes_btn, no_btn], dividechars=1)
        
        content = urwid.Pile([
            ('pack', urwid.Text(message)),
            ('pack', urwid.Text("")),  # Simple spacer instead of Divider
            ('pack', buttons)
        ])
        
        dialog = urwid.LineBox(content, title="Confirm Redeploy")
        
        self.overlay = urwid.Overlay(
            dialog, self.main_frame,
            align='center', width=50,
            valign='middle', height=8
        )
        self.loop.widget = self.overlay
    
    def refresh_vm_list(self):
        """Refresh the VM list to show updated deployment status"""
        try:
            load_kube_config()
            custom_api = client.CustomObjectsApi()
            
            crs = custom_api.list_namespaced_custom_object(
                group=CRD_GROUP,
                version=CRD_VERSION,
                namespace=NAMESPACE,
                plural=CRD_PLURAL
            )['items']
            
            # Update vm_entries with fresh data
            self.vm_entries.clear()
            for cr in crs:
                spec = cr.get('spec', {})
                vm_name = spec.get('vmName')
                kubevirt_namespace = spec.get('kubevirt_namespace', 'kubevirt')
                if not vm_name:
                    self.vm_entries.append({
                        'label': "No vmName in CR, skipping.",
                        'status': 'error',
                        'spec': spec,
                        'kubevirt_namespace': kubevirt_namespace
                    })
                    continue
                deployed = vm_exists(vm_name, kubevirt_namespace)
                label = f"{'✓' if deployed else '✗'} {vm_name}: {'Deployed' if deployed else 'Not deployed'}"
                self.vm_entries.append({
                    'label': label,
                    'status': 'deployed' if deployed else 'not_deployed',
                    'spec': spec,
                    'vm_name': vm_name,
                    'kubevirt_namespace': kubevirt_namespace
                })
            
            # Rebuild the VM list widget
            if self.vm_entries:
                vm_list_items = []
                for i, entry in enumerate(self.vm_entries):
                    label = entry.get('label', f'VM {i+1}')
                    status = entry.get('status', 'unknown')
                    
                    if status == 'deployed':
                        attr = 'deployed'
                    elif status == 'not_deployed':
                        attr = 'not_deployed'
                    else:
                        attr = 'error'
                        
                    button = urwid.Button(label, on_press=self.vm_selected, user_data=i)
                    button = urwid.AttrMap(button, attr, 'selected')
                    vm_list_items.append(button)
            else:
                vm_list_items = [urwid.Text('No VMs found')]
            
            # Update the listbox contents
            self.vm_listbox.body = urwid.SimpleFocusListWalker(vm_list_items)
            
            # Reset selection to first item if available
            if self.vm_entries:
                self.current_selection = 0
                self.vm_listbox.focus_position = 0
                self.update_details(0)
            else:
                self.current_selection = -1
                self.details_text.set_text("No VMs available")
                
        except Exception as e:
            # If refresh fails, show error but don't crash
            self.details_text.set_text(f"Error refreshing VM list: {str(e)}")
    
    def show_deployment_window(self, spec, vm_name, is_edit=False, entry=None):
        """Show real-time deployment progress in a sub-window"""
        # Create a text widget for showing deployment output
        self.deployment_lines = []  # Store individual lines for better control
        self.deployment_output = urwid.Text("Starting deployment...\n")
        self.output_walker = urwid.SimpleFocusListWalker([self.deployment_output])
        output_listbox = urwid.ListBox(self.output_walker)
        
        # Create buttons
        def close_pressed(button):
            self.close_dialog()
            # Refresh the VM list to show updated status
            self.refresh_vm_list()
        
        close_btn = urwid.Button("Close", on_press=close_pressed)
        close_btn = urwid.AttrMap(close_btn, 'button', 'button_focus')
        
        # Add a refresh button as well
        def refresh_pressed(button):
            self.refresh_vm_list()
        
        refresh_btn = urwid.Button("Refresh VM List", on_press=refresh_pressed)
        refresh_btn = urwid.AttrMap(refresh_btn, 'button', 'button_focus')
        
        button_row = urwid.Columns([close_btn, refresh_btn], dividechars=2)
        
        # Create the deployment window content
        title = f"{'Editing &' if is_edit else ''} Deploying: {vm_name}"
        content = urwid.Pile([
            ('weight', 1, output_listbox),
            ('pack', urwid.Text("")),
            ('pack', button_row)
        ])
        
        dialog = urwid.LineBox(content, title=title)
        
        # Store references for auto-scrolling
        self.output_listbox = output_listbox
        self.button_row = button_row  # Store for focus management
        
        # Create deployment window handler
        class DeploymentWindowHandler(urwid.WidgetWrap):
            def __init__(self, widget, app):
                super().__init__(widget)
                self.app = app
                self.in_buttons = False  # Track if we're focused on buttons
            
            def keypress(self, size, key):
                # Handle escape to close (after deployment completes)
                if key == 'esc' and not hasattr(self.app, 'deployment_running'):
                    self.app.close_dialog()
                    self.app.refresh_vm_list()
                    return None
                # Handle tab to switch between output and buttons
                elif key == 'tab':
                    if hasattr(self.app, 'deployment_running'):
                        return None  # Don't allow button focus during deployment
                    self.in_buttons = not self.in_buttons
                    if self.in_buttons:
                        # Focus on buttons
                        if hasattr(self.app, 'button_row'):
                            self.app.button_row.focus_position = 0
                        return None
                    else:
                        # Focus back on output
                        return None
                # If we're in button area, handle button navigation
                elif self.in_buttons and key in ('left', 'right', 'enter', ' '):
                    if hasattr(self.app, 'button_row'):
                        return self.app.button_row.keypress(size, key)
                # Allow scrolling through output when not in buttons
                elif not self.in_buttons and key in ('up', 'down', 'page up', 'page down', 'home', 'end'):
                    return super().keypress(size, key)
                # Quick close with 'c' when deployment is done
                elif key in ('c', 'C') and not hasattr(self.app, 'deployment_running'):
                    self.app.close_dialog()
                    self.app.refresh_vm_list()
                    return None
                # Refresh VM list with 'r'
                elif key in ('r', 'R'):
                    self.app.refresh_vm_list()
                    return None
                # Enter key activates focused button
                elif key == 'enter' and self.in_buttons:
                    if hasattr(self.app, 'button_row'):
                        return self.app.button_row.keypress(size, key)
                # Ignore other keys during deployment
                else:
                    return None
        
        dialog_handler = DeploymentWindowHandler(dialog, self)
        
        self.overlay = urwid.Overlay(
            dialog_handler, self.main_frame,
            align='center', width=('relative', 90),
            valign='middle', height=('relative', 70)
        )
        self.loop.widget = self.overlay
        
        # Start the deployment in a separate thread to avoid blocking the UI
        import threading
        
        def run_deployment():
            self.deployment_running = True
            try:
                self.run_playbook_with_output(spec, vm_name, entry)
            except Exception as e:
                self.append_deployment_output(f"\nError during deployment: {str(e)}\n")
            finally:
                self.deployment_running = False
                self.append_deployment_output("\n" + "="*50 + "\n")
                self.append_deployment_output("--- Deployment finished ---\n")
                self.append_deployment_output("Press Tab to access buttons, Enter to activate\n")
                self.append_deployment_output("Or use: 'c'/Esc to close, 'r' to refresh\n")
        
        deployment_thread = threading.Thread(target=run_deployment, daemon=True)
        deployment_thread.start()
    
    def append_deployment_output(self, text):
        """Append text to the deployment output window with auto-scroll"""
        if hasattr(self, 'deployment_lines') and hasattr(self, 'output_walker'):
            # Split text into lines and add each as a separate widget for better control
            lines = text.split('\n')
            for line in lines:
                if line or text.endswith('\n'):  # Include empty lines if original text had them
                    line_widget = urwid.Text(line)
                    self.deployment_lines.append(line)
                    self.output_walker.append(line_widget)
            
            # Auto-scroll to bottom
            if hasattr(self, 'output_listbox') and self.output_walker:
                try:
                    # Set focus to the last item to auto-scroll
                    self.output_listbox.focus_position = len(self.output_walker) - 1
                except (IndexError, AttributeError):
                    pass
            
            # Trigger a redraw
            if hasattr(self, 'loop'):
                self.loop.draw_screen()
        elif hasattr(self, 'deployment_output'):
            # Fallback to old method if new method fails
            current_text = self.deployment_output.text
            self.deployment_output.set_text(current_text + text)
            if hasattr(self, 'loop'):
                self.loop.draw_screen()
    
    def run_playbook_with_output(self, spec, vm_name, entry=None):
        """Run playbook and capture output for display"""
        action = spec.get('action', 'install')
        action_word = "Uninstalling" if action == 'uninstall' else "Deploying"
        
        self.append_deployment_output(f"{action_word} VM: {vm_name}\n")
        self.append_deployment_output(f"Action: {action}\n")
        
        # If this is a file-based CR and we're installing, deploy CR to cluster first
        if entry and entry.get('source') == 'file' and action == 'install':
            self.append_deployment_output("Deploying Custom Resource to cluster first...\n")
            try:
                load_kube_config()
                custom_api = client.CustomObjectsApi()
                cr_data = entry.get('cr_data')
                if cr_data:
                    deployed_cr = deploy_cr_to_cluster(custom_api, cr_data)
                    if deployed_cr:
                        self.append_deployment_output("✓ Custom Resource deployed to cluster successfully.\n")
                    else:
                        self.append_deployment_output("✗ Failed to deploy Custom Resource to cluster.\n")
                        return
                else:
                    self.append_deployment_output("✗ No CR data found for file-based VM.\n")
                    return
            except Exception as e:
                self.append_deployment_output(f"✗ Error deploying CR to cluster: {e}\n")
                return
        
        self.append_deployment_output(f"Parameters:\n")
        for key, value in spec.items():
            self.append_deployment_output(f"  {key}: {value}\n")
        self.append_deployment_output("\n" + "="*50 + "\n")
        
        kubevirt_namespace = spec.get('kubevirt_namespace', 'kubevirt')
        if not vm_name:
            self.append_deployment_output("No vmName specified in spec, skipping playbook.\n")
            return
        
        # For uninstall, we don't need to check if VM exists - just run uninstall
        if action == 'uninstall':
            self.append_deployment_output(f"Uninstalling VirtualMachine {vm_name}...\n")
        else:
            # For install/deploy actions, check if VM already exists
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
                self.append_deployment_output(f"VirtualMachine {vm_name} already exists in namespace {kubevirt_namespace}. Skipping playbook.\n")
                return
            except ApiException as e:
                if e.status != 404:
                    self.append_deployment_output(f"Error checking for existing VM: {e}\n")
                    return
                # If 404, VM does not exist, proceed with install
        
        # Run playbook
        cmd = ["ansible-playbook", PLAYBOOK_PATH]
        for k, v in spec.items():
            # Convert booleans and numbers to strings for shell
            if isinstance(v, bool):
                v = str(v).lower()
            elif v is None:
                continue
            cmd.extend(["-e", f"{k}={v}"])
        
        self.append_deployment_output(f"Running: {' '.join(cmd)}\n\n")
        
        try:
            import subprocess
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                                     text=True, bufsize=1, universal_newlines=True)
            
            # Read output line by line and display in real-time
            for line in process.stdout:
                self.append_deployment_output(line)
            
            process.wait()
            
            if process.returncode == 0:
                self.append_deployment_output(f"\n✓ Deployment completed successfully!\n")
            else:
                self.append_deployment_output(f"\n✗ Deployment failed with exit code {process.returncode}\n")
                
        except Exception as e:
            self.append_deployment_output(f"\nPlaybook execution error: {e}\n")
    
    def close_dialog(self):
        """Close any open dialog and return to main interface"""
        self.loop.widget = self.main_frame
        if hasattr(self, 'overlay'):
            delattr(self, 'overlay')
        if hasattr(self, 'edit_widgets'):
            delattr(self, 'edit_widgets')
        if hasattr(self, 'original_spec'):
            delattr(self, 'original_spec')
        if hasattr(self, 'deployment_output'):
            delattr(self, 'deployment_output')
        if hasattr(self, 'deployment_lines'):
            delattr(self, 'deployment_lines')
        if hasattr(self, 'output_walker'):
            delattr(self, 'output_walker')
        if hasattr(self, 'output_listbox'):
            delattr(self, 'output_listbox')
        if hasattr(self, 'button_row'):
            delattr(self, 'button_row')
        if hasattr(self, 'deployment_running'):
            delattr(self, 'deployment_running')
    
    def start_redeploy(self, spec, vm_name, entry=None):
        # Close the confirmation dialog and show deployment window
        self.close_dialog()
        self.show_deployment_window(spec, vm_name, is_edit=False, entry=entry)
    
    def unhandled_input(self, key):
        # If we're in an overlay (dialog), don't handle any input here
        # Let the dialog handle everything
        if hasattr(self, 'overlay') and self.loop.widget == self.overlay:
            return None
            
        if key in ('q', 'Q', 'ctrl c'):
            raise urwid.ExitMainLoop()
        elif key in ('r', 'R'):
            # Sync selection before redeploy
            self.sync_selection_from_focus()
            self.redeploy_vm(None)
        elif key in ('e', 'E'):
            # Sync selection before edit
            self.sync_selection_from_focus()
            self.edit_manifest()
        elif key in ('u', 'U'):
            # Uninstall VM
            self.sync_selection_from_focus()
            self.uninstall_vm(None)
        elif key == 'enter' and self.vm_entries:
            # Enter key to show current selection info
            self.sync_selection_from_focus()
            if 0 <= self.current_selection < len(self.vm_entries):
                entry = self.vm_entries[self.current_selection]
                vm_name = entry.get('vm_name', f'VM {self.current_selection + 1}')
                self.show_info_dialog(f"Selected: {vm_name}\n\nPress R to redeploy, E to edit, U to uninstall")
        elif key == 'f5':
            # Manual refresh
            self.refresh_vm_list()
        # Note: up/down key handling is now done automatically by AutoSelectListBox
        # Return None to indicate we handled the input (or ignored it)
        return None
    
    def run(self):
        try:
            self.loop = urwid.MainLoop(
                self.main_frame, 
                self.palette, 
                unhandled_input=self.unhandled_input
            )
            self.loop.run()
        except Exception as e:
            # If we're in a dialog when an error occurs, try to close it
            if hasattr(self, 'overlay'):
                try:
                    self.close_dialog()
                except:
                    pass
            # Re-raise the exception so we can see what went wrong
            import traceback
            print(f"Application error: {e}")
            traceback.print_exc()
            raise

def run_tui(vm_entries):
    """Launch the urwid-based TUI"""
    # Set up signal handler for Ctrl+C
    def signal_handler(signum, frame):
        raise urwid.ExitMainLoop()
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        app = VMApp(vm_entries)
        app.run()
    except KeyboardInterrupt:
        pass

def main():
    load_kube_config()
    api_ext = client.ApiextensionsV1Api()
    custom_api = client.CustomObjectsApi()

    # Ensure CRD exists (from file or template)
    if not ensure_crd_exists(api_ext):
        print("\033[91mFailed to ensure CRD exists. Exiting.\033[0m")
        return

    # Load CRs from cluster
    print("\033[94m→ Loading CRs from Kubernetes cluster...\033[0m")
    try:
        cluster_crs = custom_api.list_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=NAMESPACE,
            plural=CRD_PLURAL
        )['items']
        print(f"\033[92m✓ Found {len(cluster_crs)} CRs in cluster\033[0m")
    except Exception as e:
        print(f"\033[91mError loading CRs from cluster: {e}\033[0m")
        cluster_crs = []

    # Load CRs from manifest-controller directory
    print(f"\033[94m→ Loading CRs from {MANIFEST_CONTROLLER_DIR}...\033[0m")
    file_crs = load_cr_files_from_directory(MANIFEST_CONTROLLER_DIR)
    print(f"\033[92m✓ Found {len(file_crs)} CR files\033[0m")

    # Create VM entries from both sources
    vm_entries = []
    cr_names_seen = set()

    # Process cluster CRs first
    for cr in cluster_crs:
        spec = cr.get('spec', {})
        vm_name = spec.get('vmName')
        cr_name = cr.get('metadata', {}).get('name', vm_name)
        kubevirt_namespace = spec.get('kubevirt_namespace', 'kubevirt')
        
        if not vm_name:
            vm_entries.append({
                'label': f"No vmName in CR {cr_name}, skipping.",
                'status': 'error',
                'spec': spec,
                'kubevirt_namespace': kubevirt_namespace,
                'source': 'cluster',
                'cr_name': cr_name
            })
            continue
            
        cr_names_seen.add(cr_name)
        deployed = vm_exists(vm_name, kubevirt_namespace)
        label = f"{'✓' if deployed else '✗'} {vm_name}: {'Deployed' if deployed else 'Not deployed'} [cluster]"
        vm_entries.append({
            'label': label,
            'status': 'deployed' if deployed else 'not_deployed',
            'spec': spec,
            'vm_name': vm_name,
            'kubevirt_namespace': kubevirt_namespace,
            'source': 'cluster',
            'cr_name': cr_name
        })

    # Process file CRs (only add if not already in cluster)
    for file_cr in file_crs:
        cr_data = file_cr['data']
        spec = cr_data.get('spec', {})
        vm_name = spec.get('vmName')
        cr_name = cr_data.get('metadata', {}).get('name', vm_name)
        kubevirt_namespace = spec.get('kubevirt_namespace', 'kubevirt')
        
        if not vm_name:
            vm_entries.append({
                'label': f"No vmName in file CR {cr_name}, skipping.",
                'status': 'error',
                'spec': spec,
                'kubevirt_namespace': kubevirt_namespace,
                'source': 'file',
                'cr_name': cr_name,
                'file_path': file_cr['file_path']
            })
            continue

        # Skip if already processed from cluster
        if cr_name in cr_names_seen:
            continue

        cr_names_seen.add(cr_name)
        deployed = vm_exists(vm_name, kubevirt_namespace)
        file_label = f"{'✓' if deployed else '✗'} {vm_name}: {'Deployed' if deployed else 'Available for deployment'} [file]"
        vm_entries.append({
            'label': file_label,
            'status': 'deployed' if deployed else 'available',
            'spec': spec,
            'vm_name': vm_name,
            'kubevirt_namespace': kubevirt_namespace,
            'source': 'file',
            'cr_name': cr_name,
            'file_path': file_cr['file_path'],
            'cr_data': cr_data
        })

    print(f"\033[92m✓ Total VM entries: {len(vm_entries)}\033[0m")
    run_tui(vm_entries)

# --- Entrypoint ---
if __name__ == "__main__":
    main()
