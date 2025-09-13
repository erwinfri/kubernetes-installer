"""
Enhanced TUI Interface for Windows Services Management
Full-featured interface matching original kopf-urwid-controller.py functionality
with consolidated VMs/Services view
"""

import urwid
import logging
import queue
import time
from datetime import datetime
import threading
import os

# Import from utils with fallback
try:
    from .utils.logging_config import log_queue
except ImportError:
    try:
        from utils.logging_config import log_queue
    except ImportError:
        import queue as queue_module
        log_queue = queue_module.Queue()

logger = logging.getLogger(__name__)

class WindowsServicesTUI:
    """Enhanced TUI interface with full functionality"""
    
    def __init__(self, service_manager):
        self.service_manager = service_manager
        self.log_lines = []
        self.max_log_lines = 500
        self.status_data = {}
        self.last_status_update = 0
        self.update_interval = 5
        self.auto_scroll = True
        self.active_service_tab = 'vms'  # vms, mssql, otel
        
        # Menu state management for hierarchical navigation
        self.menu_state = None  # None, 'install_method_selection', 'uninstall_method_selection', 'service_selection'
        self.pending_action = None  # 'install', 'uninstall', 'apply', 'delete'
        self.selected_method = None  # 'kubectl', 'ansible', 'manual'
        self.popup_listbox = None  # For popup navigation
        self.popup = None  # For popup management
        self.popup_callback = None  # For popup callbacks
        
        # Enhanced color palette matching original
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
            ('status_running', 'light green', 'black'),
            ('status_stopped', 'light red', 'black'),
            ('status_unknown', 'yellow', 'black'),
            ('cr_deployed', 'light cyan', 'black'),
            ('cr_local', 'light magenta', 'black'),
            ('cr_missing', 'dark gray', 'black'),
            ('service_vm', 'light cyan', 'black'),
            ('service_mssql', 'light blue', 'black'),
            ('service_otel', 'light green', 'black'),
        ]
        
        self.setup_ui()
    

    def apply_crds_menu(self, button):
        """Show a menu to apply CRD YAMLs from manifest-controller, with ESC support."""
        import os
        folder = '/root/kubernetes-installer/manifest-controller'
        files = os.listdir(folder)
        crd_files = [f for f in files if f.endswith('.yaml') and ('crd' in f.lower())]
        if not crd_files:
            return

        def handle_crd_apply_selection(crd_name, crd_path):
            import subprocess
            result = subprocess.run(['kubectl', 'apply', '-f', crd_path], capture_output=True, text=True)
            if result.returncode == 0:
                self.add_log_line(f"✅ CRD applied: {crd_name}")
            else:
                self.add_log_line(f"❌ Failed to apply CRD {crd_name}: {result.stderr}")
                if 'no objects passed to apply' in result.stderr:
                    self.add_log_line(f"⚠️ The file {crd_name} does not contain a valid Kubernetes object. Please check the YAML content.")

        # Custom button class for ESC handling
        class CRDButton(urwid.Button):
            def __init__(self, label, crd_name, crd_path, callback, tui_instance):
                super().__init__(label)
                self.crd_name = crd_name
                self.crd_path = crd_path
                self.callback = callback
                self.tui = tui_instance
            def keypress(self, size, key):
                if key in ('enter', ' '):
                    self.tui.close_popup()
                    self.callback(self.crd_name, self.crd_path)
                    return
                if key == 'esc' or key == 'escape':
                    self.tui.close_popup()
                    self.tui.menu_state = None
                    self.tui.popup_listbox = None
                    self.tui.reset_menu_state()
                    return
                return super().keypress(size, key)

        menu_items = []
        for crd_file in crd_files:
            crd_path = os.path.join(folder, crd_file)
            btn = CRDButton(f"CRD: {crd_file}", crd_file, crd_path, handle_crd_apply_selection, self)
            menu_items.append(urwid.AttrMap(btn, 'button', 'button_focus'))

        walker = urwid.SimpleFocusListWalker(menu_items)
        listbox = urwid.ListBox(walker)
        popup_content = urwid.Pile([
            urwid.Text(('header', 'Apply CRDs - Select to Apply'), align='center'),
            urwid.Divider(),
            urwid.BoxAdapter(listbox, height=len(menu_items) + 2),
            urwid.Divider(),
            urwid.Text("Use ↑↓ arrows and Enter to apply, ESC to cancel", align='center')
        ])
        dialog = urwid.LineBox(popup_content, title="Apply CRDs")
        overlay = urwid.Overlay(
            dialog,
            self.main_frame,
            align='center',
            width=60,
            valign='middle',
            height=len(menu_items) + 8
        )
        self.original_widget = self.loop.widget
        self.loop.widget = overlay
        self.menu_state = 'crd_popup'
        self.popup_listbox = listbox

    def apply_cr_menu(self, button):
        """Show a menu to apply CR YAMLs from manifest-controller."""
        import os
        folder = '/root/kubernetes-installer/manifest-controller'
        files = os.listdir(folder)
        cr_files = [f for f in files if f.endswith('.yaml') and ('crd' not in f.lower())]
        if not cr_files:
            return

        def handle_cr_apply_selection(cr_name, cr_path):
            import subprocess
            result = subprocess.run(['kubectl', 'apply', '-f', cr_path], capture_output=True, text=True)
            if result.returncode == 0:
                self.add_log_line(f"✅ CR applied: {cr_name}")
            else:
                self.add_log_line(f"❌ Failed to apply CR {cr_name}: {result.stderr}")

        menu_items = []
        for cr_file in cr_files:
            cr_path = os.path.join(folder, cr_file)
            btn = urwid.Button(f"CR: {cr_file}", on_press=lambda button, f=cr_file, p=cr_path: (self.close_popup(), handle_cr_apply_selection(f, p)))
            menu_items.append(urwid.AttrMap(btn, 'button', 'button_focus'))

        walker = urwid.SimpleFocusListWalker(menu_items)
        listbox = urwid.ListBox(walker)
        popup_content = urwid.Pile([
            urwid.Text(('header', 'Apply CRs - Select to Apply'), align='center'),
            urwid.Divider(),
            urwid.BoxAdapter(listbox, height=len(menu_items) + 2),
            urwid.Divider(),
            urwid.Text("Use ↑↓ arrows and Enter to apply, ESC to cancel", align='center')
        ])
        dialog = urwid.LineBox(popup_content, title="Apply CRs")
        overlay = urwid.Overlay(
            dialog,
            self.main_frame,
            align='center',
            width=60,
            valign='middle',
            height=len(menu_items) + 8
        )
        self.original_widget = self.loop.widget
        self.loop.widget = overlay
        self.menu_state = 'cr_popup'
        self.popup_listbox = listbox

    def setup_ui(self):
        """Set up the enhanced user interface"""
        # Header
        header = urwid.Text(('header', 'Windows Services Management Console - Enhanced Modular System'), align='center')
        header = urwid.AttrMap(header, 'header')
        
        # Menu bar with action-focused functions
        menu_items = [
            ('Install', self.install_menu),
            ('Uninstall', self.uninstall_menu),
            ('Apply CRs', self.apply_cr_menu),
            ('Apply CRDs', self.apply_crds_menu),
            ('Delete CR', self.delete_cr_menu),
            ('Fix Issues', self.fix_issues),
            ('Clear Logs', self.clear_logs),
            ('Quit', self.quit_app)
        ]
    def setup_ui(self):
        """Set up the enhanced user interface"""
        # Header
        header = urwid.Text(('header', 'Windows Services Management Console - Enhanced Modular System'), align='center')
        header = urwid.AttrMap(header, 'header')

        # Status display panel - consolidated VMs/Services view
        self.status_walker = urwid.SimpleFocusListWalker([])
        self.status_listbox = urwid.ListBox(self.status_walker)
        self.status_frame = urwid.LineBox(self.status_listbox, title="VMs & Services Status Overview")

        # Log display
        self.log_walker = urwid.SimpleFocusListWalker([])
        self.log_listbox = urwid.ListBox(self.log_walker)
        self.log_frame = urwid.LineBox(self.log_listbox, title="System Logs [FOCUSED]")

        # Create horizontal split - VMs/Services split as requested
        self.content_columns = urwid.Columns([
            ('weight', 1, self.status_frame),
            ('weight', 2, self.log_frame)
        ], dividechars=1, focus_column=1)

        # Footer with streamlined navigation instructions
        footer_text = "F2:Status F6:Install F7:Apply F8:AutoScroll F9:Reset Tab:Switch ←→:Navigate Q:Quit ESC:Cancel"
        footer = urwid.Text(('footer', footer_text), align='center')
        footer = urwid.AttrMap(footer, 'footer')

        # Menu bar with action-focused functions
        menu_items = [
            ('Install', self.install_menu),
            ('Uninstall', self.uninstall_menu),
            ('Apply CRs', self.apply_cr_menu),
            ('Apply CRDs', self.apply_crds_menu),
            ('Delete CR', self.delete_cr_menu),
            ('Fix Issues', self.fix_issues),
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

        # Main layout
        top_section = urwid.Pile([
            ('pack', header),
            ('pack', urwid.Text("")),
            ('pack', menu_frame),
            ('pack', urwid.Divider('─')),
            ('pack', urwid.Text("")),
        ])

        main_pile = urwid.Pile([
            ('pack', top_section),
            ('weight', 1, self.content_columns),
            ('pack', footer)
        ])

        self.main_frame = main_pile
    
    def show_vms_tab(self, button=None):
        """Switch to VMs view in status display"""
        self.active_service_tab = 'vms'
        self.status_frame.set_title("Windows VMs Status")
        self.update_status_display()
        if button:  # Only log if called via button/menu
            self.add_log_line("📊 Switched to Windows VMs view")
    
    def show_mssql_tab(self, button=None):
        """Switch to MSSQL services view in status display"""
        self.active_service_tab = 'mssql'
        self.status_frame.set_title("MSSQL Services Status")
        self.update_status_display()
        if button:  # Only log if called via button/menu
            self.add_log_line("🗄️ Switched to MSSQL Services view")
    
    def show_otel_tab(self, button=None):
        """Switch to OpenTelemetry view in status display"""
        self.active_service_tab = 'otel'
        self.status_frame.set_title("OpenTelemetry Collectors Status")
        self.update_status_display()
        if button:  # Only log if called via button/menu
            self.add_log_line("📊 Switched to OpenTelemetry view")
    
    def update_status_display(self):
        """Update the consolidated status display"""
        try:
            current_time = time.time()
            
            # Throttle updates
            if current_time - self.last_status_update < self.update_interval:
                return
            
            self.last_status_update = current_time
            
            # Get comprehensive status from service manager
            status_report = self.service_manager.get_comprehensive_status()
            
            # Clear existing status display
            self.status_walker.clear()
            
            # Add header with timestamp
            timestamp = datetime.now().strftime("%H:%M:%S")
            if self.active_service_tab == 'vms':
                header_text = f'🖥️ WINDOWS VMs STATUS ({timestamp})'
                self.update_vm_status_display(status_report)
            elif self.active_service_tab == 'mssql':
                header_text = f'🗄️ MSSQL SERVICES STATUS ({timestamp})'
                self.update_mssql_status_display(status_report)
            elif self.active_service_tab == 'otel':
                header_text = f'📊 OPENTELEMETRY STATUS ({timestamp})'
                self.update_otel_status_display(status_report)
            
            # Insert header at top
            self.status_walker.insert(0, urwid.Text(('header', f'=== {header_text} ===')))
            self.status_walker.insert(1, urwid.Text(""))
            
            # Store status data for other methods
            self.status_data = status_report;
            
        except Exception as e:
            self.status_walker.clear()
            self.status_walker.append(urwid.Text(('log_error', f'Error updating status: {e}')))
    
    def update_vm_status_display(self, status_report):
        """Update VM status display with scenarios"""
        # VM scenarios
        scenarios = status_report.get('scenarios', {})
        if scenarios:
            self.status_walker.append(urwid.Text(('service_vm', '📊 VM SCENARIO ANALYSIS:')))
            for vm_name, scenario_data in scenarios.items():
                scenario = scenario_data['scenario']
                
                # Color coding based on scenario
                if 'Running' in scenario and 'Managed' in scenario:
                    color = 'status_running'
                    icon = '✅'
                elif 'Running' in scenario and 'Orphaned' in scenario:
                    color = 'status_unknown'
                    icon = '⚠️'
                elif 'No Instance' in scenario:
                    color = 'status_stopped'
                    icon = '❌'
                else:
                    color = 'status_unknown'
                    icon = '❓'
                
                status_line = f"{icon} {vm_name}: {scenario}"
                self.status_walker.append(urwid.Text((color, status_line)))
                
                # Add details
                if scenario_data.get('local_cr'):
                    self.status_walker.append(urwid.Text(('cr_local', f"   📁 Local CR: {scenario_data['local_cr']} (action: {scenario_data.get('local_cr_action', 'unknown')})")))
                if scenario_data.get('deployed_cr'):
                    self.status_walker.append(urwid.Text(('cr_deployed', f"   ☸️ Deployed CR: {scenario_data['deployed_cr']} (action: {scenario_data.get('deployed_cr_action', 'unknown')})")))
                if scenario_data.get('vm_running'):
                    self.status_walker.append(urwid.Text(('status_running', f"   🖥️ VM Status: {scenario_data.get('vm_status', 'unknown')}")))
                
                self.status_walker.append(urwid.Text(""))
        else:
            self.status_walker.append(urwid.Text(('status_unknown', '❓ No VMs found')))
        
        # Summary statistics
        windowsvm_summary = status_report.get('summary', {}).get('windowsvm', {})
        import os
        crd_files = []
        crd_names_in_folder = set()
        crd_count = 0
        try:
            folder = '/root/kubernetes-installer/manifest-controller'
            files = os.listdir(folder)
            crd_files = [f for f in files if f.endswith('.yaml') and 'crd' in f.lower()]
            crd_count = len(crd_files)
            # Extract CRD names from YAMLs
            import yaml
            for fname in crd_files:
                try:
                    with open(os.path.join(folder, fname), 'r') as f:
                        y = yaml.safe_load(f)
                        if y and y.get('kind', '').lower() == 'customresourcedefinition':
                            meta = y.get('metadata', {})
                            name = meta.get('name')
                            if name:
                                crd_names_in_folder.add(name)
                except Exception:
                    pass
        except Exception:
            crd_count = 0
        deployed_crd_names = set()
        deployed_crd_count = 0
        try:
            import subprocess
            result = subprocess.run(['kubectl', 'get', 'crd', '-o', 'name'], capture_output=True, text=True)
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    line = line.strip()
                    if line:
                        # Format: customresourcedefinition.apiextensions.k8s.io/<crd_name>
                        parts = line.split('/')
                        if len(parts) == 2:
                            deployed_crd_names.add(parts[1])
                deployed_crd_count = len(deployed_crd_names)
        except Exception:
            deployed_crd_count = 0
        # Count matches
        matching_crds = crd_names_in_folder & deployed_crd_names
        match_count = len(matching_crds)
        self.status_walker.append(urwid.Text(('header', '📈 VM SUMMARY:')))
        self.status_walker.append(urwid.Text(f"CRDs in folder: {crd_count} | Deployed CRDs: {deployed_crd_count} | Matching: {match_count}"))
        self.status_walker.append(urwid.Text(f"Local CRs: {windowsvm_summary.get('local_count', 0)}"))
        self.status_walker.append(urwid.Text(f"Deployed CRs: {windowsvm_summary.get('deployed_count', 0)}"))
        self.status_walker.append(urwid.Text(f"Running VMs: {windowsvm_summary.get('running_count', 0)}"))
    
    def update_mssql_status_display(self, status_report):
        """Update MSSQL services status display"""
        mssql_data = status_report.get('mssqlservers', {})
        
        # Local CRs
        if mssql_data.get('local_crs'):
            self.status_walker.append(urwid.Text(('service_mssql', '📁 LOCAL MSSQL CRs:')))
            for name, cr_data in mssql_data['local_crs'].items():
                target_vm = cr_data.get('target_vm', 'unknown')
                version = cr_data.get('version', 'unknown')
                enabled = cr_data.get('enabled', True)
                status_icon = '✅' if enabled else '⏸️'
                self.status_walker.append(urwid.Text(('cr_local', f"  {status_icon} {name}: target={target_vm}, version={version}")))
            self.status_walker.append(urwid.Text(""))
        
        # Deployed CRs
            for name, cr_data in mssql_data['deployed_crs'].items():
                target_vm = cr_data.get('target_vm', 'unknown')
                version = cr_data.get('version', 'unknown')
                status = cr_data.get('status', {}).get('phase', 'Unknown')
                
                if status == 'Ready':
                    color = 'status_running'
                    icon = '🟢'
                elif status == 'Failed':
                    color = 'status_stopped'
                    icon = '🔴'
                else:
                    color = 'status_unknown'
                    icon = '🟡'
                
                self.status_walker.append(urwid.Text((color, f"  {icon} {name}: target={target_vm}, version={version}, status={status}")))
            self.status_walker.append(urwid.Text(""))
        
        if not mssql_data.get('local_crs') and not mssql_data.get('deployed_crs'):
            self.status_walker.append(urwid.Text(('status_unknown', '❓ No MSSQL services found')))
        
        # Summary
        mssql_summary = status_report.get('summary', {}).get('mssqlserver', {})
        self.status_walker.append(urwid.Text(('header', '📈 MSSQL SUMMARY:')))
        self.status_walker.append(urwid.Text(f"Local CRs: {mssql_summary.get('local_count', 0)}"))
        self.status_walker.append(urwid.Text(f"Deployed CRs: {mssql_summary.get('deployed_count', 0)}"))
    
    def update_otel_status_display(self, status_report):
        """Update OpenTelemetry status display"""
        otel_data = status_report.get('otelcollectors', {})
        
        # Local CRs
        if otel_data.get('local_crs'):
            self.status_walker.append(urwid.Text(('service_otel', '📁 LOCAL OTEL CRs:')))
            for name, cr_data in otel_data['local_crs'].items():
                target_vm = cr_data.get('target_vm', 'unknown')
                metrics_type = cr_data.get('metrics_type', 'unknown')
                enabled = cr_data.get('enabled', True)
                status_icon = '✅' if enabled else '⏸️'
                self.status_walker.append(urwid.Text(('cr_local', f"  {status_icon} {name}: target={target_vm}, metrics={metrics_type}")))
            self.status_walker.append(urwid.Text(""))
        
        # Deployed CRs
        if otel_data.get('deployed_crs'):
            self.status_walker.append(urwid.Text(('service_otel', '☸️ DEPLOYED OTEL CRs:')))
            for name, cr_data in otel_data['deployed_crs'].items():
                target_vm = cr_data.get('target_vm', 'unknown')
                metrics_type = cr_data.get('metrics_type', 'unknown')
                status = cr_data.get('status', {}).get('phase', 'Unknown')
                
                if status == 'Ready':
                    color = 'status_running'
                    icon = '🟢'
                elif status == 'Failed':
                    color = 'status_stopped'
                    icon = '🔴'
                else:
                    color = 'status_unknown'
                    icon = '🟡'
                
                self.status_walker.append(urwid.Text((color, f"  {icon} {name}: target={target_vm}, metrics={metrics_type}, status={status}")))
            self.status_walker.append(urwid.Text(""))
        
        if not otel_data.get('local_crs') and not otel_data.get('deployed_crs'):
            self.status_walker.append(urwid.Text(('status_unknown', '❓ No OpenTelemetry services found')))
        
        # Summary
        otel_summary = status_report.get('summary', {}).get('otelcollector', {})
        self.status_walker.append(urwid.Text(('header', '📈 OTEL SUMMARY:')))
        self.status_walker.append(urwid.Text(f"Local CRs: {otel_summary.get('local_count', 0)}"))
        self.status_walker.append(urwid.Text(f"Deployed CRs: {otel_summary.get('deployed_count', 0)}"))
    
    def add_log_line(self, text):
        """Add a log line to the System Logs window, splitting multi-line entries for smooth display."""
        if isinstance(text, str):
            for line in text.splitlines():
                if line.strip() == '':
                    continue
                self._add_log_line_single(line)
        else:
            self._add_log_line_single(str(text))
        # Always scroll to the latest log line
        if hasattr(self, 'log_listbox') and self.log_walker:
            try:
                self.log_listbox.focus_position = len(self.log_walker) - 1
            except Exception:
                pass

    def _add_log_line_single(self, line):
        # Determine log level color
        if 'ERROR' in line.upper() or '❌' in line:
            attr = 'log_error'
        elif 'WARNING' in line.upper() or 'WARN' in line.upper() or '⚠️' in line:
            attr = 'log_warning'
        else:
            attr = 'log_info'
        log_widget = urwid.Text((attr, line))
        self.log_walker.append(log_widget)
        # Keep only recent logs
        if len(self.log_walker) > self.max_log_lines:
            self.log_walker.pop(0)
        # Auto-scroll if enabled
        if self.auto_scroll and self.log_walker:
            try:
                self.log_listbox.focus_position = len(self.log_walker) - 1
            except:
                pass
    
    # Menu action methods with central popup windows
    def install_menu(self, button):
        """Show install service selection in central popup"""
        self.show_service_selection_popup("INSTALL", [
            ("1", "Windows VM Deploy", "🖥️", "Deploy Windows Virtual Machines"),
            ("2", "MSSQL Windows Service Deploy", "🗄️", "Deploy SQL Server instances"),  
            ("3", "Windows OTel Service Deploy", "📊", "Deploy OpenTelemetry collectors")
        ], self.handle_install_selection)
    
    def uninstall_menu(self, button):
        """Show uninstall service selection in central popup"""
        self.show_service_selection_popup("UNINSTALL", [
            ("1", "Windows VM Uninstall", "🖥️", "Remove Windows Virtual Machines"),
            ("2", "MSSQL Service Uninstall", "🗄️", "Remove SQL Server instances"),
            ("3", "OTel Service Uninstall", "📊", "Remove OpenTelemetry collectors")
        ], self.handle_uninstall_selection)
    
    def apply_cr_menu(self, button):
        """Show apply CR service selection in central popup"""
        self.show_service_selection_popup("APPLY CRs", [
            ("1", "Windows VM CRs", "🖥️", "Apply Windows VM Custom Resources"),
            ("2", "MSSQL Service CRs", "🗄️", "Apply MSSQL Custom Resources"),
            ("3", "OTel Service CRs", "📊", "Apply OpenTelemetry Custom Resources")
        ], self.handle_apply_selection)
    
    def delete_cr_menu(self, button):
        """Show delete CR service selection in central popup"""
        self.show_service_selection_popup("DELETE CRs", [
            ("1", "Windows VM CRs", "🖥️", "Delete Windows VM Custom Resources"),
            ("2", "MSSQL Service CRs", "🗄️", "Delete MSSQL Custom Resources"),
            ("3", "OTel Service CRs", "📊", "Delete OpenTelemetry Custom Resources")
        ], self.handle_delete_selection)

    def show_cr_selection_popup(self, title, cr_options, callback):
        """Show CR selection in a dropdown popup with arrow navigation"""
        if not cr_options:
            return
            
        # Create menu items for CRs
        menu_items = []
        for name, cr_data, status in cr_options:
            # Determine icon based on status
            if "Ready" in status:
                icon = "✅"
            elif "Already" in status or "Deployed" in status:
                icon = "🔄"
            elif "Unknown" in status:
                icon = "🔴"
            elif "Disabled" in status:
                icon = "⏸️"
            else:
                icon = "📝"
                
            # Create button text
            button_text = f"{icon} {name}\n   {status}"
            
            
            # Create custom button that DEFINITELY handles Enter key
            class CRButton(urwid.Button):
                def __init__(self, label, cr_name, cr_data, callback, tui_instance):
                    super().__init__(label)
                    self.cr_name = cr_name
                    self.cr_data = cr_data
                    self.callback = callback
                    self.tui = tui_instance
                def keypress(self, size, key):
                    if key in ('enter', ' '):
                        self.tui.close_popup()
                        self.callback(self.cr_name, self.cr_data)
                        return
                    if key == 'esc' or key == 'escape':
                        self.tui.close_popup()
                        # Reset menu state and popup_listbox as in global handler
                        self.tui.menu_state = None
                        self.tui.popup_listbox = None
                        self.tui.reset_menu_state()
                        return
                    return super().keypress(size, key)
            
            button = CRButton(button_text, name, cr_data, callback, self)
            button.cr_name = name
            button.cr_data = cr_data
            menu_items.append(urwid.AttrMap(button, 'button', 'button_focus'))
        
        # Use standard ListBox - urwid will handle Enter key properly
        menu_walker = urwid.SimpleListWalker(menu_items)
        menu_listbox = urwid.ListBox(menu_walker)
        # Ensure focus is set to the first item so Enter works
        if len(menu_walker) > 0:
            menu_listbox.focus_position = 0
        
        # Create simple popup content
        popup_content = urwid.Pile([
            urwid.Text(('popup_title', f"🔽 {title}"), align='center'),
            urwid.Divider('─'),
            urwid.BoxAdapter(menu_listbox, height=min(len(menu_items), 6)),
            urwid.Divider('─'),
            urwid.Text("↑↓: Navigate, Enter: Select, 1: Quick Select First, ESC: Cancel", align='center')
        ])
        
        popup_box = urwid.AttrMap(urwid.LineBox(popup_content, title=title), 'popup')
        
        # Center the popup
        overlay = urwid.Overlay(
            popup_box,
            self.main_frame,
            align='center', width=60,
            valign='middle', height=min(len(menu_items) + 8, 16)  # Add more space for proper rendering
        )
        
        self.popup = overlay
        self.popup_callback = callback  # Store callback for keyboard handling
        self.popup_listbox = menu_listbox  # Store listbox reference for Enter handling
        
        # Store original widget before showing popup
        if not hasattr(self, 'original_widget') or not self.original_widget:
            self.original_widget = self.loop.widget
        
        # Set menu state for CR popup
        self.menu_state = 'cr_popup'
        
        # Show popup
        self.loop.widget = overlay
    
    def show_service_selection_popup(self, action_title, service_options, callback):
        """Show a central popup window for service selection with arrow key navigation"""
        try:
            # Custom button class for ESC handling
            class ServiceButton(urwid.Button):
                def __init__(self, label, option_key, callback, tui_instance):
                    super().__init__(label)
                    self.option_key = option_key
                    self.callback = callback
                    self.tui = tui_instance
                def keypress(self, size, key):
                    if key in ('enter', ' '):
                        self.tui.close_popup()
                        self.callback(self.option_key)
                        return
                    if key == 'esc' or key == 'escape':
                        self.tui.close_popup()
                        self.tui.menu_state = None
                        self.tui.popup_listbox = None
                        self.tui.reset_menu_state()
                        return
                    return super().keypress(size, key)

            # Create selectable menu items using ServiceButton
            menu_items = []
            for key, title, icon, description in service_options:
                button_text = f"{icon} {title}\n   {description}"
                button = ServiceButton(button_text, key, callback, self)
                button.key = key
                styled_button = urwid.AttrMap(button, 'menu', 'menu_focus')
                menu_items.append(styled_button)
            walker = urwid.SimpleFocusListWalker(menu_items)
            listbox = urwid.ListBox(walker)
            
            # Create popup content with title and instructions
            popup_content = urwid.Pile([
                urwid.Text(('header', f'🎯 {action_title} - SELECT SERVICE'), align='center'),
                urwid.Divider(),
                urwid.Text("Available Services:", align='center'),
                urwid.Divider(),
                urwid.BoxAdapter(listbox, height=len(service_options) * 3 + 2),
                urwid.Divider(),
                urwid.Text("Use ↑↓ arrows and Enter to select, ESC to cancel", align='center')
            ])
            
            # Create dialog box
            dialog = urwid.LineBox(popup_content, title=f"📋 {action_title}")
            
            # Create overlay - centered popup
            self.selection_overlay = urwid.Overlay(
                dialog,
                self.main_frame,
                align='center',
                width=60,
                valign='middle', 
                height=len(service_options) * 3 + 10
            )
            
            # Store original widget and callback
            self.original_widget = self.loop.widget
            self.popup_callback = callback
            self.popup_action = action_title
            self.service_options = service_options
            self.popup_listbox = listbox
            
            # Switch to overlay
            self.loop.widget = self.selection_overlay
            self.menu_state = 'service_popup'
            
            self.add_log_line(f"📋 {action_title} service selection opened - use ↑↓ and Enter")
            
        except Exception as e:
            self.add_log_line(f"❌ Error showing service selection popup: {e}")
    
    def close_popup(self):
        """Close the current popup and return to main interface"""
        if hasattr(self, 'original_widget') and self.original_widget:
            self.loop.widget = self.original_widget
            self.original_widget = None
            self.popup_callback = None
            self.popup_action = None
            self.service_options = None
            self.popup_listbox = None
            self.popup = None  # Clear popup reference
            self.menu_state = None
            self.add_log_line("📋 Popup closed")
    
    def handle_install_selection(self, service_key):
        """Handle install service selection"""
        service_map = {
            '1': ('vms', 'Windows VMs'),
            '2': ('mssql', 'MSSQL Services'),
            '3': ('otel', 'OpenTelemetry Services')
        }
        
        if service_key in service_map:
            service_type, service_name = service_map[service_key]
            self.add_log_line(f"🚀 Selected: Install {service_name}")
            # Go directly to CR selection for install
            self.show_cr_selection_for_install(service_type, service_name)
    
    def handle_uninstall_selection(self, service_key):
        """Handle uninstall service selection"""
        service_map = {
            '1': ('vms', 'Windows VMs'),
            '2': ('mssql', 'MSSQL Services'), 
            '3': ('otel', 'OpenTelemetry Services')
        }
        
        if service_key in service_map:
            service_type, service_name = service_map[service_key]
            self.add_log_line(f"🗑️ Selected: Uninstall {service_name}")
            # Go directly to CR selection for uninstall
            self.show_cr_selection_for_uninstall(service_type, service_name)
    
    def show_cr_selection_for_install(self, service_type, service_name):
        """Show available CRs for installation using popup menu"""
        self.add_log_line(f"📋 Loading {service_name} CRs for installation...")
        
        try:
            status_report = self.service_manager.get_comprehensive_status()
            service_map = {
                'vms': 'windowsvms',
                'mssql': 'mssqlservers',
                'otel': 'otelcollectors'
            }
            
            service_key = service_map.get(service_type)
            if service_key and service_key in status_report:
                service_data = status_report[service_key]
                local_crs = service_data.get('local_crs', {})
                deployed_crs = service_data.get('deployed_crs', {})
                
                if local_crs:
                    crs_to_show = []
                    for name, cr_data in local_crs.items():
                        status_text = "Ready to Install"
                        crs_to_show.append((name, cr_data, status_text))
                    def handle_cr_install_selection(cr_name, cr_data):
                        self.add_log_line(f"🎯 handle_cr_install_selection called with CR: {cr_name}")
                        self.execute_cr_install(service_type, service_name, cr_name, cr_data)
                    self.show_cr_selection_popup(
                        f"Install {service_name} CR",
                        crs_to_show,
                        handle_cr_install_selection
                    )
                else:
                    self.add_log_line(f"❌ No local {service_name} CRs found")
                    self.add_log_line("💡 Create CR files first or check manifest-controller directory")
            else:
                self.add_log_line(f"❌ Service type {service_type} not supported or no data available")
                
        except Exception as e:
            self.add_log_line(f"❌ Error loading CRs: {e}")
    
    def show_cr_selection_for_uninstall(self, service_type, service_name):
        """Show deployed CRs for uninstallation using popup menu"""
        self.add_log_line(f"📋 Loading deployed {service_name} CRs for uninstallation...")
        
        try:
            status_report = self.service_manager.get_comprehensive_status()
            service_map = {
                'vms': 'windowsvms',
                'mssql': 'mssqlservers',
                'otel': 'otelcollectors'
            }
            
            service_key = service_map.get(service_type)
            if service_key and service_key in status_report:
                service_data = status_report[service_key]
                deployed_crs = service_data.get('deployed_crs', {})
                
                if deployed_crs:
                    uninstallable_crs = []
                    for name, cr_data in deployed_crs.items():
                        status = cr_data.get('status', {}).get('phase', 'Unknown')
                        if status == 'Ready' or status == 'Failed' or status == 'Unknown':
                            status_text = f"Status: {status}"
                            uninstallable_crs.append((name, cr_data, status_text))
                    if uninstallable_crs:
                        def handle_cr_uninstall_selection(cr_name, cr_data):
                            self.execute_cr_uninstall(service_type, service_name, cr_name, cr_data)
                        self.show_cr_selection_popup(
                            f"Uninstall {service_name} CR", 
                            uninstallable_crs, 
                            handle_cr_uninstall_selection
                        )
                    else:
                        self.add_log_line(f"❌ No deployed {service_name} CRs found")
                else:
                    self.add_log_line(f"❌ No deployed {service_name} CRs found")
            else:
                self.add_log_line(f"❌ Service type {service_type} not supported or no data available")
                
        except Exception as e:
            self.add_log_line(f"❌ Error loading deployed CRs: {e}")
    
    def show_cr_selection_for_apply(self, service_type, service_name):
        """Show local CRs for application using popup menu"""
        self.add_log_line(f"📋 Loading {service_name} CRs for application...")
        
        try:
            status_report = self.service_manager.get_comprehensive_status()
            service_map = {
                'vms': 'windowsvms',
                'mssql': 'mssqlservers',
                'otel': 'otelcollectors'
            }
            
            service_key = service_map.get(service_type)
            if service_key and service_key in status_report:
                service_data = status_report[service_key]
                local_crs = service_data.get('local_crs', {})
                
                if local_crs:
                    applicable_crs = []
                    for name, cr_data in local_crs.items():
                        # For apply, show all local CRs
                        action = cr_data.get('action', 'unknown')
                        status_text = f"Action: {action}"
                        applicable_crs.append((name, cr_data, status_text))
                    
                    if applicable_crs:
                        # Use popup for CR selection
                        def handle_cr_apply_selection(cr_name, cr_data):
                            """Handle CR selection for apply"""
                            self.execute_cr_apply(service_type, service_name, cr_name, cr_data)
                        
                        self.show_cr_selection_popup(
                            f"Apply {service_name} CR", 
                            applicable_crs, 
                            handle_cr_apply_selection
                        )
                    else:
                        self.add_log_line(f"❌ No local {service_name} CRs found")
                        self.add_log_line("💡 Create CR files first in manifest-controller directory")
                else:
                    self.add_log_line(f"❌ No local {service_name} CRs found")
                    self.add_log_line("💡 Create CR files first in manifest-controller directory")
            else:
                self.add_log_line(f"❌ Service type {service_type} not supported or no data available")
                
        except Exception as e:
            self.add_log_line(f"❌ Error loading local CRs: {e}")

    def handle_apply_selection(self, service_key):
        """Handle apply CR service selection"""
        service_map = {
            '1': ('vms', 'WindowsVM'),
            '2': ('mssql', 'MSSQL'),
            '3': ('otel', 'OTel')
        }
        
        if service_key in service_map:
            service_type, service_name = service_map[service_key]
            self.add_log_line(f"📝 Selected: Apply {service_name} CRs")
            self.show_cr_selection_for_apply(service_type, service_name)
    
    def handle_delete_selection(self, service_key):
        """Handle delete CR service selection"""
        service_map = {
            '1': ('vms', 'Windows VM CRs'),
            '2': ('mssql', 'MSSQL CRs'),
            '3': ('otel', 'OpenTelemetry CRs')
        }
        
        if service_key in service_map:
            service_type, service_name = service_map[service_key]
            self.add_log_line(f"🗑️ Selected: Delete {service_name}")
            self.show_delete_method_selection(service_type, service_name)
    
    def show_install_method_selection(self, service_type, service_name):
        """Show method selection for install"""
        self.add_log_line("")
        self.add_log_line(f"🔧 Choose Install Method for {service_name}:")
        self.add_log_line("1️⃣ kubectl apply - Direct Kubernetes deployment") 
        self.add_log_line("2️⃣ Ansible Playbook - Automated provisioning")
        self.add_log_line("3️⃣ Manual CR Generation - Create files only")
        self.add_log_line("")
        self.add_log_line("Press 1-3 to select method...")
        
        self.menu_state = 'install_method_selection'
        self.selected_service_type = service_type
        self.selected_service_name = service_name
    
    def show_uninstall_method_selection(self, service_type, service_name):
        """Show method selection for uninstall"""
        self.add_log_line("")
        self.add_log_line(f"🔧 Choose Uninstall Method for {service_name}:")
        self.add_log_line("1️⃣ kubectl delete - Remove from Kubernetes")
        self.add_log_line("2️⃣ Ansible Cleanup - Full resource cleanup") 
        self.add_log_line("3️⃣ CR Update to 'uninstall' - Modify existing CR")
        self.add_log_line("")
        self.add_log_line("Press 1-3 to select method...")
        
        self.menu_state = 'uninstall_method_selection'
        self.selected_service_type = service_type
        self.selected_service_name = service_name
    
    def show_delete_method_selection(self, service_type, service_name):
        """Show method selection for delete CRs"""
        self.add_log_line("")
        self.add_log_line(f"🔧 Choose Delete Method for {service_name}:")
        self.add_log_line("1️⃣ kubectl delete - Remove from cluster")
        self.add_log_line("2️⃣ Graceful shutdown - Stop services first")
        self.add_log_line("3️⃣ Force delete - Immediate removal")
        self.add_log_line("")
        self.add_log_line("Press 1-3 to select method...")
        
        self.menu_state = 'delete_method_selection'
        self.selected_service_type = service_type
        self.selected_service_name = service_name
    
    def show_service_selection_for_install(self):
        """Show service type selection for installation"""
        self.add_log_line("")
        self.add_log_line(f"Step 2: Select Service Type (Method: {self.selected_method})")
        self.add_log_line("")
        self.add_log_line("Available Services:")
        self.add_log_line("1️⃣ Windows VMs - Virtual Machine deployment")
        self.add_log_line("2️⃣ MSSQL Servers - SQL Server instances")
        self.add_log_line("3️⃣ OpenTelemetry - Monitoring collectors")
        self.add_log_line("")
        self.add_log_line("Press 1-3 to select service type...")
        self.menu_state = 'service_selection'
    
    def show_service_selection_for_uninstall(self):
        """Show service type selection for uninstallation"""
        self.add_log_line("")
        self.add_log_line(f"Step 2: Select Service Type (Method: {self.selected_method})")
        self.add_log_line("")
        self.add_log_line("Available Services:")
        self.add_log_line("1️⃣ Windows VMs - Running virtual machines")
        self.add_log_line("2️⃣ MSSQL Servers - Active SQL instances")
        self.add_log_line("3️⃣ OpenTelemetry - Running collectors")
        self.add_log_line("")
        self.add_log_line("Press 1-3 to select service type...")
        self.menu_state = 'service_selection'
    
    def handle_service_selection(self, key):
        """Handle service type selection"""
        service_map = {
            '1': ('vms', 'Windows VMs'),
            '2': ('mssql', 'MSSQL Servers'),
            '3': ('otel', 'OpenTelemetry Collectors')
        }
        
        if key not in service_map:
            return
            
        service_type, service_name = service_map[key]
        self.add_log_line(f"✅ Selected: {service_name}")
        
        # Switch to the appropriate tab first
        self.active_service_tab = service_type
        if service_type == 'vms':
            self.show_vms_tab(None)
        elif service_type == 'mssql':
            self.show_mssql_tab(None)
        elif service_type == 'otel':
            self.show_otel_tab(None)
        
        # Now execute the action based on method and service
        self.execute_selected_action(service_type, service_name)
        
        # Reset menu state
        self.menu_state = None
    
    def execute_selected_action(self, service_type, service_name):
        """Execute the selected action with method and service type"""
        action = self.pending_action
        method = self.selected_method
        
        self.add_log_line("")
        self.add_log_line(f"🎯 Executing: {action.upper()} {service_name}")
        self.add_log_line(f"🔧 Method: {method}")
        self.add_log_line("")
        
        if action == 'install':
            self.execute_install_action(service_type, method)
        elif action == 'uninstall':
            self.execute_uninstall_action(service_type, method)
        
        # Reset state
        self.pending_action = None
        self.selected_method = None
    
    def execute_install_action(self, service_type, method):
        """Execute installation with specified method"""
        try:
            status_report = self.service_manager.get_comprehensive_status()
            
            if method == 'kubectl':
                self.add_log_line("📋 Available local CRs for kubectl apply:")
                self.show_available_crs_for_install(service_type, status_report)
                self.add_log_line("💡 Use 'Apply CRs' menu to select specific CRs")
                
            elif method == 'ansible':
                self.add_log_line("📋 Available services for Ansible deployment:")
                self.show_available_services_for_ansible(service_type, status_report)
                self.add_log_line("💡 Ansible will handle full provisioning workflow")
                
            elif method == 'manual':
                self.add_log_line("📝 Manual CR generation mode:")
                self.add_log_line("• Generate CR files without applying")
                self.add_log_line("• Review and modify before deployment")
                self.add_log_line("💡 Use 'Fix Issues' to generate missing CRs")
                
        except Exception as e:
            self.add_log_line(f"❌ Error in install action: {e}")
    
    def execute_uninstall_action(self, service_type, method):
        """Execute uninstallation with specified method"""
        try:
            status_report = self.service_manager.get_comprehensive_status()
            
            if method == 'kubectl':
                self.add_log_line("📋 Deployed CRs available for kubectl delete:")
                self.show_deployed_crs_for_delete(service_type, status_report)
                self.add_log_line("💡 Use 'Delete CR' menu to remove specific CRs")
                
            elif method == 'ansible':
                self.add_log_line("📋 Running services for Ansible cleanup:")
                self.show_running_services_for_cleanup(service_type, status_report)
                self.add_log_line("💡 Ansible will handle full cleanup workflow")
                
            elif method == 'cr_update':
                self.add_log_line("📝 CR update mode (action: uninstall):")
                self.add_log_line("• Modify local CRs to set action='uninstall'")
                self.add_log_line("• Apply updated CRs to trigger uninstall")
                self.add_log_line("💡 Safer method that preserves configuration")
                
        except Exception as e:
            self.add_log_line(f"❌ Error in uninstall action: {e}")
    
    def show_available_crs_for_install(self, service_type, status_report):
        """Show available CRs for the selected service type"""
        service_map = {
            'vms': 'windowsvms',
            'mssql': 'mssqlservers', 
            'otel': 'otelcollectors'
        }
        
        service_key = service_map.get(service_type)
        if not service_key:
            return
            
        service_data = status_report.get(service_key, {})
        local_crs = service_data.get('local_crs', {})
        
        if local_crs:
            for name, cr_data in local_crs.items():
                enabled = cr_data.get('enabled', True)
                status_icon = '✅' if enabled else '⏸️'
                self.add_log_line(f"  {status_icon} {name}")
        else:
            self.add_log_line(f"  ❌ No local {service_type} CRs found")
    
    def show_deployed_crs_for_delete(self, service_type, status_report):
        """Show deployed CRs for the selected service type"""
        service_map = {
            'vms': 'windowsvms',
            'mssql': 'mssqlservers',
            'otel': 'otelcollectors'
        }
        
        service_key = service_map.get(service_type)
        if not service_key:
            return
            
        service_data = status_report.get(service_key, {})
        deployed_crs = service_data.get('deployed_crs', {})
        
        if deployed_crs:
            for name, cr_data in deployed_crs.items():
                status = cr_data.get('status', {}).get('phase', 'Unknown')
                self.add_log_line(f"  🗑️ {name} (status: {status})")
        else:
            self.add_log_line(f"  ❌ No deployed {service_type} CRs found")
    
    def show_available_services_for_ansible(self, service_type, status_report):
        """Show available services for Ansible deployment"""
        self.add_log_line("  🔧 Ansible will provision:")
        self.add_log_line(f"    • {service_type.title()} instances")
        self.add_log_line("    • Required dependencies")
        self.add_log_line("    • Network configuration")
        self.add_log_line("    • Storage resources")
    
    def show_running_services_for_cleanup(self, service_type, status_report):
        """Show running services for cleanup"""
        if service_type == 'vms':
            scenarios = status_report.get('scenarios', {})
            running_vms = [name for name, data in scenarios.items() 
                          if 'Running' in data.get('scenario', '')]
            if running_vms:
                for vm_name in running_vms:
                    self.add_log_line(f"  🖥️ {vm_name}")
            else:
                self.add_log_line("  ❌ No running VMs found")
        else:
            self.add_log_line(f"  🔧 Will clean up all {service_type} resources")
    
    def show_available_crs_for_apply(self):
        """Show available CRs for the current active tab"""
        self.add_log_line("")
        self.add_log_line(f"Step 2: Available CRs for {self.active_service_tab.upper()} (Method: {self.selected_method})")
        self.add_log_line("")
        
        try:
            status_report = self.service_manager.get_comprehensive_status()
            
            service_map = {
                'vms': 'windowsvms',
                'mssql': 'mssqlservers',
                'otel': 'otelcollectors'
            }
            
            service_key = service_map.get(self.active_service_tab)
            if service_key:
                service_data = status_report.get(service_key, {})
                local_crs = service_data.get('local_crs', {})
                deployed_crs = service_data.get('deployed_crs', {})
                
                if local_crs:
                    self.add_log_line("📁 Local CRs available for apply:")
                    for name, cr_data in local_crs.items():
                        enabled = cr_data.get('enabled', True)
                        is_deployed = name in deployed_crs
                        status_icon = '✅' if enabled else '⏸️'
                        deploy_status = ' (Already Deployed)' if is_deployed else ' (Ready to Deploy)'
                        self.add_log_line(f"  {status_icon} {name}{deploy_status}")
                else:
                    self.add_log_line(f"❌ No local {self.active_service_tab} CRs found")
                    
                if self.selected_method == 'batch' and local_crs:
                    enabled_count = sum(1 for cr in local_crs.values() if cr.get('enabled', True))
                    self.add_log_line("")
                    self.add_log_line(f"🚀 Batch mode will apply {enabled_count} enabled CRs")
                elif self.selected_method == 'dry_run':
                    self.add_log_line("")
                    self.add_log_line("🔍 Dry-run will validate CRs without applying")
                    
        except Exception as e:
            self.add_log_line(f"❌ Error loading CRs: {e}")
            
        self.add_log_line("")
        self.add_log_line("💡 Use F3/F4/F5 to switch service tabs, then rerun Apply CRs")
        self.menu_state = None  # Reset menu state
        self.pending_action = None
        self.selected_method = None
    
    def show_deployed_crs_for_delete_action(self):
        """Show deployed CRs for the current active tab"""
        self.add_log_line("")
        self.add_log_line(f"Step 2: Deployed CRs for {self.active_service_tab.upper()} (Method: {self.selected_method})")
        self.add_log_line("")
        
        try:
            status_report = self.service_manager.get_comprehensive_status()
            
            service_map = {
                'vms': 'windowsvms',
                'mssql': 'mssqlservers',
                'otel': 'otelcollectors'
            }
            
            service_key = service_map.get(self.active_service_tab)
            if service_key:
                service_data = status_report.get(service_key, {})
                deployed_crs = service_data.get('deployed_crs', {})
                
                if deployed_crs:
                    self.add_log_line("☸️ Deployed CRs available for deletion:")
                    for name, cr_data in deployed_crs.items():
                        status = cr_data.get('status', {}).get('phase', 'Unknown')
                        color_icon = '🟢' if status == 'Ready' else '🟡' if status == 'Pending' else '🔴'
                        method_note = ''
                        if self.selected_method == 'graceful':
                            method_note = ' (Will stop services first)'
                        elif self.selected_method == 'force':
                            method_note = ' (Immediate removal)'
                        self.add_log_line(f"  {color_icon} {name} (status: {status}){method_note}")
                else:
                    self.add_log_line(f"❌ No deployed {self.active_service_tab} CRs found")
                    
                if self.selected_method == 'graceful':
                    self.add_log_line("")
                    self.add_log_line("🔄 Graceful shutdown will:")
                    self.add_log_line("  • Stop running services cleanly")
                    self.add_log_line("  • Wait for processes to terminate")
                    self.add_log_line("  • Remove CRs after cleanup")
                elif self.selected_method == 'force':
                    self.add_log_line("")
                    self.add_log_line("⚠️ Force delete will immediately remove CRs")
                    self.add_log_line("   This may leave orphaned resources!")
                    
        except Exception as e:
            self.add_log_line(f"❌ Error loading deployed CRs: {e}")
            
        self.add_log_line("")
        self.add_log_line("💡 Use F3/F4/F5 to switch service tabs, then rerun Delete CR")
        self.menu_state = None  # Reset menu state
        self.pending_action = None
        self.selected_method = None
    
    def delete_cr_menu(self, button):
        """Show submenu with all local CR YAMLs for deletion (no method selection)"""
        import os
        folder = '/root/kubernetes-installer/manifest-controller'
        files = os.listdir(folder)
        cr_files = [f for f in files if f.endswith('.yaml') and 'crd' not in f.lower()]
        if cr_files:
            cr_options = []
            for fname in cr_files:
                cr_path = os.path.join(folder, fname)
                cr_options.append((fname, cr_path, 'Local CR YAML'))
            def handle_cr_delete_selection(cr_name, cr_path, _status=None):
                self.add_log_line(f"🗑️ Deleting CR: {cr_name}")
                import subprocess
                result = subprocess.run(['kubectl', 'delete', '-f', cr_path], capture_output=True, text=True)
                if result.returncode == 0:
                    self.add_log_line(f"✅ Deleted CR: {cr_name}")
                else:
                    err = result.stderr
                    if 'NotFound' in err and 'error when deleting' in err:
                        self.add_log_line(f"⚠️ CR not found in cluster (already deleted or never applied): {cr_name}")
                    elif 'CRD' in err and 'not found' in err:
                        pass  # Suppress CRD not found, skipping message
                    else:
                        self.add_log_line(f"❌ Failed to delete CR {cr_name}: {err}")
            def popup_callback(cr_name, cr_path, status=None):
                self.close_popup()
                handle_cr_delete_selection(cr_name, cr_path, status)
            cr_options_for_popup = [(name, path, status) for (name, path, status) in cr_options]
            self.show_cr_selection_popup(
                f"Delete Local CR YAML",
                cr_options_for_popup,
                popup_callback
            )
        else:
            self.add_log_line(f"❌ No local CR YAMLs found in manifest-controller")
    
    def fix_issues(self, button):
        """Smart issue fixing"""
        self.add_log_line("🔧 === FIX ISSUES (SMART) ===")
        self.add_log_line("Analyzing system for common issues...")
        self.add_log_line("• Orphaned VMs")
        self.add_log_line("• Missing CRs")
        self.add_log_line("• Service mismatches")
        self.add_log_line("TODO: Implement smart issue detection")
    
    def clear_logs(self, button):
        """Clear log display"""
        self.log_walker.clear()
        self.add_log_line("🧹 Logs cleared")
    
    def quit_app(self, button):
        """Quit application"""
        raise urwid.ExitMainLoop()
    
    def update_focus_indicators(self):
        """Update focus indicators in panel titles"""
        current_focus = self.content_columns.focus_position
        if current_focus == 0:
            self.status_frame.set_title("VMs & Services Status [FOCUSED]")
            self.log_frame.set_title("System Logs")
        else:
            self.status_frame.set_title("VMs & Services Status")
            self.log_frame.set_title("System Logs [FOCUSED]")
    
    def reset_focus_and_navigation(self):
        """Reset focus and navigation state"""
        try:
            self.content_columns.focus_position = 1  # Default to logs
            self.update_focus_indicators()
            self.add_log_line("🔄 Focus and navigation reset")
        except Exception as e:
            self.add_log_line(f"❌ Reset failed: {e}")
    
    def unhandled_input(self, key):
        """Handle keyboard input with popup support"""
        # Removed noisy debug log
        # SPECIAL DEBUG: Log extra info for Enter keys
        if key == 'enter':
            self.add_log_line(f"🔥 ENTER KEY DETECTED! menu_state={self.menu_state}")
            if hasattr(self, 'popup_listbox') and self.popup_listbox:
                try:
                    focus_widget = self.popup_listbox.focus
                    self.add_log_line(f"🔥 ENTER: focus_widget type: {type(focus_widget)}")
                    if hasattr(focus_widget, 'original_widget'):
                        button = focus_widget.original_widget
                        self.add_log_line(f"🔥 ENTER: button type: {type(button)}")
                        if hasattr(button, 'cr_name'):
                            self.add_log_line(f"🔥 ENTER: button.cr_name = {button.cr_name}")
                except Exception as e:
                    self.add_log_line(f"🔥 ENTER: Debug error: {e}")
        
        # Always handle ESC: close popup or step back from any menu
        if key == 'escape':
            if self.menu_state == 'cr_popup' and self.popup:
                self.add_log_line("🔙 CR_POPUP: ESC pressed, closing popup and resetting menu state")
                self.close_popup()
                if hasattr(self, 'original_widget'):
                    self.loop.widget = self.original_widget
                self.menu_state = None
                self.popup_listbox = None
                self.reset_menu_state()
                return
            elif self.popup:
                self.close_popup()
                if hasattr(self, 'original_widget'):
                    self.loop.widget = self.original_widget
                return
            elif self.menu_state:
                self.add_log_line("🔙 Menu cancelled - returning to main")
                self.menu_state = None
                self.reset_menu_state()
                return
        
        # Handle CR popup selection
        elif self.menu_state == 'cr_popup':
            self.add_log_line(f"🔑 CR_POPUP: Key={key} received in unhandled_input!")
            if key in ('1', '2', '3', '4', '5'):
                # Handle number key shortcuts for CR selection
                self.add_log_line(f"🔑 CR_POPUP: Number key {key} pressed!")
                try:
                    index = int(key) - 1  # Convert to 0-based index
                    if hasattr(self, 'popup_listbox') and self.popup_listbox:
                        if index < len(self.popup_listbox.body):
                            # Get the button at the specified index
                            button_widget = self.popup_listbox.body[index]
                            if hasattr(button_widget, 'original_widget'):
                                button = button_widget.original_widget
                                if hasattr(button, 'cr_name') and hasattr(button, 'cr_data'):
                                    self.add_log_line(f"🔥 Number shortcut selecting: {button.cr_name}")
                                    cr_name = button.cr_name
                                    cr_data = button.cr_data
                                    self.close_popup()
                                    if hasattr(self, 'popup_callback') and self.popup_callback:
                                        self.popup_callback(cr_name, cr_data)
                                    return
                except Exception as e:
                    self.add_log_line(f"❌ Error with number shortcut: {str(e)}")
                return
            # For all other keys (including arrows, Enter), let urwid handle them naturally
            return  # Don't consume any other keys
        
        # Handle other popup types (already handled above)
        elif self.popup:
            if key in ('up', 'down'):
                # Let the listbox handle arrow navigation
                return key
        
        
        # Handle method selection states
        elif self.menu_state == 'install_method_selection':
            if key == '1':
                self.selected_method = 'kubectl'
                self.add_log_line("✅ Selected: kubectl apply method")
                self.execute_install_with_method()
                return
            elif key == '2':
                self.selected_method = 'ansible'
                self.add_log_line("✅ Selected: Ansible Playbook method")
                self.execute_install_with_method()
                return
            elif key == '3':
                self.selected_method = 'manual'
                self.add_log_line("✅ Selected: Manual CR Generation")
                self.execute_install_with_method()
                return
        elif self.menu_state == 'uninstall_method_selection':
            if key == '1':
                self.selected_method = 'kubectl'
                self.add_log_line("✅ Selected: kubectl delete method")
                self.execute_uninstall_with_method()
                return
            elif key == '2':
                self.selected_method = 'ansible'
                self.add_log_line("✅ Selected: Ansible Cleanup method")
                self.execute_uninstall_with_method()
                return
            elif key == '3':
                self.selected_method = 'cr_update'
                self.add_log_line("✅ Selected: CR Update method")
                self.execute_uninstall_with_method()
                return
        elif self.menu_state == 'delete_method_selection':
            if key == '1':
                self.selected_method = 'kubectl'
                self.add_log_line("✅ Selected: kubectl delete method")
                self.execute_delete_with_method()
                return
            elif key == '2':
                self.selected_method = 'graceful'
                self.add_log_line("✅ Selected: Graceful shutdown")
                self.execute_delete_with_method()
                return
            elif key == '3':
                self.selected_method = 'force'
                self.add_log_line("✅ Selected: Force delete")
                self.execute_delete_with_method()
                return
        
        # Standard navigation and shortcuts
        if key in ('q', 'Q'):
            raise urwid.ExitMainLoop()
        elif key == 'ctrl c':
            self.add_log_line("🛑 CTRL+C pressed - Shutting down...")
            raise urwid.ExitMainLoop()
        # (handled above)
        elif key == 'f2':
            # F2 - Show current tab status
            self.update_status_display()
            self.add_log_line("📊 Status refreshed (F2)")
        elif key == 'f6':
            # F6 - Install menu
            self.install_menu(None)
        elif key == 'f7':
            # F7 - Apply CR menu
            self.apply_cr_menu(None)
        elif key == 'f2':
            # F2 - Show current tab status
            self.update_status_display()
            self.add_log_line("📊 Status refreshed (F2)")
        elif key == 'f3':
            # F3 - VMs tab
            self.show_vms_tab(None)
        elif key == 'f4':
            # F4 - MSSQL tab
            self.show_mssql_tab(None)
        elif key == 'f5':
            # F5 - OTel tab
            self.show_otel_tab(None)
        elif key == 'f6':
            # F6 - Install menu
            self.install_menu(None)
        elif key == 'f7':
            # F7 - Apply CR menu
            self.apply_cr_menu(None)
        elif key == 'f8':
            # F8 - Toggle auto-scroll
            self.auto_scroll = not self.auto_scroll
            status = "ON" if self.auto_scroll else "OFF"
            self.add_log_line(f"📜 Auto-scroll: {status} (F8)")
            if self.auto_scroll and self.log_walker:
                try:
                    self.log_listbox.focus_position = len(self.log_walker) - 1
                except:
                    pass
        elif key == 'f9':
            # F9 - Reset focus
            self.reset_focus_and_navigation()
        elif key in ('left', 'right'):
            # Arrow keys for panel navigation
            try:
                if key == 'left':
                    self.content_columns.focus_position = 0
                    self.update_focus_indicators()
                    self.add_log_line("📊 Moved to Status Panel (←)")
                else:
                    self.content_columns.focus_position = 1
                    self.update_focus_indicators()
                    self.add_log_line("📜 Moved to Log Panel (→)")
            except Exception as e:
                self.add_log_line(f"❌ Navigation error: {e}")
        elif key == 'tab':
            # Tab navigation
            try:
                current_focus = self.content_columns.focus_position
                new_focus = 1 - current_focus
                self.content_columns.focus_position = new_focus
                self.update_focus_indicators()
                
                if new_focus == 0:
                    self.add_log_line("📊 Switched to Status Panel (Tab)")
                else:
                    self.add_log_line("📜 Switched to Log Panel (Tab)")
            except Exception as e:
                self.add_log_line(f"❌ Tab navigation error: {e}")
        elif key in ('up', 'down', 'page up', 'page down'):
            # Handle scrolling - disable auto-scroll when manually scrolling
            try:
                if self.content_columns.focus_position == 1:  # Logs panel focused
                    self.auto_scroll = False
                return key
            except Exception as e:
                self.add_log_line(f"❌ Scrolling error: {e}")
                return
        else:
            return key
    
    def reset_menu_state(self):
        """Reset all menu state variables"""
        self.pending_action = None
        self.selected_method = None
        self.selected_service_type = None
        self.selected_service_name = None
        self.popup_callback = None
        self.popup_action = None
        self.service_options = None
        self.popup_listbox = None
    
    def execute_install_with_method(self):
        """Execute install action with selected service and method"""
        self.add_log_line("")
        self.add_log_line(f"🎯 INSTALLING {self.selected_service_name}")
        self.add_log_line(f"🔧 Method: {self.selected_method}")
        self.add_log_line("")
        
        try:
            status_report = self.service_manager.get_comprehensive_status()
            
            if self.selected_method == 'kubectl':
                self.show_available_crs_for_install_final(self.selected_service_type, status_report)
            elif self.selected_method == 'ansible':
                self.show_ansible_install_options(self.selected_service_type, status_report)
            elif self.selected_method == 'manual':
                self.show_manual_cr_generation(self.selected_service_type, status_report)
                
        except Exception as e:
            self.add_log_line(f"❌ Error in install execution: {e}")
        finally:
            self.reset_menu_state()
    
    def execute_uninstall_with_method(self):
        """Execute uninstall action with selected service and method"""
        self.add_log_line("")
        self.add_log_line(f"🎯 UNINSTALLING {self.selected_service_name}")
        self.add_log_line(f"🔧 Method: {self.selected_method}")
        self.add_log_line("")
        
        try:
            status_report = self.service_manager.get_comprehensive_status()
            
            if self.selected_method == 'kubectl':
                self.show_deployed_crs_for_uninstall(self.selected_service_type, status_report)
            elif self.selected_method == 'ansible':
                self.show_ansible_cleanup_options(self.selected_service_type, status_report)
            elif self.selected_method == 'cr_update':
                self.show_cr_update_options(self.selected_service_type, status_report)
                
        except Exception as e:
            self.add_log_line(f"❌ Error in uninstall execution: {e}")
        finally:
            self.reset_menu_state()
    
    def execute_delete_with_method(self):
        """Show all CR YAMLs in manifest-controller for deletion"""
        import os
        self.add_log_line("")
        self.add_log_line(f"🎯 DELETING {self.selected_service_name}")
        self.add_log_line(f"🔧 Method: {self.selected_method}")
        self.add_log_line("")
        try:
            folder = '/root/kubernetes-installer/manifest-controller'
            files = os.listdir(folder)
            cr_files = [f for f in files if f.endswith('.yaml') and 'crd' not in f.lower()]
            if cr_files:
                cr_options = []
                for fname in cr_files:
                    cr_path = os.path.join(folder, fname)
                    cr_options.append((fname, cr_path, 'Local CR YAML'))
                def handle_cr_delete_selection(cr_name, cr_path, _status=None):
                    self.add_log_line(f"🗑️ Deleting CR: {cr_name} using {self.selected_method}")
                    import subprocess
                    result = subprocess.run(['kubectl', 'delete', '-f', cr_path], capture_output=True, text=True)
                    if result.returncode == 0:
                        self.add_log_line(f"✅ Deleted CR: {cr_name}")
                    else:
                        self.add_log_line(f"❌ Failed to delete CR {cr_name}: {result.stderr}")
                # Wrap callback for show_cr_selection_popup
                def popup_callback(cr_name, cr_path, status=None):
                    handle_cr_delete_selection(cr_name, cr_path, status)
                # Adapt to show_cr_selection_popup signature
                cr_options_for_popup = [(name, path, status) for (name, path, status) in cr_options]
                self.show_cr_selection_popup(
                    f"Delete Local CR YAML",
                    cr_options_for_popup,
                    popup_callback
                )
            else:
                self.add_log_line(f"❌ No local CR YAMLs found in manifest-controller")
        except Exception as e:
            self.add_log_line(f"❌ Error in delete execution: {e}")
        finally:
            self.reset_menu_state()
    
    def show_available_crs_for_install_final(self, service_type, status_report):
        """Show final CR list for install"""
        service_map = {
            'vms': 'windowsvms',
            'mssql': 'mssqlservers',
            'otel': 'otelcollectors'
        }
        
        service_key = service_map.get(service_type)
        if service_key:
            service_data = status_report.get(service_key, {})
            local_crs = service_data.get('local_crs', {})
            
            if local_crs:
                self.add_log_line("📁 Available CRs for installation:")
                for name, cr_data in local_crs.items():
                    enabled = cr_data.get('enabled', True)
                    status_icon = '✅' if enabled else '⏸️'
                    self.add_log_line(f"  {status_icon} {name}")
                self.add_log_line("")
                self.add_log_line("💡 Ready to install - use service manager integration")
            else:
                self.add_log_line(f"❌ No local {service_type} CRs found")
    
    def show_available_crs_for_apply_final(self, service_type, status_report):
        """Show final CR list for apply"""
        service_map = {
            'vms': 'windowsvms',
            'mssql': 'mssqlservers',
            'otel': 'otelcollectors'
        }
        
        service_key = service_map.get(service_type)
        if service_key:
            service_data = status_report.get(service_key, {})
            local_crs = service_data.get('local_crs', {})
            deployed_crs = service_data.get('deployed_crs', {})
            
            if local_crs:
                self.add_log_line("📁 CRs available for apply:")
                for name, cr_data in local_crs.items():
                    enabled = cr_data.get('enabled', True)
                    is_deployed = name in deployed_crs
                    status_icon = '✅' if enabled else '⏸️'
                    deploy_status = ' (Already Deployed)' if is_deployed else ' (Ready to Deploy)'
                    self.add_log_line(f"  {status_icon} {name}{deploy_status}")
                
                if self.selected_method == 'batch':
                    enabled_count = sum(1 for cr in local_crs.values() if cr.get('enabled', True))
                    self.add_log_line(f"🚀 Batch mode will apply {enabled_count} enabled CRs")
                elif self.selected_method == 'dry_run':
                    self.add_log_line("🔍 Dry-run will validate CRs without applying")
            else:
                self.add_log_line(f"❌ No local {service_type} CRs found")
    
    def show_deployed_crs_for_delete_final(self, service_type, status_report):
        """Show final deployed CR list for delete"""
        service_map = {
            'vms': 'windowsvms',
            'mssql': 'mssqlservers',
            'otel': 'otelcollectors'
        }
        
        service_key = service_map.get(service_type)
        if service_key:
            service_data = status_report.get(service_key, {})
            deployed_crs = service_data.get('deployed_crs', {})
            
            if deployed_crs:
                self.add_log_line("☸️ Deployed CRs available for deletion:")
                for name, cr_data in deployed_crs.items():
                    status = cr_data.get('status', {}).get('phase', 'Unknown')
                    color_icon = '🟢' if status == 'Ready' else '🟡' if status == 'Pending' else '🔴'
                    self.add_log_line(f"  {color_icon} {name} (status: {status})")
                
                if self.selected_method == 'graceful':
                    self.add_log_line("🔄 Graceful shutdown will stop services cleanly first")
                elif self.selected_method == 'force':
                    self.add_log_line("⚠️ Force delete will immediately remove CRs")
            else:
                self.add_log_line(f"❌ No deployed {service_type} CRs found")
    
    def show_ansible_install_options(self, service_type, status_report):
        """Show Ansible install options"""
        self.add_log_line("🔧 Ansible Playbook Installation:")
        self.add_log_line("  • Full automated provisioning")
        self.add_log_line("  • Dependency management")
        self.add_log_line("  • Network configuration")
        self.add_log_line("  • Storage resources")
    
    def show_deployed_crs_for_uninstall(self, service_type, status_report):
        """Show deployed CRs for uninstall"""
        self.show_deployed_crs_for_delete_final(service_type, status_report)
    
    def show_ansible_cleanup_options(self, service_type, status_report):
        """Show Ansible cleanup options"""
        self.add_log_line("🔧 Ansible Cleanup Process:")
        self.add_log_line("  • Full resource cleanup")
        self.add_log_line("  • Dependency removal")
        self.add_log_line("  • Network cleanup")
        self.add_log_line("💡 Comprehensive cleanup via Ansible")
    
    def show_cr_update_options(self, service_type, status_report):
        """Show CR update options"""
        self.add_log_line("  • Modify local CRs to set action='uninstall'")
        self.add_log_line("  • Apply updated CRs to trigger uninstall")
        self.add_log_line("💡 Safer method that preserves configuration")
    
    def update_logs(self):
        """Update logs from the queue only (no file tailing)"""
        updated = False
        try:
            # Process multiple log entries from log_queue
            for _ in range(5):
                log_line = log_queue.get_nowait()
                self.add_log_line(log_line)
                updated = True
        except queue.Empty:
            pass
        # Schedule next update
        if hasattr(self, 'loop') and self.loop:
            self.loop.set_alarm_in(0.3, lambda loop, user_data: self.update_logs())
            # Auto-refresh status every 5 seconds
            self.loop.set_alarm_in(5.0, lambda loop, user_data: self.auto_refresh_status())
    
       
    def auto_refresh_status(self):
        """Automatically refresh status display"""
        try:
            self.update_status_display()
            # Schedule next auto-refresh
            if hasattr(self, 'loop') and self.loop:
                self.loop.set_alarm_in(5.0, lambda loop, user_data: self.auto_refresh_status())
        except Exception as e:
            logger.warning(f"Auto-refresh failed: {e}")
    
    def initial_startup(self):
        """Perform initial startup tasks"""
        try:
            # Load initial status
            self.update_status_display()
            self.add_log_line("📊 Initial status display loaded")
            
            # Show startup summary
            status_report = self.service_manager.get_comprehensive_status()
            summary = status_report.get('summary', {})
            
            for service_type, service_summary in summary.items():
                local_count = service_summary.get('local_count', 0)
                deployed_count = service_summary.get('deployed_count', 0)
                self.add_log_line(f"📈 {service_type.upper()}: {local_count} local, {deployed_count} deployed")
                
        except Exception as e:
            self.add_log_line(f"⚠️ Error during startup: {e}")
    
    def execute_cr_install(self, service_type, service_name, cr_name, cr_data):
        """Execute CR installation directly"""
        self.add_log_line(f"🚀 Installing {cr_name}...")
        try:
            # Apply the CR to Kubernetes
            self.add_log_line(f"📝 Applying Custom Resource...")
            import os
            cr_file_path = None
            if 'file' in cr_data:
                filename = cr_data['file']
                cr_file_path = f"/root/kubernetes-installer/manifest-controller/{filename}"
            else:
                possible_paths = [
                    f"/root/kubernetes-installer/manifest-controller/{cr_name}-cr.yaml",
                    f"/root/kubernetes-installer/manifest-controller/{cr_name}.yaml"
                ]
                for path in possible_paths:
                    if os.path.exists(path):
                        cr_file_path = path
                        break
            if not cr_file_path or not os.path.exists(cr_file_path):
                self.add_log_line(f"❌ CR file not found for {cr_name}")
                return
            import subprocess
            result = subprocess.run(['kubectl', 'apply', '-f', cr_file_path], capture_output=True, text=True)
            if result.returncode == 0:
                self.add_log_line(f"✅ Custom Resource applied successfully from file: {cr_file_path}")
                self.add_log_line(f"⏳ Waiting for operator to process CR and run playbook...")
                self.add_log_line(f"💡 Playbook will be started by the operator, not the TUI.")
            else:
                self.add_log_line(f"❌ Failed to apply CR: {result.stderr}")
        except Exception as e:
            self.add_log_line(f"❌ Installation failed: {str(e)}")
        self.menu_state = 'main'
    
    def execute_cr_uninstall(self, service_type, service_name, cr_name, cr_data):
        """Execute CR uninstallation directly"""
        self.add_log_line(f"🗑️ Uninstalling {cr_name}...")
        
        try:
            # Get the full CR if available, otherwise reconstruct it
            if 'full_cr' in cr_data:
                full_cr = cr_data['full_cr'].copy()
            else:
                # Reconstruct CR from available data (for local CRs)
                full_cr = {
                    'apiVersion': 'infra.example.com/v1',
                    'kind': service_name,
                    'metadata': {
                        'name': cr_name,
                        'namespace': cr_data.get('namespace', 'default')
                    },
                    'spec': cr_data.get('spec', {})
                }
            
            # Update CR with uninstall action
            import yaml
            
            if 'spec' not in full_cr:
                full_cr['spec'] = {}
            full_cr['spec']['action'] = 'uninstall'
            
            # Apply updated CR to trigger uninstall
            import tempfile
            import os
            import subprocess
            
            cr_yaml = yaml.dump(full_cr, default_flow_style=False)
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                f.write(cr_yaml)
                temp_file = f.name
            
            try:
                # Apply CR with uninstall action
                self.add_log_line(f"📝 Updating CR with uninstall action...")
                result = subprocess.run(['kubectl', 'apply', '-f', temp_file], 
                                      capture_output=True, text=True)
                
                if result.returncode == 0:
                    self.add_log_line(f"✅ Custom Resource updated for uninstall")
                    
                    # Run uninstall playbook
                    playbook_map = {
                        'WindowsVM': 'k8s-redhat-kubernetes-uninstall-tasks.yaml',
                        'MSSQL': 'mssql-uninstall-tasks.yaml',
                        'OTel': 'otel-uninstall-tasks.yaml'
                    }
                    
                    playbook = playbook_map.get(service_name)
                    if playbook:
                        playbook_path = f"/root/kubernetes-installer/kubernetes/{playbook}"
                        if os.path.exists(playbook_path):
                            self.add_log_line(f"🎭 Running uninstall playbook...")
                            result = subprocess.run(['ansible-playbook', playbook_path], 
                                                  capture_output=True, text=True)
                            
                            if result.returncode == 0:
                                self.add_log_line(f"✅ Uninstall completed successfully!")
                            else:
                                self.add_log_line(f"⚠️ Playbook completed with warnings: {result.stderr}")
                        else:
                            self.add_log_line(f"❌ Failed to update CR: {result.stderr}")
                    
            finally:
                # Clean up temp file
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
                    
        except Exception as e:
            self.add_log_line(f"❌ Uninstallation failed: {str(e)}")
        
        self.menu_state = 'main'
    
    def execute_cr_apply(self, service_type, service_name, cr_name, cr_data):
        """Execute CR application directly"""
        self.add_log_line(f"📝 Applying {cr_name}...")
        
        try:
            # Apply the CR to Kubernetes
            self.add_log_line(f"📝 Applying Custom Resource to cluster...")
            try:
                # Always use the original CR file if available
                import os
                cr_file_path = None
                if 'file' in cr_data:
                    filename = cr_data['file']
                    cr_file_path = f"/root/kubernetes-installer/manifest-controller/{filename}"
                else:
                    possible_paths = [
                        f"/root/kubernetes-installer/manifest-controller/{cr_name}-cr.yaml",
                        f"/root/kubernetes-installer/manifest-controller/{cr_name}.yaml"
                    ]
                    for path in possible_paths:
                        if os.path.exists(path):
                            cr_file_path = path
                            break
                if not cr_file_path or not os.path.exists(cr_file_path):
                    self.add_log_line(f"❌ CR file not found for {cr_name}")
                    return
                # Apply the CR file directly
                import subprocess
                result = subprocess.run(['kubectl', 'apply', '-f', cr_file_path], capture_output=True, text=True)
                if result.returncode == 0:
                    self.add_log_line(f"✅ Custom Resource applied successfully from file: {cr_file_path}")
                else:
                    self.add_log_line(f"❌ Failed to apply CR: {result.stderr}")
            except Exception as e:
                self.add_log_line(f"❌ Application failed: {str(e)}")
            self.menu_state = 'main'
        except Exception as e:
            self.add_log_line(f"❌ Application failed: {str(e)}")
    
    def force_key_handler(self, key):
        """Forced key handler that logs everything and handles CR selection directly"""
    # Removed verbose key log
        
        # Check if we have a popup open (regardless of menu_state)
        has_popup = hasattr(self, 'popup') and self.popup is not None
        
        if has_popup and key == '1':
            # Force close popup and trigger installation
            self.close_popup()
            # Call execute_cr_install directly with windows2025v3
            self.execute_cr_install('WindowsVM', 'Windows VMs', 'windows2025v3', {'action': 'install'})
            return
        
        # Handle ESC for any popup
        if has_popup and key == 'escape':
            self.close_popup()
            return
        
        # For CR popups, let Enter key pass through to urwid for proper button handling
        if self.menu_state == 'cr_popup' and key == 'enter':
            # Don't consume the key, let it pass through to the widget
            return key
        
        # Call the original handler
        return self.unhandled_input(key)

    def run(self):
        """Run the enhanced TUI"""
        self.loop = urwid.MainLoop(
            self.main_frame,
            self.palette,
            unhandled_input=self.force_key_handler,  # Use forced handler for robust ESC/popup handling
            handle_mouse=True
        )
        
        # Welcome messages
        self.add_log_line("=== Windows Services Management System Started ===")
        self.add_log_line("🎯 Enhanced modular system with full functionality")
        self.add_log_line("📊 Consolidated VMs/Services view - no more VMs taking all space!")
        self.add_log_line("🔧 Use F-keys for quick actions, Tab/arrows for navigation")
        self.add_log_line("⚡ Monitoring WindowsVM, MSSQL, and OpenTelemetry resources")
        
        # Load initial status and start updates
        self.loop.set_alarm_in(0.2, lambda loop, user_data: self.initial_startup())
        self.loop.set_alarm_in(0.5, lambda loop, user_data: self.update_logs())
        
        try:
            self.loop.run()
        except KeyboardInterrupt:
            pass
        finally:
            logger.info("Enhanced TUI interface shutting down")
