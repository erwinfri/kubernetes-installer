#!/usr/bin/env python3
"""
Simple test to verify Enter key functionality is fixed
"""

import sys
import os
import urwid
import logging

# Set up simple logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Mock service manager for testing
class MockServiceManager:
    def __init__(self):
        pass

# Simple test TUI class that focuses on CR popup
class TestTUI:
    def __init__(self):
        self.popup = None
        self.menu_state = None
        self.original_widget = None
        self.log_lines = []
        
        # Create a simple main frame
        self.main_frame = urwid.Filler(urwid.Text("Enter Key Test - CR Popup will show automatically", align='center'))
        
        # Color palette
        palette = [
            ('button', 'white', 'dark blue'),
            ('button_focus', 'white', 'dark red'),
            ('popup', 'white', 'dark blue'),
            ('popup_title', 'yellow', 'dark blue'),
        ]
        
        self.loop = urwid.MainLoop(
            self.main_frame,
            palette=palette,
            unhandled_input=self.handle_input
        )
    
    def add_log_line(self, message):
        """Simple logging for testing"""
        print(f"LOG: {message}")
        
    def close_popup(self):
        """Close the popup"""
        if self.original_widget:
            self.loop.widget = self.original_widget
            self.original_widget = None
        self.popup = None
        self.menu_state = None
        self.add_log_line("🔒 Popup closed")
    
    def handle_input(self, key):
        """Handle keyboard input"""
        if key == 'esc':
            if self.popup:
                self.close_popup()
                return
            else:
                raise urwid.ExitMainLoop()
    
    def test_callback(self, cr_name, cr_data):
        """Test callback for CR selection"""
        self.add_log_line(f"✅ SUCCESS! Selected {cr_name} with data {cr_data}")
        # Update main screen to show success
        success_text = f"✅ Enter key works! Selected: {cr_name}"
        self.main_frame = urwid.Filler(urwid.Text(success_text, align='center'))
        self.loop.widget = self.main_frame

    
    def show_cr_selection_popup(self, title, cr_options, callback):
        """Show CR selection popup with custom button implementation (copied from main TUI)"""
        if not cr_options:
            self.add_log_line(f"❌ No CRs available for {title}")
            return
            
        # Create menu items using the same custom button approach
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
                    self.tui.add_log_line(f"🔑 CRButton.keypress: {key} for {self.cr_name}")
                    if key in ('enter', ' '):  # Both Enter and Space should work
                        self.tui.add_log_line(f"🎯 CRButton ACTIVATED! CR: {self.cr_name}")
                        self.tui.close_popup()
                        self.callback(self.cr_name, self.cr_data)
                        return
                    return super().keypress(size, key)
            
            button = CRButton(button_text, name, cr_data, callback, self)
            button.cr_name = name
            button.cr_data = cr_data
            menu_items.append(urwid.AttrMap(button, 'button', 'button_focus'))
        
        # Use standard ListBox
        menu_walker = urwid.SimpleListWalker(menu_items)
        menu_listbox = urwid.ListBox(menu_walker)
        
        # Create simple popup content
        popup_content = urwid.Pile([
            urwid.Text(('popup_title', f"🔽 {title}"), align='center'),
            urwid.Divider('─'),
            urwid.BoxAdapter(menu_listbox, height=min(len(menu_items), 6)),
            urwid.Divider('─'),
            urwid.Text("↑↓: Navigate, Enter: Select, ESC: Cancel", align='center')
        ])
        
        popup_box = urwid.AttrMap(urwid.LineBox(popup_content, title=title), 'popup')
        
        # Center the popup
        overlay = urwid.Overlay(
            popup_box,
            self.main_frame,
            align='center', width=60,
            valign='middle', height=min(len(menu_items) + 8, 16)
        )
        
        self.popup = overlay
        
        # Store original widget before showing popup
        if not hasattr(self, 'original_widget') or not self.original_widget:
            self.original_widget = self.loop.widget
        
        # Set menu state for CR popup
        self.menu_state = 'cr_popup'
        
        # Show popup
        self.loop.widget = overlay
        self.add_log_line(f"📋 {title} opened - use ↑↓ and Enter")

    def run(self):
        """Start the test"""
        # Automatically show CR selection popup
        test_crs = [
            ("windows2025v3", {'metadata': {'name': 'windows2025v3', 'namespace': 'default'}}, "Ready"),
            ("test-vm-2", {'metadata': {'name': 'test-vm-2', 'namespace': 'default'}}, "Pending"),
            ("test-vm-3", {'metadata': {'name': 'test-vm-3', 'namespace': 'default'}}, "Running")
        ]
        
        print("Testing CR selection popup with custom buttons...")
        print("Instructions:")
        print("- Use ↑↓ to navigate")
        print("- Press Enter or Space to select")
        print("- Press ESC to cancel")
        print("")
        
        # Show popup immediately
        self.show_cr_selection_popup("Test CR Selection", test_crs, self.test_callback)
        
        # Start the main loop
        self.loop.run()

def test_cr_selection():
    """Test the CR selection with fixed Enter key handling."""
    app = TestTUI()
    
    try:
        app.run()
        print("✅ Test completed successfully!")
    except KeyboardInterrupt:
        print("Test interrupted by user")
    except Exception as e:
        print(f"Error during test: {e}")

if __name__ == "__main__":
    test_cr_selection()