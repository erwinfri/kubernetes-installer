#!/usr/bin/env python3
"""
Test script to verify Enter key functionality in CR selection popup
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'modules'))

import urwid
import logging

class TestTUI:
    def __init__(self):
        self.popup = None
        self.menu_state = None
        self.original_widget = None
        
        # Create a simple main frame
        self.main_frame = urwid.Filler(urwid.Text("Test TUI - Press 's' to show CR selection popup, ESC to quit", align='center'))
        
        # Create the main loop
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
        """Close the popup and return to main screen"""
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
        elif key == 's' and not self.popup:
            # Show test CR selection popup
            test_cr_options = [
                ("Test VM 1", {"name": "vm1"}, "Ready"),
                ("Test VM 2", {"name": "vm2"}, "Already Deployed"),
                ("Test VM 3", {"name": "vm3"}, "Unknown Status"),
            ]
            self.show_cr_selection_popup("Test CR Selection", test_cr_options, self.test_callback)
    
    def test_callback(self, cr_name, cr_data):
        """Test callback for CR selection"""
        self.add_log_line(f"✅ SUCCESS: Selected {cr_name} with data {cr_data}")
        # Update main screen to show success
        success_text = f"✅ Enter key works! Selected: {cr_name}"
        self.main_frame = urwid.Filler(urwid.Text(success_text, align='center'))
        self.loop.widget = self.main_frame
    
    def show_cr_selection_popup(self, title, cr_options, callback):
        """Show CR selection in a dropdown popup with arrow navigation"""
        if not cr_options:
            self.add_log_line(f"❌ No CRs available for {title}")
            return
            
        # Create menu items for CRs using proper urwid button pattern
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
            
            # Create button callback using proper closure pattern
            def create_callback(cr_name, cr_data):
                def button_callback(button):
                    self.add_log_line(f"🔥 CR selected: {cr_name}")
                    self.close_popup()
                    callback(cr_name, cr_data)
                return button_callback
            
            # Create button with on_press callback - this is the correct urwid pattern
            button = urwid.Button(button_text, on_press=create_callback(name, cr_data))
            button.cr_name = name
            button.cr_data = cr_data
            menu_items.append(urwid.AttrMap(button, 'button', 'button_focus'))
        
        # Use standard ListBox - urwid will handle Enter key properly
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
            valign='middle', height=min(len(menu_items) + 6, 14)
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
        """Start the TUI"""
        print("Starting Enter key test...")
        print("Instructions:")
        print("- Press 's' to show CR selection popup")
        print("- Use ↑↓ to navigate, Enter to select")
        print("- Press ESC to close popup or quit")
        print("")
        self.loop.run()

if __name__ == "__main__":
    app = TestTUI()
    app.run()