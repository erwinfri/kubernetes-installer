#!/usr/bin/env python3
"""
Direct test of the TUI Enter key functionality
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'modules'))

from modules.tui_interface import WindowsServicesTUI
import threading
import time

def test_enter_key():
    print("🧪 Testing Enter key functionality...")
    
    try:
        # Create TUI instance
        tui = WindowsServicesTUI()
        
        # Test showing a CR popup directly
        test_cr_options = [
            ("test-vm-1", {"name": "vm1"}, "Ready"),
            ("test-vm-2", {"name": "vm2"}, "Already Deployed"),
        ]
        
        def test_callback(cr_name, cr_data):
            print(f"✅ SUCCESS! Enter key worked - selected: {cr_name}")
            print(f"   Data: {cr_data}")
            tui.exit_app()
        
        # Set up a timer to show the popup after TUI starts
        def show_popup():
            time.sleep(1)  # Wait for TUI to start
            print("📋 Showing test popup...")
            tui.show_cr_selection_popup("Test Enter Key", test_cr_options, test_callback)
        
        popup_thread = threading.Thread(target=show_popup)
        popup_thread.daemon = True
        popup_thread.start()
        
        print("🚀 Starting TUI... Press Enter on a popup item to test")
        print("   Instructions:")
        print("   1. Wait for popup to appear")
        print("   2. Use ↑↓ to navigate")
        print("   3. Press Enter to select")
        print("   4. Success message should appear")
        print()
        
        # Run the TUI
        tui.run()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_enter_key()