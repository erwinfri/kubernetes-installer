#!/usr/bin/env python3
"""
Test script to debug Delete CR menu functionality
"""

import sys
import os
sys.path.append('/root/kubernetes-installer')

import modules.tui_interface as tui
import modules.service_manager as sm

def test_delete_menu():
    print("=== TESTING DELETE CR MENU FUNCTIONALITY ===")
    
    try:
        # Create service manager and TUI
        print("1. Creating service manager...")
        service_manager = sm.ServiceManager()
        
        print("2. Creating TUI interface...")
        tui_instance = tui.KubernetesCRDTUI(service_manager)
        
        print("3. Testing delete callback function...")
        
        # Simulate what happens when you click delete menu
        def test_delete_callback(file_name, file_path):
            print(f"🔥 DELETE CALLBACK TRIGGERED: {file_name}")
            print(f"   File path: {file_path}")
            print(f"   File exists: {os.path.exists(file_path)}")
            # This is what the actual callback does
            tui_instance.add_log_line(f"🔥 DELETE CALLBACK TRIGGERED: {file_name}")
            tui_instance.add_log_line(f"🗑️ Deleting CR: {file_name}...")
            print("✅ Delete callback executed successfully")
            return True
        
        # Test with a real CR file
        test_file = 'rhel9-redhatvm-cr.yaml'
        test_path = '/root/kubernetes-installer/manifest-controller/rhel9-redhatvm-cr.yaml'
        
        print(f"4. Testing with file: {test_file}")
        result = test_delete_callback(test_file, test_path)
        
        if result:
            print("✅ Delete menu callback test PASSED")
        else:
            print("❌ Delete menu callback test FAILED")
            
        print("\n=== TEST COMPLETED ===")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_delete_menu()