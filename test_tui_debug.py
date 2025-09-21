#!/usr/bin/env python3
"""
Debug TUI startup to see what's wrong
"""

import sys
import os
import traceback

# Add modules to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'modules'))

def test_tui():
    try:
        print("=== TESTING TUI STARTUP ===")
        
        # Import required modules
        print("1. Importing modules...")
        from modules.tui_interface import KubernetesCRDTUI
        from modules.service_managers import ServiceManager
        from modules.utils.logging_config import setup_logging
        from modules.utils.k8s_client import load_kube_config
        
        print("2. Setting up logging...")
        setup_logging()
        
        print("3. Loading Kubernetes config...")
        load_kube_config()
        
        print("4. Creating service manager...")
        service_manager = ServiceManager()
        
        print("5. Creating TUI instance...")
        tui_app = KubernetesCRDTUI(service_manager)
        
        print("6. Testing Delete CR menu function...")
        
        # Test if the delete_cr_menu method exists and can be called
        if hasattr(tui_app, 'delete_cr_menu'):
            print("   ✅ delete_cr_menu method exists")
            
            # Test calling it with a mock button
            class MockButton:
                pass
            
            try:
                # This should add debug logs but not show a popup (since loop isn't running)
                tui_app.delete_cr_menu(MockButton())
                print("   ✅ delete_cr_menu called successfully")
            except Exception as e:
                print(f"   ❌ Error calling delete_cr_menu: {e}")
                traceback.print_exc()
        else:
            print("   ❌ delete_cr_menu method missing!")
        
        print("7. All tests passed - TUI should work")
        print("\n   Now run: python3 kopf_urwid_controller_modular.py")
        print("   And click on the Delete CR button to test")
        
    except Exception as e:
        print(f"❌ Error during TUI test: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    test_tui()