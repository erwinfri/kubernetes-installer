# Simple test
import sys
sys.path.insert(0, 'modules')

try:
    # Test individual imports
    print("Testing urwid...")
    import urwid
    print("✅ urwid OK")
    
    print("Testing logging...")
    import logging
    print("✅ logging OK")
    
    print("Testing datetime...")
    from datetime import datetime
    print("✅ datetime OK")
    
    # Test the TUI file directly
    print("Testing TUI file execution...")
    with open('modules/tui_interface.py', 'r') as f:
        code = f.read()
    
    print(f"File size: {len(code)} chars")
    
    # Execute in a clean namespace
    namespace = {}
    exec(code, namespace)
    
    print("✅ TUI file executed")
    print("Available classes:", [k for k, v in namespace.items() if isinstance(v, type)])
    
    if 'WindowsServicesTUI' in namespace:
        print("✅ WindowsServicesTUI found!")
        cls = namespace['WindowsServicesTUI']
        print(f"Class: {cls}")
    else:
        print("❌ WindowsServicesTUI not found")
        print("Namespace keys:", list(namespace.keys()))

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
