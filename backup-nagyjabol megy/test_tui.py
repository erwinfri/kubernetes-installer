#!/usr/bin/env python3

import sys
import os
sys.path.insert(0, 'modules')

print("Testing TUI imports...")

try:
    import urwid
    print("✓ urwid imported")
except Exception as e:
    print("✗ urwid failed:", e)

try:
    import logging
    print("✓ logging imported")
except Exception as e:
    print("✗ logging failed:", e)

try:
    import queue
    print("✓ queue imported")
except Exception as e:
    print("✗ queue failed:", e)

try:
    from utils.logging_config import log_queue
    print("✓ utils.logging_config imported")
except Exception as e:
    print("✗ utils.logging_config failed:", e)

# Try importing the TUI class directly
try:
    exec(open('modules/tui_interface.py').read())
    print("✓ TUI file executed successfully")
    if 'WindowsServicesTUI' in locals():
        print("✓ WindowsServicesTUI class found")
    else:
        print("✗ WindowsServicesTUI class not found in locals")
        print("Available:", [k for k in locals().keys() if not k.startswith('_')])
except Exception as e:
    print("✗ TUI file execution failed:", e)
    import traceback
    traceback.print_exc()
