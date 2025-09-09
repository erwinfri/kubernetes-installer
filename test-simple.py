#!/usr/bin/env python3

import os
import sys
import yaml
import urwid
from kubernetes import client, config
from kubernetes.config import load_kube_config
from kubernetes.client.rest import ApiException

class VMApp:
    def __init__(self, vm_entries):
        self.vm_entries = vm_entries
        self.current_selection = 0
        
        # Create palette
        self.palette = [
            ('header', 'white', 'dark blue'),
            ('deployed', 'light green', 'black'),
            ('not_deployed', 'light red', 'black'),
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
        
        # Just simple text, no buttons, no complex layouts
        main_text = urwid.Text("VM Management Interface\n\nTest layout without columns")
        main_filler = urwid.Filler(main_text, valign='top')
        
        # Footer
        footer = urwid.Text("Press Q to quit")
        footer = urwid.AttrMap(footer, 'header')
        
        # Main frame
        self.main_frame = urwid.Frame(
            body=main_filler,
            header=header,
            footer=footer
        )
        
    def unhandled_input(self, key):
        if key in ('q', 'Q'):
            raise urwid.ExitMainLoop()
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
            import traceback
            print(f"Application error: {e}")
            traceback.print_exc()
            raise

def main():
    vm_entries = [
        {'label': 'Test VM 1', 'status': 'not_deployed', 'spec': {}},
        {'label': 'Test VM 2', 'status': 'deployed', 'spec': {}}
    ]
    
    app = VMApp(vm_entries)
    app.run()

if __name__ == "__main__":
    main()
