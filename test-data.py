#!/usr/bin/env python3

import os
import sys
import yaml
import urwid
from kubernetes import client, config
from kubernetes.config import load_kube_config
from kubernetes.client.rest import ApiException

def test_with_actual_data():
    """Test with actual VM data to see if that causes the error"""
    
    # Load actual VM entries like the real app
    manifest_dir = "/root/kubernetes-installer/manifest-controller"
    vm_entries = []
    
    try:
        if os.path.exists(manifest_dir):
            cr_files = [f for f in os.listdir(manifest_dir) if f.endswith('-cr.yaml')]
            for cr_file in cr_files:
                cr_path = os.path.join(manifest_dir, cr_file)
                try:
                    with open(cr_path, 'r') as f:
                        cr_data = yaml.safe_load(f)
                    
                    vm_name = cr_data.get('metadata', {}).get('name', 'unknown')
                    spec = cr_data.get('spec', {})
                    
                    vm_entries.append({
                        'label': f"{vm_name} ({'deployed' if spec else 'not_deployed'})",
                        'status': 'deployed' if spec else 'not_deployed',
                        'vm_name': vm_name,
                        'spec': spec
                    })
                except Exception as e:
                    vm_entries.append({
                        'label': f"Error loading {cr_file}: {str(e)[:50]}...",
                        'status': 'error',
                        'vm_name': cr_file,
                        'spec': {}
                    })
    except Exception as e:
        vm_entries = [{'label': f'Error: {e}', 'status': 'error', 'vm_name': 'error', 'spec': {}}]
    
    if not vm_entries:
        vm_entries = [{'label': 'No VMs found', 'status': 'error', 'vm_name': 'none', 'spec': {}}]
    
    print(f"Loaded {len(vm_entries)} VM entries")
    for i, entry in enumerate(vm_entries):
        print(f"  {i}: {entry['label']}")
    
    # Create simple text list (no buttons, no complex widgets)
    vm_texts = []
    for i, entry in enumerate(vm_entries):
        label = entry.get('label', f'VM {i+1}')
        vm_texts.append(urwid.Text(f"{i+1}. {label}"))
    
    # Simple pile layout
    vm_pile = urwid.Pile(vm_texts)
    vm_fill = urwid.Filler(vm_pile, valign='top')
    
    frame = urwid.Frame(
        body=vm_fill,
        header=urwid.Text("VM List Test", align='center'),
        footer=urwid.Text("Press Q to quit")
    )
    
    def exit_on_q(key):
        if key in ('q', 'Q'):
            raise urwid.ExitMainLoop()
    
    loop = urwid.MainLoop(frame, unhandled_input=exit_on_q)
    
    try:
        loop.run()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_with_actual_data()
