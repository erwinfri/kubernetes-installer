#!/usr/bin/env python3

import sys
import os
sys.path.append('/root/kubernetes-installer')

# Test the file discovery and ordering in delete CR menu
folder = '/root/kubernetes-installer/manifest-controller'
files = os.listdir(folder)

def cr_filter(filename):
    return 'crd' not in filename.lower() and filename.endswith('.yaml')

cr_files = [f for f in files if cr_filter(f)]

print("=== ALL FILES IN MANIFEST-CONTROLLER ===")
for i, f in enumerate(files, 1):
    print(f"{i}. {f}")

print("\n=== CR FILES (after filtering) ===")
for i, f in enumerate(cr_files, 1):
    print(f"{i}. {f}")
    
print(f"\n=== FIRST CR FILE ===")
if cr_files:
    first_file = cr_files[0]
    print(f"First CR file: {first_file}")
    print(f"Full path: {os.path.join(folder, first_file)}")
    
    # Check if it contains redhatvm
    if 'redhatvm' in first_file.lower():
        print("✅ First file contains 'redhatvm'")
    else:
        print("❌ First file does NOT contain 'redhatvm'")
        
print(f"\n=== SECOND CR FILE ===")
if len(cr_files) > 1:
    second_file = cr_files[1]
    print(f"Second CR file: {second_file}")
    print(f"Full path: {os.path.join(folder, second_file)}")
    
    # Check if it contains redhatvm
    if 'redhatvm' in second_file.lower():
        print("✅ Second file contains 'redhatvm'")
    else:
        print("❌ Second file does NOT contain 'redhatvm'")