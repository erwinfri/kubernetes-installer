#!/usr/bin/env python3
import os
import yaml

folder = '/root/kubernetes-installer/manifest-controller'
files = os.listdir(folder)
cr_files = [f for f in files if f.endswith('.yaml') and 'crd' not in f.lower()]

print('📁 CR files found:')
for cr_file in cr_files:
    print(f'  • {cr_file}')

print()
print('🔍 Analyzing CR files:')

service_categories = {}

for cr_file in cr_files:
    cr_path = os.path.join(folder, cr_file)
    try:
        with open(cr_path, 'r') as f:
            cr_content = yaml.safe_load(f)
        
        if cr_content:
            kind = cr_content.get('kind', 'Unknown')
            name = cr_content.get('metadata', {}).get('name', 'Unknown')
            print(f'  📝 {cr_file}: kind={kind}, name={name}')
            
            # Fixed categorization - check specific services first
            category = 'default'
            kind_lower = kind.lower()
            
            if 'mssql' in kind_lower or 'mssql' in cr_file.lower():
                category = 'mssql'
            elif 'otel' in kind_lower or 'otel' in cr_file.lower() or 'collector' in cr_file.lower():
                category = 'otel'
            elif 'oracle' in kind_lower or 'oracle' in cr_file.lower():
                category = 'oracle'
            # Only actual VM CRs (not services running on VMs)
            elif 'vm' in kind_lower and ('windowsvm' in kind_lower or 'redhatvm' in kind_lower):
                category = 'vm'
            
            if category not in service_categories:
                service_categories[category] = []
            service_categories[category].append(cr_file)
            
        else:
            print(f'  ❌ {cr_file}: Empty or invalid YAML')
    except Exception as e:
        print(f'  ⚠️ {cr_file}: Error - {e}')

print()
print('📊 Fixed Service Categories:')
for category, files in service_categories.items():
    print(f'  {category}: {files}')