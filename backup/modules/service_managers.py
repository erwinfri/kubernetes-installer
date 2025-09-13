"""
Service Manager - Manages different types of Windows services
"""

import os
import yaml
import logging
from datetime import datetime
from kubernetes import client
from kubernetes.client.rest import ApiException

from .utils.k8s_client import get_k8s_client, get_vm_status

logger = logging.getLogger(__name__)

class ServiceManager:
    """Manages WindowsVM, MSSQLServer, and OTelCollector resources"""
    
    def __init__(self):
        self.manifest_dir = "/root/kubernetes-installer/manifest-controller"
        
        # Resource definitions
        self.resource_types = {
            'windowsvm': {
                'group': 'infra.example.com',
                'version': 'v1',
                'plural': 'windowsvms',
                'kind': 'WindowsVM'
            },
            'mssqlserver': {
                'group': 'infra.example.com',
                'version': 'v1',
                'plural': 'mssqlservers',
                'kind': 'MSSQLServer'
            },
            'otelcollector': {
                'group': 'infra.example.com',
                'version': 'v1',
                'plural': 'otelcollectors',
                'kind': 'OTelCollector'
            }
        }
    
    def _crd_exists(self, resource_def):
        """Check if a Custom Resource Definition exists"""
        try:
            k8s_api = client.ApiextensionsV1Api()
            crd_name = f"{resource_def['plural']}.{resource_def['group']}"
            k8s_api.read_custom_resource_definition(crd_name)
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            raise
        except Exception as e:
            logger.warning(f"Error checking CRD existence: {e}")
            return False
    
    def get_comprehensive_status(self):
        """Get comprehensive status of all services"""
        status_report = {
            'windowsvms': {
                'local_crs': {},
                'deployed_crs': {},
                'running_vms': {}
            },
            'mssqlservers': {
                'local_crs': {},
                'deployed_crs': {}
            },
            'otelcollectors': {
                'local_crs': {},
                'deployed_crs': {}
            },
            'scenarios': {},
            'summary': {}
        }
        
        try:
            # Get status for each resource type
            for service_type, resource_def in self.resource_types.items():
                self._get_service_status(service_type, resource_def, status_report)
            
            # Analyze scenarios for WindowsVMs
            self._analyze_vm_scenarios(status_report)
            
            # Generate summary
            self._generate_summary(status_report)
            
        except Exception as e:
            logger.error(f"Error getting comprehensive status: {e}")
        
        return status_report
    
    def _get_service_status(self, service_type, resource_def, status_report):
        """Get status for a specific service type"""
        try:
            # Check if CRD exists first
            if not self._crd_exists(resource_def):
                logger.info(f"CRD {resource_def['plural']}.{resource_def['group']} not found, skipping")
                return
                
            # 1. Scan local CR files
            if os.path.exists(self.manifest_dir):
                for file in os.listdir(self.manifest_dir):
                    if file.endswith('-cr.yaml') or file.endswith('cr.yaml'):
                        file_path = os.path.join(self.manifest_dir, file)
                        try:
                            with open(file_path, 'r') as f:
                                cr_data = yaml.safe_load(f)
                                if cr_data and cr_data.get('kind') == resource_def['kind']:
                                    name = cr_data['metadata']['name']
                                    
                                    local_cr_data = {
                                        'file': file,
                                        'namespace': cr_data['metadata'].get('namespace', 'default')
                                    }
                                    
                                    # Add service-specific data
                                    if service_type == 'windowsvm':
                                        local_cr_data.update({
                                            'vm_name': cr_data['spec'].get('vmName', name),
                                            'action': cr_data['spec'].get('action', 'unknown')
                                        })
                                    elif service_type == 'mssqlserver':
                                        local_cr_data.update({
                                            'target_vm': cr_data['spec']['targetVM']['vmName'],
                                            'version': cr_data['spec'].get('version', 'unknown'),
                                            'enabled': cr_data['spec'].get('enabled', True)
                                        })
                                    elif service_type == 'otelcollector':
                                        local_cr_data.update({
                                            'target_vm': cr_data['spec']['targetVM']['vmName'],
                                            'metrics_type': cr_data['spec'].get('metricsType', 'unknown'),
                                            'enabled': cr_data['spec'].get('enabled', True)
                                        })
                                
                                status_report[resource_def['plural']]['local_crs'][name] = local_cr_data
                        except Exception as e:
                            logger.warning(f"Failed to parse CR file {file}: {e}")
            
            # 2. Get deployed CRs
            try:
                k8s_api = get_k8s_client()
                deployed_crs = k8s_api.list_cluster_custom_object(
                    group=resource_def['group'],
                    version=resource_def['version'],
                    plural=resource_def['plural']
                )
                
                for cr in deployed_crs.get('items', []):
                    name = cr['metadata']['name']
                    # Store the full CR for uninstall operations, plus extracted data for UI
                    deployed_cr_data = {
                        'full_cr': cr,  # Store full CR for operations
                        'namespace': cr['metadata'].get('namespace', 'default'),
                        'status': cr.get('status', {})
                    }
                    
                    # Add service-specific data for UI display
                    if service_type == 'windowsvm':
                        deployed_cr_data.update({
                            'vm_name': cr['spec'].get('vmName', name),
                            'action': cr['spec'].get('action', 'unknown')
                        })
                    elif service_type == 'mssqlserver':
                        deployed_cr_data.update({
                            'target_vm': cr['spec']['targetVM']['vmName'],
                            'version': cr['spec'].get('version', 'unknown'),
                            'enabled': cr['spec'].get('enabled', True)
                        })
                    elif service_type == 'otelcollector':
                        deployed_cr_data.update({
                            'target_vm': cr['spec']['targetVM']['vmName'],
                            'metrics_type': cr['spec'].get('metricsType', 'unknown'),
                            'enabled': cr['spec'].get('enabled', True)
                        })
                    
                    status_report[resource_def['plural']]['deployed_crs'][name] = deployed_cr_data
                
            except Exception as e:
                logger.warning(f"Failed to get deployed {service_type} CRs: {e}")
        
            # 3. Get running VMs (only for windowsvm type)
            if service_type == 'windowsvm':
                self._get_running_vms_status(status_report)
            
        except Exception as e:
            logger.error(f"Error getting {service_type} status: {e}")
    
    def _get_running_vms_status(self, status_report):
        """Get status of running VMs from KubeVirt"""
        try:
            k8s_api = get_k8s_client()
            vms = k8s_api.list_namespaced_custom_object(
                group="kubevirt.io",
                version="v1",
                namespace="kubevirt",
                plural="virtualmachines"
            )
            
            for vm in vms.get('items', []):
                name = vm['metadata']['name']
                vm_status = vm.get('status', {})
                status_report['windowsvms']['running_vms'][name] = {
                    'ready': vm_status.get('ready', False),
                    'created': vm_status.get('created', False),
                    'printable_status': vm_status.get('printableStatus', 'Unknown'),
                    'conditions': vm_status.get('conditions', [])
                }
                
                # Get VMI status if exists
                try:
                    vmi = k8s_api.get_namespaced_custom_object(
                        group="kubevirt.io",
                        version="v1",
                        namespace="kubevirt",
                        plural="virtualmachineinstances",
                        name=name
                    )
                    status_report['windowsvms']['running_vms'][name]['vmi_phase'] = vmi.get('status', {}).get('phase', 'Unknown')
                    status_report['windowsvms']['running_vms'][name]['vmi_ready'] = vmi.get('status', {}).get('ready', False)
                except:
                    status_report['windowsvms']['running_vms'][name]['vmi_phase'] = 'NotCreated'
                    status_report['windowsvms']['running_vms'][name]['vmi_ready'] = False
                    
        except Exception as e:
            logger.warning(f"Failed to get running VMs: {e}")
    
    def _analyze_vm_scenarios(self, status_report):
        """Analyze scenarios for WindowsVMs"""
        vm_data = status_report['windowsvms']
        
        # Get all VM names
        all_vm_names = set()
        all_vm_names.update(cr['vm_name'] for cr in vm_data['local_crs'].values())
        all_vm_names.update(cr['vm_name'] for cr in vm_data['deployed_crs'].values())
        all_vm_names.update(vm_data['running_vms'].keys())
        
        for vm_name in all_vm_names:
            # Check local CR
            local_cr = None
            local_cr_action = None
            for cr_name, cr_data in vm_data['local_crs'].items():
                if cr_data['vm_name'] == vm_name:
                    local_cr = cr_name
                    local_cr_action = cr_data['action']
                    break
            
            # Check deployed CR
            deployed_cr = None
            deployed_cr_action = None
            for cr_name, cr_data in vm_data['deployed_crs'].items():
                if cr_data['vm_name'] == vm_name:
                    deployed_cr = cr_name
                    deployed_cr_action = cr_data['action']
                    break
            
            # Check running VM
            vm_running = vm_name in vm_data['running_vms']
            vm_status = vm_data['running_vms'].get(vm_name, {}).get('printable_status', 'NotFound')
            
            # Determine scenario
            scenario = self._determine_vm_scenario(local_cr, deployed_cr, vm_running, 
                                                 local_cr_action, deployed_cr_action, vm_status)
            
            status_report['scenarios'][vm_name] = {
                'scenario': scenario,
                'local_cr': local_cr,
                'local_cr_action': local_cr_action,
                'deployed_cr': deployed_cr,
                'deployed_cr_action': deployed_cr_action,
                'vm_running': vm_running,
                'vm_status': vm_status
            }
    
    def _determine_vm_scenario(self, local_cr, deployed_cr, vm_running, 
                             local_cr_action, deployed_cr_action, vm_status):
        """Determine the scenario for a VM"""
        if vm_running and deployed_cr:
            return f"Running & Managed: VM running with active CR (action: {deployed_cr_action})"
        elif vm_running and not deployed_cr:
            return "Running & Orphaned: VM running but no managing CR found"
        elif not vm_running and deployed_cr:
            return f"Managed but No Instance: CR exists (action: {deployed_cr_action}) but VM not running"
        elif not vm_running and local_cr:
            return f"Local CR Only: Local CR exists (action: {local_cr_action}) but not deployed"
        else:
            return "Unknown: Inconsistent state"
    
    def _generate_summary(self, status_report):
        """Generate summary statistics"""
        summary = {}
        
        for service_type in self.resource_types.keys():
            if service_type in status_report:
                service_data = status_report[service_type]
                summary[service_type] = {
                    'local_count': len(service_data.get('local_crs', {})),
                    'deployed_count': len(service_data.get('deployed_crs', {}))
                }
                
                if service_type == 'windowsvm':
                    summary[service_type]['running_count'] = len(service_data.get('running_vms', {}))
        
        status_report['summary'] = summary
    
    def get_local_crs_by_type(self, service_type):
        """Get local CRs for a specific service type"""
        resource_def = self.resource_types.get(service_type)
        if not resource_def:
            return []
        
        local_crs = []
        if os.path.exists(self.manifest_dir):
            for file in os.listdir(self.manifest_dir):
                if file.endswith('-cr.yaml') or file.endswith('cr.yaml'):
                    file_path = os.path.join(self.manifest_dir, file)
                    try:
                        with open(file_path, 'r') as f:
                            cr_data = yaml.safe_load(f)
                            if cr_data and cr_data.get('kind') == resource_def['kind']:
                                local_crs.append({
                                    'name': cr_data['metadata']['name'],
                                    'file': file,
                                    'data': cr_data
                                })
                    except Exception as e:
                        logger.warning(f"Failed to parse CR file {file}: {e}")
        
        return local_crs
