#!/usr/bin/env python3
"""
Test script to verify Enter key functionality in CR selection popup.
"""

import sys
import logging
import time
from modules.tui_interface import WindowsServicesTUI
from modules.service_managers import ServiceManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def test_popup():
    """Test the CR selection popup with Enter key."""
    service_manager = ServiceManager()
    tui = WindowsServicesTUI(service_manager)
    
    # Mock some CR data for testing - format: (name, cr_data, status)
    test_crs = [
        ("test-vm-1", {'metadata': {'name': 'test-vm-1', 'namespace': 'default'}}, "Ready"),
        ("test-vm-2", {'metadata': {'name': 'test-vm-2', 'namespace': 'default'}}, "Pending"),
        ("test-vm-3", {'metadata': {'name': 'test-vm-3', 'namespace': 'default'}}, "Running")
    ]
    
    def test_callback(selected_cr):
        """Callback when CR is selected"""
        logger.info(f"✓ SUCCESS! Enter key worked! Selected: {selected_cr['metadata']['name']}")
        tui.loop.stop()
    
    logger.info("Testing CR selection popup...")
    
    # Create and show popup
    tui.show_cr_selection_popup(test_crs, "Test VMs", test_callback)
    
    logger.info("Popup created successfully!")
    logger.info("Try pressing:")
    logger.info("  - Number keys 1-3 to select VM")
    logger.info("  - Enter key to confirm selection")
    logger.info("  - ESC to cancel")
    
    # Start the main loop
    try:
        tui.loop.run()
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Error during test: {e}")

if __name__ == "__main__":
    test_popup()