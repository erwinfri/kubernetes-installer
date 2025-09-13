#!/usr/bin/env python3
"""
Modular Windows Services Management System
Main entry point for the Kopf + urwid TUI controller
"""

import os
import sys
import logging
import threading
import time
import signal

# Add the modules directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'modules'))

# Import modular components
from modules.tui_interface import WindowsServicesTUI
from modules.kopf_handlers import setup_kopf_handlers
from modules.service_managers import ServiceManager
from modules.utils.logging_config import setup_logging
from modules.utils.k8s_client import load_kube_config

# Global TUI app instance
tui_app = None

def run_kopf_operator():
    """Run the Kopf operator in a separate thread"""
    try:
        load_kube_config()
        logger = logging.getLogger(__name__)
        logger.info("Starting Kopf operator thread...")
        
        # Set up Kopf handlers for all resource types
        setup_kopf_handlers()
        
        # Run Kopf operator
        import kopf
        kopf.run(
            clusterwide=False,
            namespace=os.getenv('WATCH_NAMESPACE', 'default'),
            standalone=True,
        )
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Kopf operator error: {e}")

def main():
    """Main entry point"""
    global tui_app
    
    # Set up logging system
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("=== Windows Services Management System Starting ===")
    
    try:
        # Load Kubernetes config
        load_kube_config()
        
        # Initialize service manager
        service_manager = ServiceManager()
        
        # Create TUI application
        tui_app = WindowsServicesTUI(service_manager)
        
        # Start Kopf operator in background thread
        kopf_thread = threading.Thread(target=run_kopf_operator, daemon=True)
        kopf_thread.start()
        
        # Wait a moment for Kopf to initialize
        time.sleep(1)
        
        # Start TUI
        logger.info("Starting TUI interface...")
        tui_app.run()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
    finally:
        logger.info("Shutting down Windows Services Management System")

if __name__ == "__main__":
    main()
