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
import subprocess
import psutil

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
        logger.info("[OPERATOR] Starting Kopf operator thread...")
        print("[OPERATOR] Kopf operator thread starting...")
        
        # Set up Kopf handlers for all resource types
        setup_kopf_handlers()
        logger.info("[OPERATOR] Kopf handlers set up.")
        print("[OPERATOR] Kopf handlers set up.")
        
        # Run Kopf operator
        import kopf
        logger.info("[OPERATOR] Running kopf.run()...")
        print("[OPERATOR] Running kopf.run()...")
        kopf.run(
            clusterwide=False,
            namespace=os.getenv('WATCH_NAMESPACE', 'default'),
            standalone=True,
        )
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Kopf operator error: {e}")
        print(f"[OPERATOR] Kopf operator error: {e}")

def run_kopf_operator_thread():
    """Run the Kopf operator in a background thread for the TUI."""
    def _run():
        import kopf
        load_kube_config()
        setup_kopf_handlers()
        kopf.run(
            clusterwide=False,
            namespace=os.getenv('WATCH_NAMESPACE', 'default'),
            standalone=True,
        )
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t

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

        # If running as operator only, just run operator and log to console
        if '--operator-only' in sys.argv:
            run_kopf_operator()
            return

        # Start operator in background thread
        run_kopf_operator_thread()

        # Create TUI application
        tui_app = WindowsServicesTUI(service_manager)

        # Start TUI only (Kopf operator must be run as a separate process)
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

# In each handler, add a print and logger.info at the top:
def handle_windowsvm(body, meta, spec, status, namespace, **kwargs):
    logger.info("[OPERATOR] handle_windowsvm triggered!")
    print("[OPERATOR] handle_windowsvm triggered!")
    # ...existing code...
