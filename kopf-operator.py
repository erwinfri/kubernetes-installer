#!/usr/bin/env python3
"""
Standalone Kopf Operator for Windows Services Management
"""
import os
import logging
import sys

# Add the modules directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'modules'))

from modules.kopf_handlers import setup_kopf_handlers
from modules.utils.logging_config import setup_logging
from modules.utils.k8s_client import load_kube_config

if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("=== Starting Standalone Kopf Operator ===")
    try:
        load_kube_config()
        setup_kopf_handlers()
        import kopf
        kopf.run(
            clusterwide=False,
            namespace=os.getenv('WATCH_NAMESPACE', 'default'),
            standalone=True,
        )
    except Exception as e:
        logger.error(f"Kopf operator error: {e}")
        raise
