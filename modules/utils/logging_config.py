"""
Logging configuration module for the Windows Services Management System
"""

import logging
import queue

# Global log queue for TUI
log_queue = queue.Queue()

class TUILogHandler(logging.Handler):
    """Custom log handler that sends logs to the TUI"""
    def emit(self, record):
        try:
            msg = self.format(record)
            # Clean up verbose prefixes for better TUI display
            if ':' in msg:
                # Remove logger name prefix (everything before the first colon)
                msg = msg.split(':', 1)[1].strip()
            log_queue.put(msg)
        except Exception:
            pass

def setup_logging():
    """Set up the logging system for the application
    Only add console StreamHandler if running with --operator-only (operator mode).
    In TUI mode (default), only the TUI handler is active.
    """
    import sys
    # Remove all existing handlers first
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Set up our custom TUI handler
    tui_handler = TUILogHandler()
    tui_handler.setFormatter(logging.Formatter('%(name)s: %(message)s'))

    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(tui_handler)

    # Only add console handler if running as operator only
    if '--operator-only' in sys.argv:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s'))
        logging.root.addHandler(console_handler)

    # Suppress overly verbose loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('kubernetes').setLevel(logging.WARNING)

    # Get logger for this module
    logger = logging.getLogger(__name__)
    logger.info("Logging system initialized")
