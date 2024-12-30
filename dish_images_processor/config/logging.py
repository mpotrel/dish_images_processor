import logging
import sys

def configure_logging(log_level=logging.INFO):
    """
    Configure logging for the application

    Args:
        log_level (int, optional): Logging level. Defaults to logging.INFO.
    """
    # Create a formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Configure console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Optional: Add file logging
    try:
        file_handler = logging.FileHandler('dish_images_processor.log')
        file_handler.setFormatter(formatter)

        # Add handlers to root logger
        logging.getLogger().addHandler(console_handler)
        logging.getLogger().addHandler(file_handler)
    except PermissionError:
        print("Warning: Unable to create log file. Logging to console only.")
        logging.getLogger().addHandler(console_handler)

    # Optionally, reduce log levels for noisy libraries
    logging.getLogger('confluent_kafka').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

def get_logger(name):
    """
    Convenience method to get a configured logger

    Args:
        name (str): Name of the logger

    Returns:
        logging.Logger: Configured logger
    """
    return logging.getLogger(name)
