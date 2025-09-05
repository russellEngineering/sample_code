import logging
import logging.config
import time


class RateLimitFilter(logging.Filter):
    def __init__(self, rate_limit_seconds):
        super().__init__()
        self.rate_limit_seconds = rate_limit_seconds
        self.last_logged = 0

    def filter(self, record):
        current_time = time.time()
        if current_time - self.last_logged >= self.rate_limit_seconds:
            self.last_logged = current_time
            return True
        return False


def setup_logging(default_level=logging.INFO):
    """
    Setup logging configuration
    """
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,

        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            },
        },

        'handlers': {
            'console': {
                'level': default_level,
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
            },
            'file': {
                'level': default_level,
                'class': 'logging.FileHandler',
                'filename': 'app.log',
                'formatter': 'standard',
            },
        },

        'loggers': {
            '': {
                'handlers': ['console', 'file'],
                'level': default_level,
                'propagate': True,
            },
        }
    }

    logging.config.dictConfig(logging_config)
