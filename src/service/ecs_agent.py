import requests

from src.bento.common.utils import get_logger
from src.common.utils import get_exception_msg


def set_scale_in_protection(status, url):
    log = get_logger('ECS Scale In Protection Service')
    """
    Sets the scale-in protection status by making an HTTP request to the provided url.
    """
    try:
        response = requests.post(url, json={'scaleInProtection': status})
        if response.status_code != 200:
            log.exception(f'Failed to set scale-in protection. HTTP status code: {response.status_code}')

    except Exception as e:
        log.debug(e)
        log.exception(f'Unexpected error occurred during scale-in protection setup: {get_exception_msg()}')