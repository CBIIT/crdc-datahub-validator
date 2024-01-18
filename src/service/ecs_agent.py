import requests
import os
from src.bento.common.utils import get_logger
from src.common.utils import get_exception_msg

ecs_agent_uri = os.environ.get("ECS_AGENT_URI")
url = f"{ecs_agent_uri}/task-protection/v1/state" if ecs_agent_uri else ""


def set_scale_in_protection(status):
    if not url:
        return
    log = get_logger('ECS Scale In Protection Service')
    log.info(f'scale-in protection info. {url}')
    """
    Sets the scale-in protection status by making an HTTP request to the provided url.
    """
    try:
        response = requests.put(url, headers={'Content-Type': 'application/json'}, json={"ProtectionEnabled": status})
        if response.status_code != 200:
            log.exception(f'Failed to set scale-in protection. HTTP status code: {response.status_code}')

    except Exception as e:
        log.debug(e)
        log.exception(f'Unexpected error occurred during setting scale-in protection setup: {get_exception_msg()}')


def get_scale_in_protection():
    if not url:
        return
    log = get_logger('ECS Scale In Protection Service')
    """
    Get the scale-in protection status by making an HTTP request to the provided url.
    """
    try:
        response = requests.get(url)
        if response.status_code != 200:
            log.exception(f'Failed to get scale-in protection. HTTP status code: {response.status_code}')
        log.info(f'scale-in protection info. {response}')
    except Exception as e:
        log.debug(e)
        log.exception(f'Unexpected error occurred during getting scale-in protection: {get_exception_msg()}')
