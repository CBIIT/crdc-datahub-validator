import requests
import os
from bento.common.utils import get_logger
from common.utils import get_exception_msg

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
            log.exception(f'Failed error messages setting scale-in protection: {response}')

    except Exception as e:
        log.exception(e)
        log.exception(f'Unexpected error occurred during setting scale-in protection setup: {get_exception_msg()}')
