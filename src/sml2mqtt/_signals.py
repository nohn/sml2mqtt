import signal
import traceback
from asyncio import create_task, get_event_loop, sleep
from typing import Dict, Optional, Type, Union

import sml2mqtt.mqtt
from sml2mqtt._log import log
from sml2mqtt.errors import DeviceFailed, DeviceSetupFailed

return_code: Optional[int] = None


async def stop_loop():
    await sleep(0.1)
    get_event_loop().stop()


def shutdown_with_exception(e: Union[Exception, Type[Exception]]):
    global return_code

    ret_map: Dict[int, Type[Exception]] = {10: DeviceSetupFailed, 11: DeviceFailed}

    log_traceback = True

    # get return code based on the error
    for r, cls in ret_map.items():
        if isinstance(e, cls):
            return_code = r
            break

        if e is cls:
            log_traceback = False
            return_code = r
            break
    else:
        return_code = 1

    if log_traceback:
        for line in traceback.format_exc().splitlines():
            log.error(line)

    do_shutdown()


def get_ret_code() -> int:
    if return_code is None:
        log.warning('No return code set!')
        return 2

    return return_code


def shutdown_handler(sig, frame):
    global return_code
    return_code = 0

    print('Shutting down ...')
    log.info('Shutting down ...')

    do_shutdown()


def add_shutdown_handler():
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)


def do_shutdown():
    create_task(sml2mqtt.mqtt.shutdown_mqtt())
    create_task(stop_loop())
