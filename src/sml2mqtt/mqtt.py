import asyncio
import logging
import time
import traceback
from asyncio import Future, create_task
from typing import Final, Optional
from sml2mqtt._log import log as _parent_logger

from asyncio_mqtt import Client, MqttCodeError, MqttError, Will

import sml2mqtt
from sml2mqtt.config import CONFIG

log = _parent_logger.getChild('mqtt')

MQTT: Optional[Client] = None
CONNECT: Optional[Future] = None

RECONNECT_AFTER: Final = 15
PUBS_FAILED_SINCE: Optional[float] = None


async def shutdown_mqtt():
    global CONNECT
    if CONNECT is not None:
        CONNECT.cancel()
        CONNECT = None
    if MQTT is not None:
        if MQTT._client.is_connected():
            create_task(MQTT.disconnect())


WAIT_BETWEEN_CONNECTS = 0


async def _connect():
    global MQTT, CONNECT, PUBS_FAILED_SINCE, WAIT_BETWEEN_CONNECTS

    # We don't publish anything if we just analyze the data from the reader
    if sml2mqtt._args.ARGS.analyze:
        return None

    while True:
        try:
            await asyncio.sleep(WAIT_BETWEEN_CONNECTS)
            
            will_topic = CONFIG.mqtt.topics.get_topic(CONFIG.mqtt.topics.last_will)

            MQTT = Client(
                hostname=CONFIG.mqtt.connection.host,
                port=CONFIG.mqtt.connection.port,
                username=CONFIG.mqtt.connection.user if CONFIG.mqtt.connection.user else None,
                password=CONFIG.mqtt.connection.password if CONFIG.mqtt.connection.password else None,
                will=Will(will_topic, payload='OFFLINE')
            )

            log.debug(f'Connecting to {CONFIG.mqtt.connection.host}:{CONFIG.mqtt.connection.port}')
            await MQTT.connect()
            log.debug('Success!')

            # signal that we are online
            await publish(will_topic, 'ONLINE')
            WAIT_BETWEEN_CONNECTS = 0
            PUBS_FAILED_SINCE = None
            break

        except MqttError as e:
            WAIT_BETWEEN_CONNECTS = min(180, max(1, WAIT_BETWEEN_CONNECTS) * 2)
            log.error(f'{e} ({e.__class__.__name__})')
        except Exception:
            WAIT_BETWEEN_CONNECTS = min(180, max(1, WAIT_BETWEEN_CONNECTS) * 2)
            for line in traceback.format_exc().splitlines():
                log.error(line)
            return None

    CONNECT = None


async def connect():
    global CONNECT
    if CONNECT is None:
        CONNECT = create_task(_connect())


async def publish(topic, value):
    global CONNECT, PUBS_FAILED_SINCE

    if MQTT is None or not MQTT._client.is_connected():
        await connect()
        return None

    # publish message
    try:
        await MQTT.publish(topic, value, qos=CONFIG.mqtt.publish.qos, retain=CONFIG.mqtt.publish.retain)
        PUBS_FAILED_SINCE = None
    except MqttError as e:
        log.error(f'{e} ({e.__class__.__name__})')
        if isinstance(e, MqttCodeError) and e.rc == 4 and PUBS_FAILED_SINCE is None:
            PUBS_FAILED_SINCE = time.time()
        
        if PUBS_FAILED_SINCE is not None:
            if time.time() - PUBS_FAILED_SINCE >= RECONNECT_AFTER:
                await connect()
