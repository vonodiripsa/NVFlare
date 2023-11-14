# Copyright (c) 2023, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import logging
import time
from typing import Any, Dict, List
from asyncio import CancelledError, IncompleteReadError
import uuid
import aioredis
from nvflare.fuel.f3.connection import BytesAlike, Connection
from nvflare.fuel.f3.comm_error import CommError
from nvflare.fuel.f3.drivers.aio_context import AioContext
from nvflare.fuel.f3.drivers.base_driver import BaseDriver
from nvflare.fuel.f3.drivers.connector_info import ConnectorInfo, Mode
from nvflare.fuel.f3.drivers.driver_params import DriverCap, DriverParams
from nvflare.fuel.f3.drivers.net_utils import get_tcp_urls
from nvflare.fuel.f3.drivers.net_utils import parse_url
from nvflare.fuel.utils import fobs
from nvflare.security.logging import secure_format_exception

CONN_TIMEOUT = 10

log = logging.getLogger(__name__)


class RedisConnection(Connection):
    def __init__(self, redis: Any, read_queue: str, write_queue: str, aio_context: AioContext,
                 connector: ConnectorInfo):
        super().__init__(connector)
        self.redis = redis
        self.read_queue = read_queue
        self.write_queue = write_queue
        self.aio_context = aio_context
        self.closing = False
        self.conn_props = self._get_aio_properties()

    def get_conn_properties(self) -> dict:
        return self.conn_props

    def send_frame(self, frame: BytesAlike):
        self.aio_context.run_coro(self._async_send_frame(frame))

    def close(self):
        self.closing = True
        self.aio_context.run_coro(self.redis.close())

    def _get_aio_properties(self) -> dict:
        conn_props = {
            DriverParams.PEER_ADDR.value: "Unknown",
            DriverParams.LOCAL_ADDR.value: "Unknown",
            DriverParams.PEER_CN.value: None}
        return conn_props

    async def read_loop(self):
        try:
            while not self.closing:
                frame = await self._async_read_frame()
                if frame:
                    self.process_frame(frame)
        except IncompleteReadError:
            if log.isEnabledFor(logging.DEBUG):
                closer = "locally" if self.closing else "by peer"
                log.debug(f"Connection {self} is closed {closer}")
        except CancelledError as error:
            log.debug(f"Connection {self} is closed by peer: {error}")
        except Exception as ex:
            log.error(f"Read error for connection {self}: {secure_format_exception(ex)}")

    async def _async_send_frame(self, frame: BytesAlike):
        try:
            if not isinstance(frame, bytes):
                frame = bytes(frame)
            await self.redis.rpush(self.write_queue, frame)
        except Exception as ex:
            if not self.closing:
                log.error(f"Error sending frame for connection {self}: {secure_format_exception(ex)}")

    async def _async_read_frame(self):
        while not self.closing:
            value = await self.redis.blpop(self.read_queue, timeout=1)
            if value:
                _, second = value
                if second:
                    return second


class AioRedisDriver(BaseDriver):

    def __init__(self):
        super().__init__()
        self.aio_ctx = AioContext.get_global_context()
        self.ssl_context = None
        self.stopping = False
        self.futures = []

    @staticmethod
    def supported_transports() -> List[str]:
        return ["redis", "rediss"]

    @staticmethod
    def capabilities() -> Dict[str, Any]:
        return {DriverCap.HEARTBEAT.value: False, DriverCap.SUPPORT_SSL.value: True}

    # server
    def listen(self, connector: ConnectorInfo):
        self._run(connector, Mode.PASSIVE)
    # client

    def connect(self, connector: ConnectorInfo):
        self._run(connector, Mode.ACTIVE)

    def shutdown(self):
        self.stopping = True
        # self.redis.wait_closed()
        # self.close_all()
        # if self.stop_event:
        #     self.stop_event.set_result(None)

    @staticmethod
    def get_urls(scheme: str, resources: dict) -> (str, str):
        secure = resources.get(DriverParams.SECURE)
        if secure:
            scheme = "rediss"
        return get_tcp_urls(scheme, resources)

    def _run(self, connector: ConnectorInfo, mode: Mode):
        self.connector = connector
        if mode != self.connector.mode:
            raise CommError(CommError.ERROR, f"Connector mode doesn't match driver mode for {self.connector}")
        try:
            self.aio_ctx.run_coro(self._async_run(mode)).result()
        except CancelledError:
            log.debug(f"Connector {self.connector} is cancelled")

    async def _async_run(self, mode: Mode):
        
        var_name = "NVFLARE_REDIS_URL_" + self.connector.params.get("server_name", "None").replace("-", "_").upper()
        url = os.getenv(var_name)
        
        if url:       
            params = parse_url(url)  
        else:
            params = self.connector.params
            
        host = params.get(DriverParams.HOST.value)
        port = params.get(DriverParams.PORT.value)
        channel = "channel_" + params.get("channel", "nvflare")
        
        if mode == Mode.ACTIVE:  # client
            coroutine = self._connect(host, port, channel)
        else:  # server
            coroutine = self._listen(host, port, channel)
        await coroutine

    async def _connect(self, host, port, channel):

        conn_name = str(uuid.uuid4())
        redis = None
        try:
            redis = await aioredis.from_url(
                f"redis://{host}:{port}",
            )

            timestamp = time.time()
            await redis.rpush(channel, fobs.dumps((conn_name, timestamp)))

            # Wait till server picks up the connection before a connection is created
            while not self.stopping:
                if time.time() - timestamp > CONN_TIMEOUT:
                    raise CommError(CommError.ERROR, f"Connection {conn_name} timed out after {CONN_TIMEOUT} seconds")

                connections = await redis.lrange(channel, 0, -1)
                if conn_name in (fobs.loads(x)[0] for x in connections):
                    time.sleep(0.1)
                    continue
                else:
                    read_queue = conn_name + "_A"
                    write_queue = conn_name + "_B"
                    await self._create_connection(read_queue, write_queue, host, port)
                    break
        except Exception as ex:
            log.error(f"Redis error for connection {conn_name}: {ex}")
        finally:
            if redis:
                await redis.close()

    async def _listen(self, host, port, channel):

        redis = None
        conn_name = "None"
        try:
            redis = await aioredis.from_url(
                        f"redis://{host}:{port}",
                    )

            while not self.stopping:
                value = await redis.blpop(channel, timeout=1)
                if not value:
                    continue
                _, buf = value
                conn_name, timestamp = fobs.loads(buf)
                if time.time() - timestamp > CONN_TIMEOUT:
                    log.debug(f"Connection {conn_name} is too old, it has expired after {CONN_TIMEOUT} seconds")
                    continue

                # The listener side should reverse read/write queue
                read_queue = conn_name + "_B"
                write_queue = conn_name + "_A"
                coro = self._create_connection(read_queue, write_queue, host, port)
                loop = self.aio_ctx.get_event_loop()
                future = loop.create_task(coro)
                self.futures.append(future)

        except Exception as ex:
            log.error(f"Error listening for connection {conn_name}: {ex}")
        finally:
            if redis:
                await redis.close()

    async def _create_connection(self, read_queue, write_queue, host, port):

        try:
            redis = await aioredis.from_url(
                        f"redis://{host}:{port}",
                    )

            conn = RedisConnection(redis, read_queue, write_queue, self.aio_ctx, self.connector)
            self.add_connection(conn)
            await conn.read_loop()
            self.close_connection(conn)
        except Exception as ex:
            log.error("Connection {conn} closed due to error: {ex")
  