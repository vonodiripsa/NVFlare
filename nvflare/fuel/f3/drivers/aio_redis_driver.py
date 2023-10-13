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
import asyncio
import logging
from concurrent.futures import CancelledError
import threading
from typing import Any, Dict, List
from asyncio import CancelledError, IncompleteReadError
import uuid
import aioredis
import ssl
from nvflare.fuel.f3.connection import BytesAlike, Connection
from nvflare.fuel.f3.comm_error import CommError
from nvflare.fuel.f3.drivers.aio_context import AioContext
from nvflare.fuel.f3.drivers.base_driver import BaseDriver
from nvflare.fuel.f3.drivers.connector_info import ConnectorInfo, Mode
from nvflare.fuel.f3.drivers.driver_params import DriverCap, DriverParams
from nvflare.fuel.f3.drivers.net_utils import get_ssl_context
from nvflare.fuel.f3.drivers.net_utils import get_tcp_urls
from nvflare.security.logging import secure_format_exception
from cryptography import x509
from cryptography.hazmat.backends import default_backend

log = logging.getLogger(__name__)


class RedisConnection(Connection):
    def __init__(self, redis: Any, server_queue: str, client_queue: str, aio_context: AioContext, connector: ConnectorInfo,
                 secure: bool):
        super().__init__(connector)
        # self.url = redis_url
        self.redis = redis
        self.server_queue = server_queue
        self.client_queue = client_queue
        self.aio_context = aio_context
        self.closing = False
        self.secure = secure
        self.conn_props = self._get_aio_properties()

    def get_conn_properties(self) -> dict:
        return self.conn_props

    def send_frame(self, frame: BytesAlike):
        self.aio_context.run_coro(self._async_send_frame(frame))

    def close(self):
        self.closing = True
        self.aio_context.run_coro(self.redis.close())

    def _get_aio_properties(self) -> dict:
        conn_props = {}
        conn_props[DriverParams.PEER_ADDR.value] = self._get_redis_connection_info(self.redis).get("remote_address", None)
        conn_props[DriverParams.LOCAL_ADDR.value] = self._get_redis_connection_info(self.redis).get("local_address", None)
        conn_props[DriverParams.PEER_CN.value] = self._get_redis_connection_info(self.redis).get("peer_cert_name", None)
        return conn_props

    def _get_redis_connection_info(self, redis):
        # context = ssl.create_default_context(cafile='/path/to/your/ssl/ca.pem')
        # Get peer certificate
        # ssl_object = redis.connection._transport.get_extra_info('ssl_object')
        peer_common_name = "Unknown"
        #
        # if ssl_object:
        #     peer_cert = ssl_object.getpeercert(binary=True)
        #     peer_cert = x509.load_der_x509_certificate(peer_cert, default_backend())
        #     peer_subject = peer_cert.subject
        #     peer_common_name = next(
        #         (
        #             attr.value
        #             for attr in peer_subject
        #             if attr.oid == x509.oid.NameOID.COMMON_NAME
        #         ),
        #         None
        #     )
        # else:
        #     peer_common_name = "Unknown"
        # Get local address and remote address
        # local_address = redis.connection._transport.get_extra_info('sockname')
        # remote_address = redis.connection._transport.get_extra_info('peername')
        local_address = "local_addr"
        remote_address = "remote_addr"
        # redis.close()
        # await redis.wait_closed()
        return {
            'local_address': local_address,
            'remote_address': remote_address,
            'peer_common_name': peer_common_name,
            'peer_cert_name': peer_common_name
        }

    async def read_loop(self):
        try:
            while not self.closing:
                frame = await self._async_read_frame()
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
            await self.redis.rpush(self.client_queue, frame)
        except Exception as ex:
            if not self.closing:
                log.error(f"Error sending frame for connection {self}: {secure_format_exception(ex)}")

    async def _async_read_frame(self):
        # ssl_context = ssl.create_default_context(cafile='/path/to/your/ssl/ca.pem')
        # redis = await aioredis.create_redis(
        #     "rediss://your.redis.host:6380",
        #     encoding="utf-8",
        #     ssl=self.ssl_context
        # )
        # value = await self.redis.rpop(self.conn_name, 1)
        while not self.closing:
            value = await self.redis.blpop(self.server_queue, timeout=1)
            if value:
                _, second = value
                if second:
                    return second


class AioRedisDriver(BaseDriver):
    # ssl_context = ssl.create_default_context(cafile='/path/to/your/ssl/ca.pem')
    def __init__(self):
        super().__init__()
        self.aio_ctx = AioContext.get_global_context()
        self.ssl_context = None
        self.stopping = False

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
        self.redis.close()
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
        params = self.connector.params
        host = params.get(DriverParams.HOST.value)
        port = params.get(DriverParams.PORT.value)
        channel = "channel_" + params.get("channel")
        if mode == Mode.ACTIVE:  # client
            coroutine = self._connect(host, port, channel)
        else:  # server
            coroutine = self._listen(host, port, channel)
        await coroutine

    async def _connect(self, host, port, channel):
        self.ssl_context = get_ssl_context(self.connector.params, ssl_server=False)
        conn_name = str(uuid.uuid4())
        
        redis = await aioredis.from_url(
                    f"redis://{host}:{port}",
                )

        # redis = await aioredis.create_redis(
        #             (host, port),
        #             ssl=self.ssl_context
        #             )
        
        # redis = await aioredis.from_url(
        #             f"redis://{host}:{port}",
        #             # encoding="utf-8",
        #             # ssl=self.ssl_context,
        #             # db=0
        #         )
        
        await redis.rpush(channel, conn_name)
        server_queue = conn_name + "_in"
        client_queue = conn_name + "_out"
        await self._create_connection(server_queue, client_queue, host, port)
        redis.close()

    async def _listen(self, host, port, channel):
        self.ssl_context = get_ssl_context(self.connector.params, ssl_server=True)
        
        # redis = await aioredis.create_redis(
        #             (host, port),
        #             ssl=self.ssl_context
        # )
        
        redis = await aioredis.from_url(
                    f"redis://{host}:{port}",
                )        
        
        # redis = await aioredis.create_redis(
        #             (host, port),
        #             ssl=self.ssl_context
        #             )        
        
        # redis = await aioredis.from_url(
        #     f"rediss://{host}:{port}"
        #     # encoding="utf-8",
        #     # ssl=self.ssl_context,
        #     # db=0
        # )
        while not self.stopping:
            value = await redis.blpop(channel, timeout=1)
            if not value:
                continue
            _, conn_name = value

            conn_name = str(conn_name, "UTF-8")
            # The listener side should reverse in/out
            server_queue = conn_name + "_out"
            client_queue = conn_name + "_in"
            await self._create_connection(server_queue, client_queue, host, port)

    async def _create_connection(self, server_queue, client_queue, host, port):
        
        redis = await aioredis.create_redis(
                    (host, port),
                    ssl=self.ssl_context
        )
        
        # redis = await aioredis.create_redis(
        #     (host, port),
        #     ssl=self.ssl_context
        #     )
        
        
        # redis = await aioredis.from_url(
        #     f"rediss://{host}:{port}"
        #     # encoding="utf-8",
        #     # ssl=self.ssl_context,
        #     # db=0
        # )
        sec = self.ssl_context is not None
        conn = RedisConnection(redis, server_queue, client_queue, self.aio_ctx, self.connector, sec)
        self.add_connection(conn)
        await conn.read_loop()
        self.close_connection(conn)
  