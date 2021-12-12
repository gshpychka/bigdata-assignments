import time
from abc import abstractmethod
import websockets.client
from google.cloud import pubsub_v1

import os
import logging
import typing
import json
from websockets.typing import Data
import asyncio


class WebsocketStreamerBase:
    def __init__(
        self,
        url: str,
        logger: logging.Logger,
        connection_kwargs: typing.Dict[str, typing.Any] = {},
    ) -> None:
        self.url = url
        self.connection_kwargs = connection_kwargs
        self.logger = logger

    async def streamer(self):
        time_between_reconnects_s = 5
        while True:
            self.logger.info(f"Connecting to {self.url}")
            async with websockets.client.connect(
                self.url, **self.connection_kwargs
            ) as connection:
                self.logger.info(f"Connected to {self.url}")
                await self.initialize(connection)
                await self.subscribe(connection)
                while connection.open:
                    data = await connection.recv()
                    self.logger.debug(f"Response: {data}")
                    await self.handle(data)

            self.logger.warning(
                f"Connection closed, reconnecting in {time_between_reconnects_s}s..."
            )
            await asyncio.sleep(time_between_reconnects_s)

    async def initialize(
        self, connection: websockets.client.WebSocketClientProtocol
    ) -> None:
        pass

    @abstractmethod
    async def subscribe(
        self, connection: websockets.client.WebSocketClientProtocol
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def handle(self, data: Data) -> None:
        raise NotImplementedError


class FinancialDataStreamer(WebsocketStreamerBase):
    def __init__(
        self,
        instrument_names: typing.Sequence[str],
        logger: logging.Logger,
        pubsub_topic_path: str,
    ):

        url = "wss://www.deribit.com/ws/api/v2"

        super().__init__(
            url=url,
            logger=logger
            # connection_kwargs=dict(compression=None),
        )
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = pubsub_topic_path
        self.instrument_names = instrument_names

    @staticmethod
    def get_subscription_message(channels) -> str:
        message = {
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": 420,
            "params": {"channels": channels},
        }
        return json.dumps(message)

    async def subscribe(
        self, connection: websockets.client.WebSocketClientProtocol
    ) -> None:
        channel_name_template = "ticker.{}.raw"
        channels = [
            channel_name_template.format(instrument_name)
            for instrument_name in self.instrument_names
        ]
        message = self.get_subscription_message(channels=channels)
        await connection.send(message)
        pass

    async def handle(self, data: Data) -> None:
        encoded = str(data).encode("utf-8")

        future = self.publisher.publish(self.topic_path, encoded)
        self.logger.debug(future.result())

    def start(self):
        asyncio.run(self.streamer())


def setup_logging(level: typing.Union[int, str]) -> logging.Logger:
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    logging.basicConfig(level=level, handlers=[handler], force=True)
    return logger


if __name__ == "__main__":
    pubsub_topic_path = os.environ["PUBSUB_TOPIC_PATH"]
    log_level = int(os.getenv("LOG_LEVEL", logging.INFO))
    logger = setup_logging(level=log_level)
    print("hello world")
    print(pubsub_topic_path)

    instrument_names = ["BTC-PERPETUAL", "ETH-PERPETUAL"]

    streamer = FinancialDataStreamer(
        instrument_names=instrument_names,
        logger=logger,
        pubsub_topic_path=pubsub_topic_path,
    )

    streamer.start()
