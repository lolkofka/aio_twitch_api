import json
import logging
import time
import asyncio
import websockets
from websockets.exceptions import ConnectionClosed

from TwitchChannelPointsMiner.utils import create_nonce

logger = logging.getLogger(__name__)


class TwitchWebSocket:
    def __init__(self, index, parent_pool, url):
        self.index = index
        self.url = url
        self.parent_pool = parent_pool
        self.is_closed = False
        self.is_opened = False
        
        self.is_reconnecting = False
        self.forced_close = False
        
        # Custom attribute
        self.topics = []
        self.pending_topics = []
        
        self.twitch = parent_pool.twitch
        self.streamers = parent_pool.streamers
        self.events_predictions = parent_pool.events_predictions
        
        self.last_message_timestamp = None
        self.last_message_type_channel = None
        
        self.last_pong = time.time()
        self.last_ping = time.time()
    
    async def connect(self):
        while not self.forced_close:
            try:
                async with websockets.connect(self.uri) as websocket:
                    self.websocket = websocket
                    self.is_opened = True
                    self.is_closed = False
                    await self.handle_messages()
            except (ConnectionClosed, OSError) as e:
                self.is_closed = True
                logger.warning(f"#{self.index} - Connection closed with error: {e}")
                await asyncio.sleep(5)  # Reconnect delay
    
    async def handle_messages(self):
        try:
            async for message in self.websocket:
                await self.on_message(message)
        except ConnectionClosed:
            self.is_closed = True
    
    async def on_message(self, message):
        data = json.loads(message)
        logger.debug(f"#{self.index} - Received: {data}")
        # Обработка сообщения
    
    async def close(self):
        self.forced_close = True
        await self.websocket.close()
    
    async def listen(self, topic, auth_token=None):
        data = {"topics": [str(topic)]}
        if topic.is_user_topic() and auth_token is not None:
            data["auth_token"] = auth_token
        nonce = create_nonce()
        await self.send({"type": "LISTEN", "nonce": nonce, "data": data})
    
    async def ping(self):
        await self.send({"type": "PING"})
        self.last_ping = time.time()
    
    async def send(self, request):
        try:
            request_str = json.dumps(request, separators=(",", ":"))
            logger.debug(f"#{self.index} - Send: {request_str}")
            await self.websocket.send(request_str)
        except ConnectionClosed:
            self.is_closed = True
    
    def elapsed_last_pong(self):
        return (time.time() - self.last_pong) // 60
    
    def elapsed_last_ping(self):
        return (time.time() - self.last_ping) // 60
