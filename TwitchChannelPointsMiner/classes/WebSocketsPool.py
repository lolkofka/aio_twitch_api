import json
import logging
import random
import time
import asyncio
from dateutil import parser

from TwitchChannelPointsMiner.classes.entities.EventPrediction import EventPrediction
from TwitchChannelPointsMiner.classes.entities.Message import Message
from TwitchChannelPointsMiner.classes.entities.Raid import Raid
from TwitchChannelPointsMiner.classes.Settings import Events, Settings
from TwitchChannelPointsMiner.classes.TwitchWebSocket import TwitchWebSocket
from TwitchChannelPointsMiner.constants import WEBSOCKET
from TwitchChannelPointsMiner.utils import (
    get_streamer_index,
    internet_connection_available,
)

logger = logging.getLogger(__name__)


class WebSocketsPool:
    __slots__ = ["ws", "twitch", "streamers", "events_predictions"]
    
    def __init__(self, twitch, streamers, events_predictions):
        self.ws = []
        self.twitch = twitch
        self.streamers = streamers
        self.events_predictions = events_predictions
    
    async def submit(self, topic):
        if not self.ws or len(self.ws[-1].topics) >= 50:
            self.ws.append(await self.__new(len(self.ws)))
            await self.__start(-1)
        
        await self.__submit(-1, topic)
    
    async def __submit(self, index, topic):
        if topic not in self.ws[index].topics:
            self.ws[index].topics.append(topic)
        
        if not self.ws[index].is_opened:
            self.ws[index].pending_topics.append(topic)
        else:
            await self.ws[index].listen(topic, self.twitch.twitch_login.get_auth_token())
    
    async def __new(self, index):
        ws = TwitchWebSocket(
            index=index,
            parent_pool=self,
            uri=WEBSOCKET
        )
        return ws
    
    async def __start(self, index):
        if Settings.disable_ssl_cert_verification:
            import ssl
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            logger.warn("SSL certificate verification is disabled! Be aware!")
        else:
            ssl_context = None
        
        asyncio.create_task(self.ws[index].connect(ssl_context=ssl_context))
    
    async def end(self):
        for ws in self.ws:
            ws.forced_close = True
            await ws.close()
    
    @staticmethod
    async def on_open(ws):
        ws.is_opened = True
        await ws.ping()
        
        for topic in ws.pending_topics:
            await ws.listen(topic, ws.twitch.twitch_login.get_auth_token())
        
        while not ws.is_closed:
            if not ws.is_reconnecting:
                await ws.ping()
                await asyncio.sleep(random.uniform(25, 30))
                
                if ws.elapsed_last_pong() > 5:
                    logger.info(f"#{ws.index} - The last PONG was received more than 5 minutes ago")
                    await WebSocketsPool.handle_reconnection(ws)
    
    @staticmethod
    async def on_error(ws, error):
        logger.error(f"#{ws.index} - WebSocket error: {error}")
    
    @staticmethod
    async def on_close(ws, close_status_code, close_reason):
        logger.info(f"#{ws.index} - WebSocket closed")
        await WebSocketsPool.handle_reconnection(ws)
    
    @staticmethod
    async def handle_reconnection(ws):
        if not ws.is_reconnecting:
            ws.is_closed = True
            ws.is_reconnecting = True
            
            if not ws.forced_close:
                logger.info(f"#{ws.index} - Reconnecting to Twitch PubSub server in ~60 seconds")
                await asyncio.sleep(30)
                
                while not internet_connection_available():
                    random_sleep = random.randint(1, 3)
                    logger.warning(f"#{ws.index} - No internet connection available! Retry after {random_sleep}m")
                    await asyncio.sleep(random_sleep * 60)
                
                parent_pool = ws.parent_pool
                new_ws = await parent_pool.__new(ws.index)
                parent_pool.ws[ws.index] = new_ws
                
                await parent_pool.__start(ws.index)
                await asyncio.sleep(30)
                
                for topic in ws.topics:
                    await parent_pool.__submit(ws.index, topic)
    
    @staticmethod
    async def on_message(ws, message):
        logger.debug(f"#{ws.index} - Received: {message.strip()}")
        response = json.loads(message)
        
        if response["type"] == "MESSAGE":
            message = Message(response["data"])
            
            if (
                    ws.last_message_type_channel is not None
                    and ws.last_message_timestamp is not None
                    and ws.last_message_timestamp == message.timestamp
                    and ws.last_message_type_channel == message.identifier
            ):
                return
            
            ws.last_message_timestamp = message.timestamp
            ws.last_message_type_channel = message.identifier
            
            streamer_index = get_streamer_index(ws.streamers, message.channel_id)
            if streamer_index != -1:
                try:
                    if message.topic == "community-points-user-v1":
                        if message.type in ["points-earned", "points-spent"]:
                            balance = message.data["balance"]["balance"]
                            ws.streamers[streamer_index].channel_points = balance
                            if Settings.enable_analytics:
                                ws.streamers[streamer_index].persistent_series(
                                    event_type=message.data["point_gain"]["reason_code"]
                                    if message.type == "points-earned"
                                    else "Spent"
                                )
                        
                        if message.type == "points-earned":
                            earned = message.data["point_gain"]["total_points"]
                            reason_code = message.data["point_gain"]["reason_code"]
                            
                            logger.info(
                                f"+{earned} → {ws.streamers[streamer_index]} - Reason: {reason_code}.",
                                extra={
                                    "emoji": ":rocket:",
                                    "event": Events.get(f"GAIN_FOR_{reason_code}"),
                                },
                            )
                            ws.streamers[streamer_index].update_history(reason_code, earned)
                            if Settings.enable_analytics:
                                ws.streamers[streamer_index].persistent_annotations(
                                    reason_code, f"+{earned} - {reason_code}"
                                )
                        elif message.type == "claim-available":
                            await ws.twitch.claim_bonus(
                                ws.streamers[streamer_index],
                                message.data["claim"]["id"],
                            )
                    
                    elif message.topic == "video-playback-by-id":
                        if message.type == "stream-up":
                            ws.streamers[streamer_index].stream_up = time.time()
                        elif message.type == "stream-down":
                            if ws.streamers[streamer_index].is_online:
                                ws.streamers[streamer_index].set_offline()
                        elif message.type == "viewcount":
                            if ws.streamers[streamer_index].stream_up_elapsed():
                                await ws.twitch.check_streamer_online(ws.streamers[streamer_index])
                    
                    elif message.topic == "raid":
                        if message.type == "raid_update_v2":
                            raid = Raid(
                                message.message["raid"]["id"],
                                message.message["raid"]["target_login"],
                            )
                            await ws.twitch.update_raid(ws.streamers[streamer_index], raid)
                    
                    elif message.topic == "community-moments-channel-v1":
                        if message.type == "active":
                            await ws.twitch.claim_moment(
                                ws.streamers[streamer_index], message.data["moment_id"]
                            )
                    
                    elif message.topic == "predictions-channel-v1":
                        event_dict = message.data["event"]
                        event_id = event_dict["id"]
                        event_status = event_dict["status"]
                        
                        current_tmsp = parser.parse(message.timestamp)
                        
                        if (
                                message.type == "event-created"
                                and event_id not in ws.events_predictions
                        ):
                            if event_status == "ACTIVE":
                                prediction_window_seconds = float(event_dict["prediction_window_seconds"])
                                prediction_window_seconds = ws.streamers[streamer_index].get_prediction_window(
                                    prediction_window_seconds)
                                event = EventPrediction(
                                    ws.streamers[streamer_index],
                                    event_id,
                                    event_dict["title"],
                                    parser.parse(event_dict["created_at"]),
                                    prediction_window_seconds,
                                    event_status,
                                    event_dict["outcomes"],
                                )
                                if (
                                        ws.streamers[streamer_index].is_online
                                        and event.closing_bet_after(current_tmsp) > 0
                                ):
                                    streamer = ws.streamers[streamer_index]
                                    bet_settings = streamer.settings.bet
                                    if (
                                            bet_settings.minimum_points is None
                                            or streamer.channel_points > bet_settings.minimum_points
                                    ):
                                        ws.events_predictions[event_id] = event
                                        start_after = event.closing_bet_after(current_tmsp)
                                        
                                        asyncio.create_task(WebSocketsPool.start_prediction_task(ws.twitch,
                                                                                                 ws.events_predictions[
                                                                                                     event_id],
                                                                                                 start_after))
                                        
                                        logger.info(
                                            f"Place the bet after: {start_after}s for: {ws.events_predictions[event_id]}",
                                            extra={
                                                "emoji": ":alarm_clock:",
                                                "event": Events.BET_START,
                                            },
                                        )
                                    else:
                                        logger.info(
                                            f"{streamer} have only {streamer.channel_points} channel points and the minimum for bet is: {bet_settings.minimum_points}",
                                            extra={
                                                "emoji": ":pushpin:",
                                                "event": Events.BET_FILTERS,
                                            },
                                        )
                        
                        elif (
                                message.type == "event-updated"
                                and event_id in ws.events_predictions
                        ):
                            ws.events_predictions[event_id].status = event_status
                            if (
                                    not ws.events_predictions[event_id].bet_placed
                                    and not ws.events_predictions[event_id].bet.decision
                                    and event_status == "LOCKED"
                            ):
                                ws.events_predictions[event_id].bet.decision = {
                                    "emoji": ":x:",
                                    "choice": None,
                                    "title": "Locked",
                                    "color": "#5c575f",
                                }
                                ws.events_predictions[event_id].closing_bet_at = parser.parse(event_dict["locked_at"])
                                
                                if Settings.enable_analytics:
                                    ws.streamers[streamer_index].persistent_annotations(
                                        "PREDICTION_LOCKED", f"{ws.events_predictions[event_id].title}"
                                    )
                        
                        elif message.type == "event-concluded":
                            event_id = message.data["prediction"]["event_id"]
                            if event_id in ws.events_predictions:
                                event_prediction = ws.events_predictions[event_id]
                                if message.type == "prediction-result" and event_prediction.bet_confirmed:
                                    points = event_prediction.parse_result(message.data["prediction"]["result"])
                                    
                                    decision = event_prediction.bet.get_decision()
                                    choice = event_prediction.bet.decision["choice"]
                                    
                                    logger.info(
                                        (
                                            f"{event_prediction} - Decision: {choice}: {decision['title']} "
                                            f"({decision['color']}) - Result: {event_prediction.result['string']}"
                                        ),
                                        extra={
                                            "emoji": ":bar_chart:",
                                            "event": Events.get(f"BET_{event_prediction.result['type']}"),
                                        },
                                    )
                                    
                                    ws.streamers[streamer_index].update_history("PREDICTION", points["gained"])
                                    
                                    if event_prediction.result["type"] == "REFUND":
                                        ws.streamers[streamer_index].update_history("REFUND", -points["placed"],
                                                                                    counter=-1)
                                    elif event_prediction.result["type"] == "WIN":
                                        ws.streamers[streamer_index].update_history("PREDICTION", -points["won"],
                                                                                    counter=-1)
                                    
                                    if event_prediction.result["type"] and Settings.enable_analytics:
                                        ws.streamers[streamer_index].persistent_annotations(
                                            event_prediction.result["type"], f"{ws.events_predictions[event_id].title}")
                                
                                elif message.type == "prediction-made":
                                    event_prediction.bet_confirmed = True
                                    if Settings.enable_analytics:
                                        ws.streamers[streamer_index].persistent_annotations("PREDICTION_MADE",
                                                                                            f"Decision: {event_prediction.bet.decision['choice']} - {event_prediction.title}")
                    
                except Exception:
                    logger.error(f"Exception raised for topic: {message.topic} and message: {message}", exc_info=True)
    
        elif response["type"] == "RESPONSE" and response.get("error"):
            error_message = response.get("error", "")
            logger.error(f"Error while trying to listen for a topic: {error_message}")
    
            if "ERR_BADAUTH" in error_message:
                username = ws.twitch.twitch_login.username
                logger.error(
                    f"Received the ERR_BADAUTH error, most likely you have an outdated cookie file \"cookies\\{username}.pkl\". Delete this file and try again.")
    
        elif response["type"] == "RECONNECT":
            logger.info(f"#{ws.index} - Reconnection required")
            await WebSocketsPool.handle_reconnection(ws)
    
        elif response["type"] == "PONG":
            ws.last_pong = time.time()


    @staticmethod
    async def start_prediction_task(twitch, event_prediction, start_after):
        await asyncio.sleep(start_after)
        await twitch.place_bet(event_prediction)