# --- Standard Libraries ---
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, timezone
import pytz # Handles timezones
import requests
from bs4 import BeautifulSoup
import logging
import sys
import os
import schedule
import traceback
import json
import random
import threading
import yaml      # For reading config.yaml
import asyncio   # Required for ctrader-open-api
from typing import Dict, Any, Optional, List, Union, Tuple

# --- Third-Party Libraries ---
import pandas_ta as ta
try:
    from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
    from telegram.constants import ParseMode
    from telegram.error import TelegramError, RetryAfter, NetworkError
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    print("WARNING: 'python-telegram-bot' not installed. Telegram features will be disabled.")
    # Define dummy classes/functions if needed elsewhere, though better to check TELEGRAM_AVAILABLE
    class Bot: pass
    class Update: pass
    class ContextTypes: pass
    class InlineKeyboardButton: pass
    class InlineKeyboardMarkup: pass


# --- cTrader Library ---
# *** USING OFFICIAL ctrader-open-api LIBRARY ***
try:
    from ctrader_open_api import Client, Auth, TcpProtocol, Protobuf, MessagesFactory
    from ctrader_open_api.enums import ProtoOATrendbarPeriod, ProtoOAOrderType, ProtoOATradeSide, ProtoOATimeInForce, ProtoErrorCode, ProtoPayloadType, ProtoExecutionType, ProtoChangeBonusType, ProtoChangeBalanceType, ProtoOrderStatus, ProtoPositionStatus, ProtoTradeSide
    CTRADER_API_AVAILABLE = True
except ImportError:
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("!!! ERROR: Could not import 'ctrader-open-api'.                            !!!")
    print("!!! Please install it: pip install ctrader-open-api                        !!!")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    CTRADER_API_AVAILABLE = False
    # Define dummy enums needed later if API failed to import (for syntax checking)
    class ProtoOATrendbarPeriod: M1=1; M5=2; M15=3; M30=4; H1=5; H4=6; D1=7
    class ProtoOAOrderType: MARKET=1; LIMIT=2; STOP=3
    class ProtoOATradeSide: BUY=1; SELL=2
    class ProtoOATimeInForce: IMMEDIATE_OR_CANCEL=3; FILL_OR_KILL=4; GTC=1
    class ProtoErrorCode: pass
    class ProtoPayloadType: pass
    class ProtoExecutionType: pass
    class ProtoChangeBonusType: pass
    class ProtoChangeBalanceType: pass
    class ProtoOrderStatus: pass
    class ProtoPositionStatus: pass
    class ProtoTradeSide: BUY=1; SELL=2 # Duplicate, but fine


# --- Load Configuration ---
CONFIG: Optional[Dict[str, Any]] = None
def load_config(config_path: str = 'config.yaml') -> bool:
    """Loads configuration from YAML file and applies environment variable overrides."""
    global CONFIG
    try:
        with open(config_path, 'r') as f:
            CONFIG = yaml.safe_load(f)
        if not CONFIG:
            print(f"ERROR: Configuration file '{config_path}' is empty or invalid.")
            return False

        # --- Apply Environment Variable Overrides (Crucial for Deployment) ---
        CONFIG['ctrader']['app_client_id'] = os.getenv('CTRADER_APP_CLIENT_ID', CONFIG.get('ctrader', {}).get('app_client_id'))
        CONFIG['ctrader']['app_secret'] = os.getenv('CTRADER_APP_SECRET', CONFIG.get('ctrader', {}).get('app_secret'))
        CONFIG['ctrader']['account_id'] = int(os.getenv('CTRADER_ACCOUNT_ID', CONFIG.get('ctrader', {}).get('account_id', 0)))
        # Access token is fetched dynamically now, so remove it from static config/env vars here
        # CONFIG['ctrader']['access_token'] = os.getenv('CTRADER_ACCESS_TOKEN', CONFIG.get('ctrader', {}).get('access_token'))

        CONFIG['fundamental_analysis']['newsapi_key'] = os.getenv('NEWSAPI_KEY', CONFIG.get('fundamental_analysis', {}).get('newsapi_key'))

        # Telegram Config - Ensure defaults if section exists
        if 'notifications' not in CONFIG: CONFIG['notifications'] = {}
        CONFIG['notifications']['telegram_bot_token'] = os.getenv('TELEGRAM_BOT_TOKEN', CONFIG['notifications'].get('telegram_bot_token'))
        CONFIG['notifications']['telegram_chat_id'] = os.getenv('TELEGRAM_CHAT_ID', CONFIG['notifications'].get('telegram_chat_id')) # Legacy, prefer admin_ids
        admin_ids_env = os.getenv('TELEGRAM_ADMIN_USER_IDS')
        if admin_ids_env:
            try:
                CONFIG['notifications']['admin_user_ids'] = [int(uid.strip()) for uid in admin_ids_env.split(',')]
            except ValueError:
                print(f"WARNING: Invalid TELEGRAM_ADMIN_USER_IDS env var format. Should be comma-separated integers.")
                CONFIG['notifications']['admin_user_ids'] = CONFIG['notifications'].get('admin_user_ids', [])
        else:
             CONFIG['notifications']['admin_user_ids'] = CONFIG['notifications'].get('admin_user_ids', [])

        # Ensure enable_auto_trading exists and is boolean
        enable_auto_env = os.getenv('ENABLE_AUTO_TRADING')
        if enable_auto_env is not None:
            CONFIG['execution']['enable_auto_trading'] = enable_auto_env.lower() == 'true'
        elif 'execution' in CONFIG and 'enable_auto_trading' in CONFIG['execution']:
             CONFIG['execution']['enable_auto_trading'] = bool(CONFIG['execution']['enable_auto_trading'])
        else:
             if 'execution' not in CONFIG: CONFIG['execution'] = {}
             CONFIG['execution']['enable_auto_trading'] = False # Default to False if not set

        # Data directory
        if 'logging' not in CONFIG: CONFIG['logging'] = {}
        CONFIG['logging']['data_dir'] = os.getenv('BOT_DATA_DIR', CONFIG['logging'].get('data_dir', 'trading_bot_data_ctrader'))

        # Convert numeric/boolean types safely
        def get_typed_config(section, key, type_func, default):
            if section not in CONFIG: CONFIG[section] = {}
            val = CONFIG[section].get(key, default)
            try: return type_func(val)
            except (ValueError, TypeError): return default

        CONFIG['ctrader']['port_demo'] = get_typed_config('ctrader', 'port_demo', int, 5035)
        CONFIG['ctrader']['port_live'] = get_typed_config('ctrader', 'port_live', int, 5035)
        CONFIG['trading']['risk_per_trade_percent'] = get_typed_config('trading', 'risk_per_trade_percent', float, 1.0)
        CONFIG['trading']['min_trade_lots'] = get_typed_config('trading', 'min_trade_lots', float, 0.01)
        CONFIG['trading']['max_trade_lots'] = get_typed_config('trading', 'max_trade_lots', float, 10.0) #Sensible default max
        CONFIG['trading']['max_open_trades_per_symbol'] = get_typed_config('trading', 'max_open_trades_per_symbol', int, 1)
        CONFIG['trading']['tp_levels'] = get_typed_config('trading', 'tp_levels', int, 1)
        CONFIG['trading']['min_rr_ratio'] = get_typed_config('trading', 'min_rr_ratio', float, 1.5)

        # Ensure symbol is always uppercase
        CONFIG['trading']['symbol'] = get_typed_config('trading', 'symbol', str, "XAUUSD").upper()

        # Ensure timezone is valid
        try:
            pytz.timezone(CONFIG.get('timezone', {}).get('local_tz', 'UTC'))
        except Exception as e:
            print(f"WARNING: Invalid timezone '{CONFIG.get('timezone', {}).get('local_tz')}' in config, defaulting to UTC. Error: {e}")
            if 'timezone' not in CONFIG: CONFIG['timezone'] = {}
            CONFIG['timezone']['local_tz'] = 'UTC'

        return True
    except FileNotFoundError:
        print(f"ERROR: Configuration file '{config_path}' not found.")
        return False
    except Exception as e:
        print(f"ERROR: Failed to load or parse configuration '{config_path}': {e}")
        traceback.print_exc()
        return False

if not load_config():
    sys.exit(1) # Exit if config fails to load

# --- Setup Logger ---
LOG_LEVEL_STR = CONFIG.get('logging', {}).get('log_level', 'INFO').upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_STR, logging.INFO)
DATA_DIR = CONFIG.get('logging', {}).get('data_dir', 'trading_bot_data_ctrader')
LOG_FILE = os.path.join(DATA_DIR, CONFIG.get('logging', {}).get('log_file', 'ctrader_bot.log'))
PERFORMANCE_LOG_FILE = os.path.join(DATA_DIR, CONFIG.get('logging', {}).get('performance_log_file', 'performance.csv'))
STATE_FILE = os.path.join(DATA_DIR, CONFIG.get('logging', {}).get('state_file', 'bot_state.json'))

def setup_logger() -> logging.Logger:
    """Sets up the global logger."""
    if not os.path.exists(DATA_DIR):
        try:
            os.makedirs(DATA_DIR)
        except OSError as e:
            print(f"ERROR: Could not create data directory {DATA_DIR}: {e}")
            sys.exit(1)

    log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s')
    logger = logging.getLogger("CTraderBot")
    logger.setLevel(LOG_LEVEL)

    # Prevent duplicate handlers if logger is reconfigured
    if logger.hasHandlers():
        logger.handlers.clear()

    # File Handler
    try:
        file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
        file_handler.setFormatter(log_format)
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"ERROR: Could not set up file logging to {LOG_FILE}: {e}")

    # Console Handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_format)
    logger.addHandler(stream_handler)

    # Reduce verbosity of libraries
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.INFO) # Keep INFO for Telegram bot library
    logging.getLogger("ctrader_open_api").setLevel(logging.INFO) # Set cTrader library level

    return logger

logger = setup_logger()
logger.info(f"--- Bot Version: {CONFIG.get('bot_version', 'N/A')} ---")
logger.info(f"--- Configuration loaded from config.yaml ---")
logger.info(f"--- Auto Trading Enabled: {CONFIG['execution']['enable_auto_trading']} ---")
logger.info(f"--- Data Directory: {DATA_DIR} ---")

# --- Timezone Setup ---
try:
    LOCAL_TZ_STR = CONFIG.get('timezone', {}).get('local_tz', 'UTC')
    LOCAL_TZ = pytz.timezone(LOCAL_TZ_STR)
    logger.info(f"--- Local Timezone: {LOCAL_TZ_STR} ---")
except Exception as e:
    logger.warning(f"Invalid timezone '{LOCAL_TZ_STR}' in config, defaulting to UTC. Error: {e}")
    LOCAL_TZ = pytz.timezone("UTC")

# --- NewsAPI & Telegram Setup ---
NEWSAPI_ENABLED = CONFIG.get('fundamental_analysis', {}).get('enable', False) and CONFIG.get('fundamental_analysis', {}).get('newsapi_key')
if NEWSAPI_ENABLED:
    try:
        from newsapi import NewsApiClient
    except ImportError:
        NEWSAPI_ENABLED = False
        logger.warning("NewsAPI enabled in config, but 'newsapi-python' not installed.")

TELEGRAM_ENABLED = CONFIG.get('notifications', {}).get('enable_telegram', False) and CONFIG.get('notifications', {}).get('telegram_bot_token') and CONFIG.get('notifications', {}).get('admin_user_ids') and TELEGRAM_AVAILABLE
if TELEGRAM_ENABLED:
     logger.info(f"Telegram notifications enabled for Admin IDs: {CONFIG['notifications']['admin_user_ids']}")
elif CONFIG.get('notifications', {}).get('enable_telegram', False):
     logger.warning("Telegram notifications enabled in config, but token/admin_ids missing or library not installed. Disabling.")


# ==============================================================================
# 2. CTRADER CONNECTOR CLASS (Using ctrader-open-api)
# ==============================================================================
class CTraderConnector:
    """
    Handles connection and interaction with the cTrader Open API using the
    official 'ctrader-open-api' library.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config['ctrader']
        self.trading_config = config['trading']
        self.client: Optional[Client] = None
        self._connected: bool = False
        self._authenticated: bool = False
        self.connection_lock = asyncio.Lock() # Use asyncio lock
        self.request_lock = asyncio.Lock()    # Lock for sending requests to avoid conflicts
        self.latest_ticks: Dict[int, Dict[str, Any]] = {} # {symbol_id: tick_data}
        self.symbol_details: Dict[str, Dict[str, Any]] = {} # {symbol_name_upper: details_dict}
        self.symbol_id_map: Dict[int, str] = {} # {symbol_id: symbol_name_upper}
        self.account_details: Optional[Dict[str, Any]] = None
        self.open_positions: Dict[int, Dict[str, Any]] = {} # {position_id: position_dict}
        self.tick_lock = asyncio.Lock()
        self.position_lock = asyncio.Lock()
        self.symbol_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._message_handler_task: Optional[asyncio.Task] = None
        self._auth_token: Optional[str] = None # Store the dynamically fetched OAuth token
        self._ctid_trader_account_id: Optional[int] = None # Store the API account ID

        # --- Determine Host/Port ---
        self.host: str = self.config['host_demo'] if self.config['use_demo_account'] else self.config['host_live']
        self.port: int = self.config['port_demo'] if self.config['use_demo_account'] else self.config['port_live']
        self.app_id: str = self.config['app_client_id']
        self.app_secret: str = self.config['app_secret']
        self.target_account_id: int = self.config['account_id'] # User-facing account ID from config

        if not CTRADER_API_AVAILABLE:
             logger.critical("!!! cTrader API library 'ctrader-open-api' not available. Cannot proceed. !!!")
             raise ImportError("ctrader-open-api library not found.")

        if not self.app_id or not self.app_secret or not self.target_account_id:
            logger.critical("!!! cTrader App Client ID/Secret or Account ID missing in config or environment variables. Cannot connect. !!!")
            raise ValueError("cTrader API credentials or Account ID missing.")

        # --- Instantiate the cTrader Client ---
        self.client = Client(self.host, self.port, TcpProtocol)
        logger.info(f"CTraderConnector initialized for {'Demo' if self.config['use_demo_account'] else 'Live'} ({self.host}:{self.port})")

    async def _handle_message(self, message: Protobuf):
        """Primary callback for all incoming messages."""
        payload_type = message.payloadType
        # logger.debug(f"Received Message: Type={ProtoPayloadType(payload_type).name if payload_type in ProtoPayloadType._value2member_map_ else payload_type}, ClientMsgId={message.clientMsgId}")

        # Route message based on payload type
        if payload_type == ProtoPayloadType.ERROR_RES:
            await self._handle_error(message)
        elif payload_type == ProtoPayloadType.HEARTBEAT_EVENT:
            await self._send_heartbeat_response() # Keep connection alive
        elif payload_type == ProtoPayloadType.PROTO_OA_ACCOUNT_AUTH_RES:
            await self._handle_account_auth_res(message)
        elif payload_type == ProtoPayloadType.PROTO_OA_SYMBOLS_LIST_RES:
             await self._handle_symbols_list_res(message)
        elif payload_type == ProtoPayloadType.PROTO_OA_TRADER_RES:
             await self._handle_trader_res(message)
        elif payload_type == ProtoPayloadType.PROTO_OA_SPOT_EVENT:
             await self._handle_spot_event(message) # Tick data
        elif payload_type == ProtoPayloadType.PROTO_OA_EXECUTION_EVENT:
             await self._handle_execution_event(message) # Order/Position updates
        elif payload_type == ProtoPayloadType.PROTO_OA_GET_TRENDBARS_RES:
             # This should ideally be handled by awaiting the response directly
             logger.debug("Received Trendbar Response (handled via awaited call)")
             pass
        elif payload_type == ProtoPayloadType.PROTO_OA_DEAL_LIST_RES:
             logger.debug("Received Deal List Response (handled via awaited call)")
             pass # Also handled by awaiting response
        # Add handlers for other relevant message types as needed

    async def _send_heartbeat_response(self):
        """Sends a heartbeat back to the server."""
        request = MessagesFactory.ProtoHeartbeatEvent()
        await self._send_request(request)
        # logger.debug("Sent Heartbeat Response")

    async def _handle_error(self, message: Protobuf):
        """Handles PROTO_OA_ERROR_RES messages."""
        error_msg = Protobuf.extract(message)
        error_code = error_msg.errorCode
        description = error_msg.description
        logger.error(f"cTrader API Error: Code={error_code} ({ProtoErrorCode(error_code).name if error_code in ProtoErrorCode._value2member_map_ else 'Unknown'}), Desc={description}, ClientMsgId={message.clientMsgId}")
        # Specific error handling logic can be added here
        if error_code == ProtoErrorCode.OA_AUTH_TOKEN_EXPIRED or \
           error_code == ProtoErrorCode.INVALID_OA_CLIENT_AUTH_TOKEN or \
           error_code == ProtoErrorCode.ACCOUNT_NOT_AUTHORIZED:
            logger.warning("Authentication error detected. Attempting re-authentication...")
            self._authenticated = False
            self._auth_token = None # Clear token
            # Attempt re-auth (might need a small delay)
            await asyncio.sleep(2)
            await self.authenticate_account() # Try to re-auth

    async def _handle_account_auth_res(self, message: Protobuf):
        """Handles account authentication response."""
        response = Protobuf.extract(message)
        # Check if the response corresponds to the account we wanted
        if response.ctidTraderAccountId == self._ctid_trader_account_id:
            self._authenticated = True
            logger.info(f"cTrader Account {self.target_account_id} (ctid: {self._ctid_trader_account_id}) authorized successfully.")
            # Fetch essential data after successful auth
            await self.fetch_account_details()
            await self.fetch_all_symbol_details() # Get all symbols for ID mapping
            await self.subscribe_to_symbols([self.trading_config['symbol']])
            await self.fetch_open_positions()
        else:
            logger.warning(f"Received auth response for unexpected account: {response.ctidTraderAccountId}. Ignoring.")


    async def _handle_trader_res(self, message: Protobuf):
        """Handles trader (account details) response."""
        response = Protobuf.extract(message)
        if response.trader.ctidTraderAccountId == self._ctid_trader_account_id:
            details = {
                'ctidTraderAccountId': response.trader.ctidTraderAccountId,
                'balance': response.trader.balance / 100.0, # Convert from cents/minor units
                'equity': response.trader.equity / 100.0,
                'marginLevel': response.trader.marginLevel, # Percentage or NaN
                'currency': response.trader.depositAssetId, # Assuming deposit asset ID is currency code; might need lookup
                # Add other relevant fields: freeMargin, marginUsed, etc.
            }
            # Need to map depositAssetId (e.g., 1) to currency code (e.g., "USD")
            # This requires fetching Asset details, or making an assumption based on common usage
            # For now, we'll assume it's the currency if available, otherwise use a default
            details['currency'] = self._get_currency_from_asset_id(response.trader.depositAssetId) or 'USD' # Placeholder lookup

            self.account_details = details
            logger.info(f"Account details updated: Balance={details['balance']:.2f} {details['currency']}, Equity={details['equity']:.2f}")
        else:
             logger.warning(f"Received trader response for unexpected account: {response.trader.ctidTraderAccountId}. Ignoring.")

    def _get_currency_from_asset_id(self, asset_id: int) -> Optional[str]:
        # Basic mapping - should ideally fetch ProtoOAAssetListRes
        # Common IDs: 1=USD, 2=EUR, 3=GBP, 4=JPY ... (this varies by broker!)
        # This is a simplification and might be incorrect.
        mapping = {1: "USD", 2: "EUR", 3: "GBP", 4: "JPY", 5: "AUD", 6: "CAD", 7:"CHF", 8:"NZD"}
        return mapping.get(asset_id)


    async def _handle_spot_event(self, message: Protobuf):
        """Handles incoming tick data."""
        event = Protobuf.extract(message)
        symbol_id = event.symbolId
        bid = event.bid / 100000.0 if event.HasField("bid") else None # Prices are typically 1e5 scaled integers
        ask = event.ask / 100000.0 if event.HasField("ask") else None
        # Sometimes only trendbars come in spot events
        if bid is None or ask is None:
            # logger.debug(f"Spot event for SymbolID {symbol_id} without bid/ask (likely trendbar update).")
            return

        timestamp_ms = int(time.time() * 1000) # API doesn't seem to provide timestamp in spot event itself

        async with self.symbol_lock:
            symbol_name = self.symbol_id_map.get(symbol_id)
            if not symbol_name:
                 # logger.warning(f"Received tick for unknown SymbolID: {symbol_id}. Fetching symbols again.")
                 # await self.fetch_all_symbol_details() # Might be too slow here
                 return
            details = self.symbol_details.get(symbol_name)
            if not details: return # Should not happen if symbol_name was found

        digits = details.get('digits', 5)
        pip_position = details.get('pipPosition', 4)
        pip_value_price = 10**(-pip_position) if pip_position is not None else 0.0

        tick_data = {
            'time': datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc),
            'bid': round(bid, digits),
            'ask': round(ask, digits),
            'spread_raw': round(ask - bid, digits),
            'spread_pips': round((ask - bid) / pip_value_price, 1) if pip_value_price > 0 else 0,
            'symbolId': symbol_id,
            'symbol': symbol_name # Add symbol name for easier use
        }
        async with self.tick_lock:
            self.latest_ticks[symbol_id] = tick_data
        # logger.debug(f"Tick received for {symbol_name} (ID:{symbol_id}): Bid={bid:.{digits}f}, Ask={ask:.{digits}f}")


    async def _handle_execution_event(self, message: Protobuf):
        """Handles order fills, position updates, etc."""
        event = Protobuf.extract(message)
        exec_type = event.executionType
        # logger.debug(f"Execution Event: Type={ProtoExecutionType(exec_type).name}, ClientMsgId={message.clientMsgId}")

        position_updated = False
        order = event.order if event.HasField("order") else None
        position = event.position if event.HasField("position") else None

        # --- Order Related ---
        if order:
             order_id = order.orderId
             position_id = order.positionId # Link order to position
             order_status = order.orderStatus
             logger.info(f"Order Event: OrderID={order_id}, PosID={position_id}, Status={ProtoOrderStatus(order_status).name}")
             # Can update internal order state if needed

        # --- Position Related ---
        if position:
             pos_id = position.positionId
             pos_status = position.positionStatus
             logger.info(f"Position Event: PosID={pos_id}, Status={ProtoPositionStatus(pos_status).name}, Vol={position.tradeData.volume/100.0}") # Volume in units

             # Check if position is closed or opened/modified
             if pos_status == ProtoPositionStatus.POSITION_STATUS_CLOSED:
                 async with self.position_lock:
                     if pos_id in self.open_positions:
                         closed_position_data = self.open_positions.pop(pos_id)
                         position_updated = True
                         logger.info(f"Position {pos_id} closed via ExecutionEvent.")
                         # TODO: Trigger performance logging here using 'closed_position_data'
                         # Need to fetch deal list for accurate P/L, comms, swap associated with this closure.
                         # Example call (needs implementation in connector and tracker):
                         # await self.log_closed_trade_from_history(closed_position_data)
             elif pos_status == ProtoPositionStatus.POSITION_STATUS_OPEN or pos_status == ProtoPositionStatus.POSITION_STATUS_CLOSING: # CLOSING means partial close pending
                 async with self.position_lock:
                      # Convert position protobuf to dict for storage
                      pos_data = self._protobuf_position_to_dict(position)
                      if pos_data:
                          self.open_positions[pos_id] = pos_data
                          position_updated = True
                          logger.debug(f"Position {pos_id} opened/updated via ExecutionEvent.")

        # --- Balance Related ---
        # if exec_type == ProtoExecutionType.DEPOSIT_WITHDRAW: ... handle balance updates ...

        if position_updated:
             # Optional: Trigger state save or notify other components
             pass

    def _protobuf_position_to_dict(self, position_proto) -> Optional[Dict[str, Any]]:
         """Converts a ProtoOAPosition protobuf message to a dictionary."""
         if not position_proto: return None

         symbol_id = position_proto.tradeData.symbolId
         asyncio.run_coroutine_threadsafe(self.ensure_symbol_details_fetched(symbol_id), asyncio.get_event_loop()) # Fetch if missing
         symbol_name = self.symbol_id_map.get(symbol_id, f"ID_{symbol_id}")
         details = self.symbol_details.get(symbol_name)
         digits = details.get('digits', 5) if details else 5

         pos_dict = {
             'positionId': position_proto.positionId,
             'ctidTraderAccountId': position_proto.ctidTraderAccountId,
             'symbolId': symbol_id,
             'symbolName': symbol_name, # Add symbol name
             'tradeSide': position_proto.tradeData.tradeSide, # ProtoOATradeSide enum (1=BUY, 2=SELL)
             'volume': position_proto.tradeData.volume, # In UNITS (e.g., 100 = 0.01 lots for XAUUSD)
             'entryPrice': position_proto.price, # Already scaled correctly? API docs suggest yes.
             'openTimestamp': position_proto.tradeData.openTimestamp,
             'slPrice': position_proto.stopLoss if position_proto.HasField("stopLoss") else None,
             'tpPrice': position_proto.takeProfit if position_proto.HasField("takeProfit") else None,
             'swap': position_proto.swap / 100.0, # Convert from cents
             'commission': position_proto.commission / 100.0, # Convert from cents
             'grossProfit': position_proto.grossProfit / 100.0, # Convert from cents (current unrealized P/L)
             'marginRate': position_proto.marginRate,
             'comment': position_proto.tradeData.comment if position_proto.tradeData.HasField("comment") else None,
             'status': position_proto.positionStatus, # ProtoPositionStatus enum
             'digits': digits # Store digits for formatting
             # Add other fields like guaranteedStopLoss, usedMargin etc. if needed
         }
         return pos_dict

    async def ensure_symbol_details_fetched(self, symbol_id: int):
        """Fetches symbol details if not already cached (used by callbacks)."""
        async with self.symbol_lock:
            if symbol_id not in self.symbol_id_map:
                logger.info(f"Details missing for SymbolID {symbol_id}. Fetching all symbols.")
                await self.fetch_all_symbol_details() # Fetch all to populate map


    async def _send_request(self, request_proto: Protobuf, *, timeout: float = 10.0, requires_auth: bool = True):
        """Sends a request protobuf message and waits for a response."""
        if not self.client or not self._connected:
            logger.error("Cannot send request: Client not connected.")
            return None
        if requires_auth and not self._authenticated:
             logger.error("Cannot send request: Client not authenticated.")
             # Optionally trigger re-auth here
             # await self.authenticate_account()
             # if not self._authenticated: return None
             return None

        # The ctrader-open-api library handles message IDs internally when using client.send()
        async with self.request_lock: # Ensure only one request is sent at a time
            try:
                # logger.debug(f"Sending Request: {type(request_proto).__name__}")
                response = await asyncio.wait_for(
                    self.client.send(request_proto),
                    timeout=timeout
                )
                # logger.debug(f"Received Response for {type(request_proto).__name__}")
                # Basic error check within response (if applicable)
                if hasattr(response, 'errorCode'):
                    logger.error(f"API call {type(request_proto).__name__} returned error in response: {response.errorCode}")
                    # Handle specific errors if needed
                return response
            except asyncio.TimeoutError:
                logger.error(f"Request {type(request_proto).__name__} timed out after {timeout}s.")
                return None
            except ConnectionError as e:
                logger.error(f"Connection error during request {type(request_proto).__name__}: {e}")
                self._connected = False # Mark as disconnected
                self._authenticated = False
                return None
            except Exception as e:
                logger.error(f"Exception during request {type(request_proto).__name__}: {e}", exc_info=True)
                return None

    async def connect(self, retries: Optional[int] = None) -> bool:
        """Establishes connection, authenticates the app, and authorizes the account."""
        retry_count = retries if retries is not None else self.config.get('connect_retries', 3)
        delay = self.config.get('connect_retry_delay_s', 5)
        attempt = 0

        async with self.connection_lock:
            if self._connected and self._authenticated:
                logger.info("Already connected and authenticated.")
                return True

            while attempt < retry_count:
                attempt += 1
                logger.info(f"Attempting cTrader connection and auth (Attempt {attempt}/{retry_count})...")
                try:
                    # --- 1. Connect ---
                    if not self.client.is_connected():
                        connection_task = asyncio.create_task(self.client.connect())
                        await asyncio.wait_for(connection_task, timeout=10) # 10s timeout for connection
                        if not self.client.is_connected():
                             logger.error(f"Connection attempt {attempt} failed.")
                             if attempt < retry_count: await asyncio.sleep(delay)
                             continue
                        self._connected = True
                        # Start the message handler loop
                        self._message_handler_task = asyncio.create_task(self.client.listen(self._handle_message))
                        logger.info("TCP Connection established. Starting message listener.")

                    # --- 2. Authenticate Application ---
                    if not self._auth_token:
                         logger.info("Authenticating application...")
                         auth = Auth(self.app_id, self.app_secret)
                         auth_response = await asyncio.wait_for(auth.authorize(), timeout=10)
                         if auth_response and auth_response.access_token and auth_response.token_type == "Bearer":
                              self._auth_token = auth_response.access_token
                              logger.info("Application authenticated successfully.")
                         else:
                              logger.error(f"Application authentication failed: {auth_response}")
                              await self.disconnect() # Disconnect on auth fail
                              if attempt < retry_count: await asyncio.sleep(delay)
                              continue

                    # --- 3. Get Available Accounts ---
                    if not self._ctid_trader_account_id:
                         logger.info("Fetching available trading accounts...")
                         acc_list_req = MessagesFactory.ProtoOAAccountListReq(self._auth_token)
                         acc_list_res = await self._send_request(acc_list_req, requires_auth=False) # Auth token is in payload
                         if acc_list_res and acc_list_res.HasField("ctidTraderAccount"):
                             found = False
                             for acc in acc_list_res.ctidTraderAccount:
                                 # The account ID in config is the user-visible one, not the ctidTraderAccountId usually
                                 # We need a way to map. Assuming the user configured the *correct* ctid.
                                 # A better approach would be to list accounts and let user choose/verify.
                                 # For now, we assume the config 'account_id' IS the ctidTraderAccountId.
                                 if acc.ctidTraderAccountId == self.target_account_id:
                                      self._ctid_trader_account_id = acc.ctidTraderAccountId
                                      logger.info(f"Found target account: ID={self.target_account_id}, Broker={acc.brokerName}")
                                      found = True
                                      break
                             if not found:
                                 logger.error(f"Target account ID {self.target_account_id} not found in authorized accounts.")
                                 # List found accounts for debugging:
                                 acc_ids = [str(a.ctidTraderAccountId) for a in acc_list_res.ctidTraderAccount]
                                 logger.error(f"Available account IDs: {', '.join(acc_ids)}")
                                 await self.disconnect()
                                 # Don't retry if account not found, it's a config error
                                 return False # Exit connect attempt

                         else:
                              logger.error(f"Failed to fetch account list: {acc_list_res}")
                              await self.disconnect()
                              if attempt < retry_count: await asyncio.sleep(delay)
                              continue

                    # --- 4. Authorize Trading Account ---
                    if not self._authenticated:
                         logger.info(f"Authorizing trading account {self._ctid_trader_account_id}...")
                         acc_auth_req = MessagesFactory.ProtoOAAccountAuthReq(self._auth_token, self._ctid_trader_account_id)
                         # Response handled by _handle_account_auth_res callback
                         await self._send_request(acc_auth_req, requires_auth=False) # Auth token in payload

                         # Wait a moment for the callback to set the flag
                         await asyncio.sleep(2)
                         if self._authenticated:
                              logger.info("Connect and Auth process complete.")
                              return True
                         else:
                              logger.error("Account authorization confirmation not received.")
                              await self.disconnect()
                              if attempt < retry_count: await asyncio.sleep(delay)
                              continue

                    # If we reach here, it means we were already connected and authenticated
                    return True

                except asyncio.TimeoutError as e:
                    logger.error(f"Timeout during cTrader connect/auth attempt {attempt}: {e}", exc_info=False)
                    await self.disconnect() # Ensure clean state
                    if attempt < retry_count: await asyncio.sleep(delay)
                except ConnectionRefusedError as e:
                    logger.error(f"Connection refused during cTrader connect/auth attempt {attempt}: {e}")
                    await self.disconnect()
                    if attempt < retry_count: await asyncio.sleep(delay)
                except Exception as e:
                    logger.error(f"Exception during cTrader connect/auth attempt {attempt}: {e}", exc_info=True)
                    await self.disconnect() # Ensure clean state
                    if attempt < retry_count: await asyncio.sleep(delay)

            logger.critical(f"Failed to connect and authenticate to cTrader after {retry_count} attempts.")
            return False

    async def disconnect(self):
        """Disconnects from the API."""
        logger.info("Disconnecting from cTrader API...")
        self._stop_event.set() # Signal background tasks to stop if any

        async with self.connection_lock:
            if self._message_handler_task and not self._message_handler_task.done():
                self._message_handler_task.cancel()
                try:
                    await self._message_handler_task
                except asyncio.CancelledError:
                    logger.info("Message handler task cancelled.")
                self._message_handler_task = None

            if self.client and self.client.is_connected():
                try:
                    await self.client.disconnect()
                except Exception as e:
                    logger.error(f"Exception during cTrader client disconnect: {e}")

            self._connected = False
            self._authenticated = False
            self._auth_token = None
            # Keep _ctid_trader_account_id ? Maybe, maybe not. Clear for safety.
            # self._ctid_trader_account_id = None
        logger.info("Disconnected.")

    async def check_connection(self) -> bool:
        """Checks if the connection is active and authenticated."""
        async with self.connection_lock:
            is_ok = self._connected and self._authenticated and self.client and self.client.is_connected()

        if not is_ok:
            logger.warning("check_connection: Not connected or authenticated.")
            # Attempt reconnect automatically
            await self.connect(retries=1) # Try one reconnect attempt
            async with self.connection_lock: # Re-check status
                is_ok = self._connected and self._authenticated and self.client and self.client.is_connected()
        else:
            # Optional: Perform a lightweight check like getting balance
            if self.config.get('enable_heartbeat_check', False): # Add config flag
                 try:
                    await self.fetch_account_details(force_refresh=True) # Use fetch as a heartbeat
                 except Exception as e:
                      logger.warning(f"Heartbeat check (fetch_account_details) failed: {e}. Marking as potentially disconnected.")
                      is_ok = False
                      self._connected = False # Assume connection issue
                      self._authenticated = False
                      # Trigger reconnect on next cycle
        return is_ok


    # --- Callback Handlers (Now integrated into _handle_message) ---
    # Individual handlers like _handle_tick_data, _handle_error, etc. are called by _handle_message

    # --- Data Fetching Methods ---
    def get_current_tick(self, symbol_name: str) -> Optional[Dict[str, Any]]:
        """Gets the latest stored tick data for a symbol name."""
        symbol_name = symbol_name.upper()
        details = self.get_symbol_details(symbol_name) # Use sync getter for cached details
        if not details:
            logger.warning(f"Cannot get tick: No details cached for {symbol_name}")
            # Optionally trigger async fetch here if needed, but this func is sync
            return None
        symbol_id = details.get('symbolId')
        if not symbol_id:
             logger.warning(f"Cannot get tick: Symbol ID missing for {symbol_name}")
             return None

        # Use asyncio lock correctly in sync method (if needed, but tick_lock is async)
        # This might be better redesigned if called from async context often
        # For now, assume it's called less frequently or access is brief
        # Or make this method async? Let's keep it sync for now.
        # Consider potential race condition if called while lock is held by async task.
        # A read-only access might be okay without lock if updates are atomic enough.
        # async with self.tick_lock: # Can't await in sync method
        return self.latest_ticks.get(symbol_id) # Direct access, assuming dict updates are atomic enough


    async def fetch_all_symbol_details(self):
        """Fetches details for all symbols for the current account."""
        if not self._ctid_trader_account_id:
             logger.error("Cannot fetch symbols: Account not authorized.")
             return False
        logger.info(f"Fetching all symbol details for account {self._ctid_trader_account_id}...")
        request = MessagesFactory.ProtoOASymbolsListReq(self._ctid_trader_account_id)
        response = await self._send_request(request)

        if response and response.HasField("symbol"):
            count = 0
            async with self.symbol_lock:
                self.symbol_details.clear()
                self.symbol_id_map.clear()
                for symbol_data in response.symbol:
                    name = symbol_data.symbolName
                    symbol_id = symbol_data.symbolId
                    details = self._protobuf_symbol_to_dict(symbol_data)
                    if name and details:
                         self.symbol_details[name.upper()] = details
                         self.symbol_id_map[symbol_id] = name.upper()
                         count += 1
            logger.info(f"Fetched and cached details for {count} symbols.")
            return True
        else:
            logger.error(f"Failed to fetch symbol list: {response}")
            return False

    async def fetch_symbol_details(self, symbol_name: str) -> Optional[Dict[str, Any]]:
        """Fetches and caches details for a specific symbol by name."""
        symbol_name = symbol_name.upper()
        async with self.symbol_lock:
            if symbol_name in self.symbol_details:
                return self.symbol_details[symbol_name] # Return cached

        # If not cached, fetch all symbols (API doesn't have get single symbol by name)
        logger.info(f"Symbol {symbol_name} not in cache. Fetching all symbols to find it.")
        if await self.fetch_all_symbol_details():
            async with self.symbol_lock: # Re-check cache after fetching all
                return self.symbol_details.get(symbol_name)
        else:
            logger.error(f"Failed to fetch any symbols, cannot get details for {symbol_name}.")
            return None


    def _protobuf_symbol_to_dict(self, symbol_data) -> Optional[Dict[str, Any]]:
         """Converts ProtoOASymbol protobuf to dictionary."""
         if not symbol_data: return None
         # Volume conversion: min/max/step are in 1e-2 units (cents of lots?)
         # Need to clarify how this translates to API volume units (1e-5 of base unit?)
         # Assuming lotSize = 10000 (e.g. XAUUSD Ounces or EURUSD Units)
         # If minTradeVolume is 1000, it means 0.1 lots?
         # Let's assume the volumes in symbol_data are in LOTS * 100 (cents of lots)
         # And the API expects volume in UNITS (base units * 100000?) -- Needs verification!
         # *** THIS VOLUME CONVERSION IS A GUESS AND LIKELY NEEDS ADJUSTMENT ***
         # CTrader Volume: Typically lots * 100 (e.g., 1 lot = 100, 0.01 lots = 1)
         # ProtoOASymbol Volume: minVolume/maxVolume/stepVolume = volume in CENTS of lots (e.g., 1 = 0.01 lots)

         lot_size = symbol_data.lotSize # Usually 100000 for Forex, check for Indices/Metals
         if lot_size == 0: lot_size = 100000 # Default guess if missing

         # Convert ProtoOA volume (cents of lots) to cTrader API volume units (lots * 100)
         min_volume_api = symbol_data.minVolume # e.g., 1 cent of lot = 1 API unit (0.01 lots)
         max_volume_api = symbol_data.maxVolume # e.g., 100000 cents of lot = 1000 API units (10 lots)
         step_volume_api = symbol_data.stepVolume # e.g., 1 cent of lot = 1 API unit step (0.01 lots)

         details = {
             "symbolId": symbol_data.symbolId,
             "symbolName": symbol_data.symbolName,
             "baseAssetId": symbol_data.baseAssetId,
             "quoteAssetId": symbol_data.quoteAssetId,
             "digits": symbol_data.digits,
             "pipPosition": symbol_data.pipPosition,
             "lotSize": lot_size, # Base units per lot (e.g., 100000 for EURUSD)
             "minVolume": min_volume_api, # Min volume in API units (lots * 100)
             "maxVolume": max_volume_api, # Max volume in API units (lots * 100)
             "stepVolume": step_volume_api, # Step volume in API units (lots * 100)
             "enabled": symbol_data.enabled,
             "swapLong": symbol_data.swapLong, # Scaled by 100? Check docs
             "swapShort": symbol_data.swapShort,
             # Add more fields if needed: measurementUnits, commissionType, etc.
         }
         return details

    def get_symbol_details(self, symbol_name: str) -> Optional[Dict[str, Any]]:
         """Gets cached symbol details by name (synchronous)."""
         # Use lock briefly for read access, assuming dict access is thread-safe enough
         # async with self.symbol_lock: # Cannot await in sync method
         return self.symbol_details.get(symbol_name.upper())

    def get_symbol_details_by_id(self, symbol_id: int) -> Optional[Dict[str, Any]]:
        """Gets cached symbol details by ID (synchronous)."""
        symbol_name = self.symbol_id_map.get(symbol_id)
        if symbol_name:
            return self.symbol_details.get(symbol_name)
        return None

    async def subscribe_to_symbols(self, symbol_names: List[str]):
        """Subscribes to real-time tick data for given symbols."""
        if not self._ctid_trader_account_id:
             logger.error("Cannot subscribe: Account not authorized.")
             return False
        if not self._connected:
             logger.error("Cannot subscribe: Not connected.")
             return False

        symbol_ids_to_sub: List[int] = []
        symbol_ids_to_unsub: List[int] = [] # Optional: Unsubscribe others if needed

        for symbol_name in symbol_names:
            details = self.get_symbol_details(symbol_name) # Sync getter
            if not details:
                 logger.warning(f"Details for {symbol_name} not found in cache. Attempting fetch.")
                 details = await self.fetch_symbol_details(symbol_name) # Fetch specific if needed
                 if not details:
                      logger.error(f"Cannot subscribe to {symbol_name}, failed to get details.")
                      continue
            symbol_id = details.get('symbolId')
            if symbol_id:
                 symbol_ids_to_sub.append(symbol_id)
            else: logger.error(f"Symbol ID missing for {symbol_name}, cannot subscribe.")

        if not symbol_ids_to_sub:
             logger.warning("No valid symbol IDs found to subscribe to.")
             return False

        logger.info(f"Subscribing to spots for Symbol IDs: {symbol_ids_to_sub} (Account {self._ctid_trader_account_id})...")
        request = MessagesFactory.ProtoOASubscribeSpotsReq(
            self._ctid_trader_account_id,
            symbol_ids_to_sub
        )
        response = await self._send_request(request)

        if response: # Response is ProtoOASubscribeSpotsRes (currently empty)
            logger.info(f"Successfully sent spot subscription request for IDs: {symbol_ids_to_sub}")
            # Store subscribed symbols if needed
            return True
        else:
            logger.error(f"Failed to send spot subscription request for IDs: {symbol_ids_to_sub}")
            return False

    async def get_historical_data(self, symbol_name: str, timeframe_str: str, num_bars: int) -> Optional[pd.DataFrame]:
        """Fetches historical OHLCV data."""
        if not self._ctid_trader_account_id: return None

        details = self.get_symbol_details(symbol_name) # Sync getter
        if not details:
            details = await self.fetch_symbol_details(symbol_name)
            if not details:
                logger.error(f"Cannot get historical data for {symbol_name}, details/ID missing.")
                return None
        symbol_id = details.get('symbolId')
        if not symbol_id:
             logger.error(f"Symbol ID missing for {symbol_name}")
             return None

        period_enum = self._convert_tf_str_to_enum(timeframe_str)
        if period_enum is None:
            logger.error(f"Unsupported timeframe string: {timeframe_str}")
            return None

        # Calculate 'from' and 'to' timestamps
        # API requires 'to' timestamp and number of bars ('count') *before* 'to'
        # We want the *latest* N bars, so 'to' should be now.
        to_timestamp = int(time.time() * 1000) # Current time in ms UTC

        logger.debug(f"Fetching {num_bars} bars for {symbol_name} (ID: {symbol_id}), TF: {timeframe_str} (Enum: {period_enum}), ending around now...")
        request = MessagesFactory.ProtoOAGetTrendbarsReq(
            ctidTraderAccountId=self._ctid_trader_account_id,
            period=period_enum,
            symbolId=symbol_id,
            count=num_bars,
            toTimestamp=to_timestamp
        )
        bars_result = await self._send_request(request)

        if bars_result and hasattr(bars_result, 'trendbar') and len(bars_result.trendbar) > 0:
            bars_data = bars_result.trendbar
            logger.debug(f"Received {len(bars_data)} bars from API.")
            data = []
            # OHLC are relative to delta, Volume is uint64
            # Need to reconstruct absolute OHLC
            # Timestamps are in minutes/seconds/etc from start time based on period? Check docs.
            # It seems `timestamp` field IS the start time of the bar in MS UTC.
            # Open = deltaOpen + low
            # High = deltaHigh + low
            # Close = deltaClose + low
            # Low = low (absolute value)

            for bar in bars_data:
                 low = bar.low / 100000.0 # Assuming price is 1e5 scaled
                 o = (bar.deltaOpen + bar.low) / 100000.0 if bar.HasField("deltaOpen") else low # Open is optional? Use low if missing.
                 h = (bar.deltaHigh + bar.low) / 100000.0
                 c = (bar.deltaClose + bar.low) / 100000.0

                 data.append({
                     'time': pd.to_datetime(bar.utcTimestampInMinutes * 60 * 1000, unit='ms', utc=True), # Convert minutes to ms timestamp
                     'open': o,
                     'high': h,
                     'low': low,
                     'close': c,
                     'volume': bar.volume # Volume is tick count? Or actual volume? API says "number of ticks"
                 })

            if not data:
                 logger.warning(f"Processed 0 bars after parsing for {symbol_name} {timeframe_str}.")
                 return pd.DataFrame()

            df = pd.DataFrame(data)
            df.set_index('time', inplace=True)

            # Ensure standard columns exist
            standard_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in standard_cols:
                 if col not in df.columns: df[col] = np.nan # Add missing columns as NaN
            df = df[standard_cols] # Select/reorder

            logger.debug(f"Fetched {len(df)} bars for {symbol_name} {timeframe_str}. Last bar time: {df.index[-1]}")
            # Sort just in case API doesn't guarantee order
            df.sort_index(inplace=True)
            return df
        elif bars_result and hasattr(bars_result, 'trendbar') and len(bars_result.trendbar) == 0:
             logger.warning(f"Received 0 bars from API for {symbol_name} {timeframe_str} (Count: {num_bars}).")
             return pd.DataFrame() # Return empty dataframe
        else:
            logger.error(f"Failed to get historical data or unexpected format for {symbol_name} {timeframe_str}: {bars_result}")
            return None

    def _convert_tf_str_to_enum(self, tf_str: str) -> Optional[int]:
        """Converts string like "M5" to the library's ProtoOATrendbarPeriod enum value."""
        mapping = {
            "M1": ProtoOATrendbarPeriod.M1, "M2": ProtoOATrendbarPeriod.M2, "M3": ProtoOATrendbarPeriod.M3,
            "M4": ProtoOATrendbarPeriod.M4, "M5": ProtoOATrendbarPeriod.M5, "M10": ProtoOATrendbarPeriod.M10,
            "M15": ProtoOATrendbarPeriod.M15, "M30": ProtoOATrendbarPeriod.M30,
            "H1": ProtoOATrendbarPeriod.H1, "H2": ProtoOATrendbarPeriod.H2, "H3": ProtoOATrendbarPeriod.H3,
            "H4": ProtoOATrendbarPeriod.H4, "H6": ProtoOATrendbarPeriod.H6, "H8": ProtoOATrendbarPeriod.H8,
            "H12": ProtoOATrendbarPeriod.H12,
            "D1": ProtoOATrendbarPeriod.D1,
            "W1": ProtoOATrendbarPeriod.W1,
            "MN1": ProtoOATrendbarPeriod.MN1,
        }
        return mapping.get(tf_str.upper())

    def _convert_enum_to_tf_str(self, tf_enum: int) -> str:
         """Converts library enum ProtoOATrendbarPeriod to string like "M5"."""
         reverse_mapping = {v: k for k, v in {
            "M1": ProtoOATrendbarPeriod.M1, "M2": ProtoOATrendbarPeriod.M2, "M3": ProtoOATrendbarPeriod.M3,
            "M4": ProtoOATrendbarPeriod.M4, "M5": ProtoOATrendbarPeriod.M5, "M10": ProtoOATrendbarPeriod.M10,
            "M15": ProtoOATrendbarPeriod.M15, "M30": ProtoOATrendbarPeriod.M30,
            "H1": ProtoOATrendbarPeriod.H1, "H2": ProtoOATrendbarPeriod.H2, "H3": ProtoOATrendbarPeriod.H3,
            "H4": ProtoOATrendbarPeriod.H4, "H6": ProtoOATrendbarPeriod.H6, "H8": ProtoOATrendbarPeriod.H8,
            "H12": ProtoOATrendbarPeriod.H12,
            "D1": ProtoOATrendbarPeriod.D1,
            "W1": ProtoOATrendbarPeriod.W1,
            "MN1": ProtoOATrendbarPeriod.MN1,
         }.items()}
         return reverse_mapping.get(tf_enum, f"Enum({tf_enum})")

    # --- Account & Position Info ---
    async def fetch_account_details(self, force_refresh: bool = False):
         """Fetches and caches account balance and currency."""
         if not self._ctid_trader_account_id: return None
         if self.account_details and not force_refresh: return self.account_details # Return cache

         logger.info("Fetching account details...")
         request = MessagesFactory.ProtoOATraderReq(self._ctid_trader_account_id)
         # Response handled by _handle_trader_res callback, which updates self.account_details
         await self._send_request(request)
         # Wait a short time for callback processing
         await asyncio.sleep(0.2)
         return self.account_details

    def get_account_balance(self) -> Optional[float]:
        """Returns the cached account balance (synchronous)."""
        return self.account_details.get('balance') if self.account_details else None

    def get_account_currency(self) -> Optional[str]:
         """Returns the cached account currency (synchronous)."""
         return self.account_details.get('currency') if self.account_details else None

    def get_ctid_trader_account_id(self) -> Optional[int]:
         """Returns the internal cTrader account ID."""
         return self._ctid_trader_account_id

    async def fetch_open_positions(self) -> List[Dict[str, Any]]:
        """Fetches and caches currently open positions."""
        if not self._ctid_trader_account_id: return []
        logger.debug("Fetching open positions...")
        request = MessagesFactory.ProtoOAGetPositionListReq(self._ctid_trader_account_id)
        result = await self._send_request(request)

        if result and hasattr(result, 'position') and len(result.position) >= 0:
             current_positions_proto = result.position
             new_positions_dict = {}
             async with self.symbol_lock: # Need lock for symbol mapping
                 for pos_proto in current_positions_proto:
                     pos_dict = self._protobuf_position_to_dict(pos_proto)
                     if pos_dict:
                          new_positions_dict[pos_dict['positionId']] = pos_dict

             async with self.position_lock:
                  self.open_positions = new_positions_dict
             logger.debug(f"Fetched {len(self.open_positions)} open positions.")
             return list(self.open_positions.values())
        else:
             logger.error(f"Failed to fetch open positions: {result}")
             async with self.position_lock: self.open_positions = {} # Clear cache on failure
             return []

    def get_open_positions(self) -> List[Dict[str, Any]]:
        """Returns the cached list of open positions (synchronous)."""
        # async with self.position_lock: # Cannot await
        return list(self.open_positions.values()) # Assume atomic read

# --- Continuation of CTraderConnector class ---

    # --- Order Execution Methods ---

    async def place_market_order(self,
                                 symbol_name: str,
                                 trade_side: ProtoOATradeSide, # Use the enum directly
                                 volume_lots: float, # Take volume in LOTS
                                 stop_loss_price: Optional[float] = None,
                                 take_profit_price: Optional[float] = None,
                                 signal_id: str = "") -> Optional[Dict[str, Any]]:
        """
        Places a market order using LOTS for volume.
        Converts lots to cTrader API volume units based on symbol details.
        Handles SL/TP by PRICE.
        """
        if not CONFIG['execution']['enable_auto_trading']:
            logger.warning("Auto-trading disabled. Skipping order placement.")
            return None
        if not await self.check_connection(): return None
        if not self._ctid_trader_account_id: return None

        symbol_name = symbol_name.upper()
        details = self.get_symbol_details(symbol_name) # Sync getter
        if not details:
            details = await self.fetch_symbol_details(symbol_name)
            if not details:
                logger.error(f"Cannot place order for {symbol_name}, details/ID missing.")
                return None
        symbol_id = details.get('symbolId')
        if not symbol_id:
            logger.error(f"Cannot place order: Symbol ID missing for {symbol_name}")
            return None

        # --- Volume Conversion and Validation ---
        # cTrader API expects volume in units = lots * 100
        volume_units = int(round(volume_lots * 100))

        min_vol_api = details.get('minVolume', 1) # API units (lots * 100)
        max_vol_api = details.get('maxVolume', 100000) # API units (lots * 100)
        step_vol_api = details.get('stepVolume', 1) # API units (lots * 100)

        if volume_units < min_vol_api:
             logger.error(f"Order volume {volume_units} units ({volume_lots} lots) is less than minimum {min_vol_api} units for {symbol_name}. Cannot place order.")
             return None
        if volume_units > max_vol_api:
             logger.warning(f"Order volume {volume_units} units ({volume_lots} lots) exceeds maximum {max_vol_api} units for {symbol_name}. Clamping to max.")
             volume_units = max_vol_api
        # Adjust volume to step (round down to nearest step)
        volume_units = (volume_units // step_vol_api) * step_vol_api
        if volume_units < min_vol_api: # Check again after rounding
             logger.error(f"Order volume {volume_units} units (after step rounding) is less than minimum {min_vol_api}. Cannot place order.")
             return None

        # Ensure integer volume (should be already, but belt-and-suspenders)
        volume_units = int(volume_units)
        final_lots = volume_units / 100.0 # Recalculate lots for logging

        # --- SL/TP Handling ---
        # API allows SL/TP by relative pips (stopLossInPips, takeProfitInPips)
        # OR by absolute price (stopLoss, takeProfit)
        # Let's use absolute price as passed in.
        # Ensure prices are rounded to symbol digits
        digits = details.get('digits', 5)
        sl_price_rounded = round(stop_loss_price, digits) if stop_loss_price is not None else None
        tp_price_rounded = round(take_profit_price, digits) if take_profit_price is not None else None

        comment = f"{CONFIG.get('bot_version', 'CTBot')}-{signal_id[:15]}" # Keep comment short
        client_order_id = f"signal_{signal_id}_{int(time.time()*1000)}" # Unique client ID

        retries = CONFIG.get('execution', {}).get('order_retry_count', 2)
        delay = CONFIG.get('execution', {}).get('order_retry_delay_s', 3)
        attempt = 0

        logger.info(f"Attempting Market Order: {symbol_name} ({trade_side.name}), Vol: {volume_units} units ({final_lots:.2f} lots), SL: {sl_price_rounded}, TP: {tp_price_rounded}")

        while attempt <= retries:
            attempt += 1
            try:
                order_params = {
                    "ctidTraderAccountId": self._ctid_trader_account_id,
                    "symbolId": symbol_id,
                    "orderType": ProtoOAOrderType.MARKET,
                    "tradeSide": trade_side,
                    "volume": volume_units,
                    "comment": comment,
                    "label": client_order_id, # Use label for client order ID
                }
                if sl_price_rounded is not None:
                     order_params["stopLoss"] = sl_price_rounded
                if tp_price_rounded is not None:
                     order_params["takeProfit"] = tp_price_rounded
                # Add other params if needed: positionId (for closing/reducing), timeInForce, etc.

                request = MessagesFactory.ProtoOANewOrderReq(**order_params)
                # Response is ProtoOAExecutionEvent, handled by callback
                # The send() itself doesn't directly return the execution details
                # We need to wait for the callback _handle_execution_event
                await self._send_request(request)

                # --- How to confirm success? ---
                # We need to wait for the execution event callback to update the position state.
                # This requires a mechanism to correlate the request with the event.
                # Using the 'label' (client_order_id) might help if it's reflected in the event.
                # Let's wait a short period and check if a position was opened.
                await asyncio.sleep(CONFIG.get('timing', {}).get('order_confirmation_wait_s', 2.0)) # Wait for callback

                # Check internal state for the newly opened position (using comment/label if possible)
                async with self.position_lock:
                    # Search for a position matching symbol, side, volume, maybe comment/label
                    # This correlation is tricky and not guaranteed.
                    # A better way is needed if the API provides direct confirmation response.
                    # Checking the execution event callback is the primary method.
                    # This check here is a fallback/secondary confirmation.
                    found_position = None
                    for pos_id, pos_data in self.open_positions.items():
                         # Basic matching criteria (might match unrelated positions if opened concurrently)
                         if (pos_data.get('symbolId') == symbol_id and
                             pos_data.get('tradeSide') == trade_side and
                             pos_data.get('volume') == volume_units and # Check volume match
                             pos_data.get('comment') == comment):
                              found_position = pos_data
                              break

                if found_position:
                    pos_id = found_position['positionId']
                    logger.info(f"Market order for {symbol_name} appears successful (Position {pos_id} found).")
                    # Return a success-like dictionary, perhaps with the found position ID
                    return {
                        "result": True,
                        "message": "Order sent, position likely opened.",
                        "positionId": pos_id, # Include position ID if found
                        "volumeUnits": volume_units,
                        "executedLots": final_lots,
                        "entryPrice": found_position.get('entryPrice'), # If available
                         # Add order ID if available from event?
                         # Needs enhancement based on execution event handling
                    }
                else:
                    # If position not found after wait, assume failure for now
                    # The execution event callback might still report success later.
                    logger.warning(f"Market order confirmation for {symbol_name} not found in state after wait (Attempt {attempt}). Callback might still confirm.")
                    # Assume failure for this attempt, retry if applicable
                    # NOTE: This could lead to duplicate orders if the callback is just delayed!
                    # Careful design is needed here. Relying solely on callback is safer.
                    if attempt <= retries:
                         await asyncio.sleep(delay)
                         continue
                    else:
                         logger.error(f"Market order failed or confirmation delayed after {attempt} attempts.")
                         return {"result": False, "message": "Order failed or confirmation delayed.", "errorCode": "CONFIRMATION_TIMEOUT"}


            except Exception as e:
                 logger.error(f"Exception placing market order (Attempt {attempt}): {e}", exc_info=True)
                 if attempt <= retries:
                      await asyncio.sleep(delay)
                 else:
                      return {"result": False, "message": f"Exception: {e}", "errorCode": "EXCEPTION"}

        logger.error(f"Market order failed permanently after {retries + 1} attempts.")
        return {"result": False, "message": "Order failed after retries.", "errorCode": "RETRIES_EXCEEDED"}


    async def close_position(self, position_id: int, volume_lots: float) -> Optional[Dict[str, Any]]:
        """Closes part or all of an open position using LOTS."""
        if not CONFIG['execution']['enable_auto_trading']: return None
        if not await self.check_connection(): return None
        if not self._ctid_trader_account_id: return None

        # --- Volume Conversion ---
        volume_units = int(round(volume_lots * 100))
        if volume_units <= 0:
             logger.error(f"Invalid close volume: {volume_lots} lots ({volume_units} units)")
             return {"result": False, "message": "Invalid close volume"}

        logger.info(f"Attempting to close position {position_id}, Volume: {volume_units} units ({volume_lots:.2f} lots)")

        # --- Get Position Details for Validation (Optional but good practice) ---
        async with self.position_lock:
            position_to_close = self.open_positions.get(position_id)
        if position_to_close:
            symbol_id = position_to_close.get('symbolId')
            current_volume_units = position_to_close.get('volume', 0)
            if volume_units > current_volume_units:
                 logger.warning(f"Attempting to close {volume_units} units, but position {position_id} only has {current_volume_units}. Closing full position.")
                 volume_units = current_volume_units
            # Get symbol details for step validation
            details = self.get_symbol_details_by_id(symbol_id)
            if details:
                 step_vol_api = details.get('stepVolume', 1)
                 volume_units = (volume_units // step_vol_api) * step_vol_api # Ensure closes on step
                 if volume_units <= 0:
                      logger.error(f"Calculated close volume became zero after step rounding for position {position_id}.")
                      return {"result": False, "message": "Close volume zero after step rounding."}
            else: logger.warning(f"Could not get symbol details for step validation on position {position_id}")

        else:
             logger.warning(f"Position {position_id} not found in local cache for close validation.")
             # Proceed anyway, API will reject if invalid

        try:
            request = MessagesFactory.ProtoOAClosePositionReq(
                ctidTraderAccountId=self._ctid_trader_account_id,
                positionId=position_id,
                volume=volume_units
            )
            # Response is ProtoOAExecutionEvent, handled by callback
            await self._send_request(request)

            # Wait for confirmation via callback (similar issue as placing order)
            await asyncio.sleep(CONFIG.get('timing', {}).get('order_confirmation_wait_s', 2.0))

            # Check state again (callback should ideally handle removal)
            async with self.position_lock:
                position_still_open = position_id in self.open_positions
                partially_closed = False
                if position_still_open:
                     # If it still exists, check if volume decreased (partial close)
                     # This assumes the callback updated the volume correctly
                     if position_to_close and self.open_positions[position_id].get('volume', 0) < position_to_close.get('volume', 0):
                          partially_closed = True


            if not position_still_open or partially_closed:
                 status = "Partially Closed" if partially_closed else "Fully Closed"
                 logger.info(f"Close instruction for position {position_id} appears successful ({status}).")
                 return {"result": True, "message": f"Position {status}."}
            else:
                 # Could be delayed callback or actual failure
                 logger.warning(f"Position {position_id} still found in state after close attempt. Confirmation delayed or failed.")
                 # Returning success here is risky. Let's indicate potential issue.
                 return {"result": False, "message": "Close failed or confirmation delayed.", "errorCode": "CONFIRMATION_TIMEOUT"}

        except Exception as e:
            logger.error(f"Exception closing position {position_id}: {e}", exc_info=True)
            return {"result": False, "message": f"Exception: {e}", "errorCode": "EXCEPTION"}

    async def modify_position_sltp(self,
                                   position_id: int,
                                   sl_price: Optional[float] = None,
                                   tp_price: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """Modifies SL/TP of an open position by PRICE."""
        if not CONFIG['execution']['enable_auto_trading']: return None
        if not await self.check_connection(): return None
        if not self._ctid_trader_account_id: return None

        # --- Get Symbol Details for Rounding ---
        async with self.position_lock:
            position_to_modify = self.open_positions.get(position_id)
        if not position_to_modify:
             logger.warning(f"Position {position_id} not found in local cache for modification.")
             # Allow API call anyway, it will fail if invalid
             digits = 5 # Default guess
        else:
             digits = position_to_modify.get('digits', 5)

        sl_price_rounded = round(sl_price, digits) if sl_price is not None else None
        tp_price_rounded = round(tp_price, digits) if tp_price is not None else None

        if sl_price_rounded is None and tp_price_rounded is None:
             logger.warning("Modify SL/TP called with no changes requested.")
             return {"result": False, "message": "No SL/TP changes provided.", "errorCode": "NO_CHANGE"}

        logger.info(f"Attempting to modify SL/TP for position {position_id}: SL={sl_price_rounded}, TP={tp_price_rounded}")

        try:
            modify_params = {
                 "ctidTraderAccountId": self._ctid_trader_account_id,
                 "positionId": position_id
            }
            if sl_price_rounded is not None:
                modify_params["stopLoss"] = sl_price_rounded
            if tp_price_rounded is not None:
                 modify_params["takeProfit"] = tp_price_rounded

            request = MessagesFactory.ProtoOAAmendPositionSLTPReq(**modify_params)
            # Response is ProtoOAExecutionEvent, handled by callback
            await self._send_request(request)

            # Wait for confirmation?
            await asyncio.sleep(CONFIG.get('timing', {}).get('order_confirmation_wait_s', 1.5))

            # Verify change in state (callback should update SL/TP)
            async with self.position_lock:
                updated_position = self.open_positions.get(position_id)
                sl_updated = False
                tp_updated = False
                if updated_position:
                    if sl_price_rounded is not None and updated_position.get('slPrice') == sl_price_rounded:
                         sl_updated = True
                    if tp_price_rounded is not None and updated_position.get('tpPrice') == tp_price_rounded:
                         tp_updated = True

            # Check if the requested changes were applied
            modification_successful = True
            if sl_price_rounded is not None and not sl_updated: modification_successful = False
            if tp_price_rounded is not None and not tp_updated: modification_successful = False

            if modification_successful:
                logger.info(f"Modify SL/TP instruction sent and confirmed for position {position_id}.")
                return {"result": True, "message": "SL/TP modification confirmed."}
            else:
                logger.warning(f"SL/TP modification for position {position_id} failed or confirmation delayed.")
                return {"result": False, "message": "Modification failed or confirmation delayed.", "errorCode": "CONFIRMATION_TIMEOUT"}

        except Exception as e:
            logger.error(f"Exception modifying position {position_id}: {e}", exc_info=True)
            return {"result": False, "message": f"Exception: {e}", "errorCode": "EXCEPTION"}


    async def get_deal_history(self, from_timestamp_ms: int, to_timestamp_ms: int) -> List[Dict[str, Any]]:
        """
        Fetches deal history (fills, closures, balance changes) for the account within a time range.
        Required for accurate P/L calculation in PerformanceTracker.
        """
        if not self._ctid_trader_account_id: return []
        logger.info(f"Fetching deal history from {datetime.fromtimestamp(from_timestamp_ms/1000)} to {datetime.fromtimestamp(to_timestamp_ms/1000)}...")

        # API limitations: Max deals per request? Pagination needed?
        # ProtoOAGetDealListReq has maxCount parameter. Need pagination loop.
        all_deals = []
        max_count_per_req = 1000 # Check API limits
        current_from_ts = from_timestamp_ms

        while current_from_ts < to_timestamp_ms:
             logger.debug(f"Fetching deal batch from {current_from_ts}...")
             request = MessagesFactory.ProtoOAGetDealListReq(
                 ctidTraderAccountId=self._ctid_trader_account_id,
                 fromTimestamp=current_from_ts,
                 toTimestamp=to_timestamp_ms,
                 maxCount=max_count_per_req
             )
             response = await self._send_request(request)

             if response and hasattr(response, 'deal') and len(response.deal) > 0:
                 batch_deals = response.deal
                 all_deals.extend(batch_deals)
                 logger.debug(f"Fetched {len(batch_deals)} deals in this batch.")

                 if not response.hasMore: # Check if there are more deals beyond this batch
                      logger.debug("No more deals indicated by API.")
                      break # Exit loop

                 # Update 'from' timestamp for next iteration to the timestamp of the last deal + 1ms
                 # Ensure deals are sorted by timestamp? API usually returns them ordered.
                 last_deal_ts = batch_deals[-1].executionTimestamp
                 if last_deal_ts >= current_from_ts: # Prevent infinite loop if timestamps don't advance
                     current_from_ts = last_deal_ts + 1
                 else:
                     logger.warning("Deal timestamps did not advance, breaking history fetch loop to prevent issues.")
                     break

                 if len(batch_deals) < max_count_per_req:
                      logger.debug("Fetched less than max count, assuming end of deals for range.")
                      break # Exit loop if fewer than max returned

             elif response and hasattr(response, 'deal') and len(response.deal) == 0:
                  logger.debug("Fetched 0 deals in this batch. Assuming end of deals for range.")
                  break # No more deals found in this range
             else:
                  logger.error(f"Failed to fetch deal list batch or unexpected format: {response}")
                  # Should we return partial results or indicate failure?
                  return [] # Indicate failure for now

        logger.info(f"Total deals fetched: {len(all_deals)}")

        # Convert ProtoOADeal messages to dictionaries
        deal_dicts = [self._protobuf_deal_to_dict(deal) for deal in all_deals if deal]
        return [d for d in deal_dicts if d] # Filter out None results

    def _protobuf_deal_to_dict(self, deal_proto) -> Optional[Dict[str, Any]]:
        """Converts a ProtoOADeal protobuf message to a dictionary."""
        if not deal_proto: return None

        deal_dict = {
             "dealId": deal_proto.dealId,
             "orderId": deal_proto.orderId,
             "positionId": deal_proto.positionId,
             "volume": deal_proto.volume, # Volume in units (lots * 100)
             "filledVolume": deal_proto.filledVolume, # In units
             "tradeSide": deal_proto.tradeSide, # ProtoOATradeSide
             "createTimestamp": deal_proto.createTimestamp,
             "executionTimestamp": deal_proto.executionTimestamp,
             "utcLastUpdateTimestamp": deal_proto.utcLastUpdateTimestamp,
             "executionPrice": deal_proto.executionPrice, # Already scaled? Assume yes.
             "dealStatus": deal_proto.dealStatus, # ProtoOADealStatus
             "commission": deal_proto.commission / 100.0, # In cents
             "swap": deal_proto.swap / 100.0, # In cents
             "baseToUsdConversionRate": deal_proto.baseToUsdConversionRate if deal_proto.HasField("baseToUsdConversionRate") else None,
             "closePositionDetail": None, # Populate below if available
             "balance": deal_proto.balance / 100.0, # Account balance *after* this deal
             "comment": deal_proto.comment if deal_proto.HasField("comment") else None,
             "symbolId": deal_proto.symbolId # Added symbol ID
        }

        if deal_proto.HasField("closePositionDetail"):
             cpd = deal_proto.closePositionDetail
             deal_dict["closePositionDetail"] = {
                 "entryPrice": cpd.entryPrice, # Scaled? Assume yes.
                 "grossProfit": cpd.grossProfit / 100.0, # In cents
                 "swap": cpd.swap / 100.0, # In cents
                 "commission": cpd.commission / 100.0, # In cents
                 "balance": cpd.balance / 100.0, # Account balance after closure related changes
                 "quoteToDepositConversionRate": cpd.quoteToDepositConversionRate if cpd.HasField("quoteToDepositConversionRate") else None,
                 "closedVolume": cpd.closedVolume # In units
             }
             # Calculate Net P/L for this closing deal portion
             deal_dict["netPl"] = cpd.grossProfit + cpd.swap + cpd.commission # Already in cents, sum them up
             deal_dict["netPl"] /= 100.0 # Convert sum to account currency units

        return deal_dict


# ==============================================================================
# 3. DATA MANAGER CLASS (Largely unchanged, uses Connector's async methods)
# ==============================================================================
class DataManager:
    def __init__(self, connector: CTraderConnector, config: Dict[str, Any]):
        self.connector = connector
        self.config = config['logging']
        self.trading_config = config['trading']
        self.data_dir = self.config['data_dir']
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        self.historical_data: Dict[Tuple[str, str], pd.DataFrame] = {} # { (symbol, timeframe_str): df }
        self.last_fetch_time: Dict[Tuple[str, str], float] = {} # { (symbol, timeframe): timestamp }
        self.save_interval_seconds = 3600 # Save data approx every hour
        self.last_save_time: Dict[Tuple[str, str], float] = {} # { (symbol, timeframe): timestamp }
        self._lock = asyncio.Lock() # Lock for accessing shared data structures


    def _get_filepath(self, symbol: str, timeframe_str: str) -> str:
        filename = f"{symbol.upper()}_{timeframe_str.upper()}.csv"
        return os.path.join(self.data_dir, filename)

    async def load_initial_data(self, symbol: str, timeframe_str: str, num_bars: int) -> Optional[pd.DataFrame]:
        """Loads data from CSV if available/recent, otherwise fetches from API."""
        symbol = symbol.upper()
        timeframe_str = timeframe_str.upper()
        filepath = self._get_filepath(symbol, timeframe_str)
        df = None
        load_from_file = False

        async with self._lock: # Lock during file access and initial load
            try:
                if os.path.exists(filepath):
                    df = pd.read_csv(filepath, index_col='time', parse_dates=True)
                    # Ensure timezone is UTC
                    if df.index.tz is None: df.index = df.index.tz_localize('UTC')
                    else: df.index = df.index.tz_convert('UTC')

                    # Check if data is recent enough (e.g., last bar within reasonable window)
                    # Timeframe duration needed for better check
                    tf_seconds = self._get_tf_seconds(timeframe_str)
                    recency_threshold = timedelta(days=1) if tf_seconds >= 3600 else timedelta(hours=1) # Adjust threshold based on TF
                    if not df.empty and (datetime.now(pytz.utc) - df.index[-1] < recency_threshold):
                        logger.info(f"Loaded {len(df)} bars for {symbol} {timeframe_str} from file. Last bar: {df.index[-1]}")
                        load_from_file = True
                    else:
                        logger.info(f"Local data {symbol} {timeframe_str} is old/empty or doesn't meet recency. Fetching fresh.")
                        df = None # Force fetch
                else:
                    logger.info(f"No local file for {symbol} {timeframe_str}. Fetching.")
            except pd.errors.EmptyDataError:
                 logger.warning(f"Local file {filepath} is empty. Fetching fresh.")
                 df = None
            except Exception as e:
                logger.error(f"Error loading {filepath}: {e}. Fetching fresh.")
                df = None

            if df is None:
                df = await self.connector.get_historical_data(symbol, timeframe_str, num_bars)
                if df is not None and not df.empty:
                    self._save_data_sync(symbol, timeframe_str, df) # Save the newly fetched data (run save in executor?)
                elif df is None:
                     logger.error(f"Failed to fetch initial data for {symbol} {timeframe_str} from API.")
                     return None # Critical error if initial fetch fails
                # If df is empty DataFrame, still store it to avoid refetching immediately

            self.historical_data[(symbol, timeframe_str)] = df if df is not None else pd.DataFrame() # Store empty if fetch failed but returned empty
            self.last_fetch_time[(symbol, timeframe_str)] = time.time()
            return df

    def _get_tf_seconds(self, timeframe_str: str) -> int:
         """Approximate seconds per timeframe for recency check."""
         tf = timeframe_str.upper()
         if tf.startswith("M"): return int(tf[1:]) * 60
         if tf.startswith("H"): return int(tf[1:]) * 3600
         if tf.startswith("D"): return 86400
         if tf.startswith("W"): return 86400 * 7
         if tf.startswith("MN"): return 86400 * 30
         return 60 # Default M1


    async def update_data(self, symbol: str, timeframe_str: str, num_bars_to_maintain: int) -> Optional[pd.DataFrame]:
        """Fetches the latest bars and appends/merges with existing data."""
        symbol = symbol.upper()
        timeframe_str = timeframe_str.upper()
        key = (symbol, timeframe_str)

        async with self._lock: # Lock during update process
            if key not in self.historical_data:
                logger.warning(f"No existing data for {symbol} {timeframe_str} to update. Loading initial.")
                # Call load_initial_data without lock as it handles its own locking
                # Need to release lock before calling? No, load_initial_data acquires same lock.
                 return await self.load_initial_data(symbol, timeframe_str, num_bars_to_maintain)


            existing_df = self.historical_data[key]
            if existing_df is None or existing_df.empty:
                 logger.warning(f"Existing DF empty for {symbol} {timeframe_str}. Reloading.")
                 return await self.load_initial_data(symbol, timeframe_str, num_bars_to_maintain)

            # Fetch recent bars - Number depends on expected gaps + buffer
            # Fetching ~30 should cover most minor connection issues
            fetch_count = 30 # How many bars to request in the update call
            logger.debug(f"Updating data for {symbol} {timeframe_str}...")
            # Need to release lock to call connector? No, connector is async.
            new_df = await self.connector.get_historical_data(symbol, timeframe_str, fetch_count)

            if new_df is None: # API call failed
                logger.warning(f"Failed to fetch update data for {symbol} {timeframe_str}. Using existing.")
                # Still update fetch time to avoid constant retries if API is down
                self.last_fetch_time[key] = time.time()
                return existing_df # Return the old data
            elif new_df.empty: # API call succeeded but returned no new bars
                 logger.debug(f"No new bars received for {symbol} {timeframe_str}.")
                 self.last_fetch_time[key] = time.time()
                 return existing_df

            # Combine, remove duplicates (keeping newest), sort, and trim
            try:
                # Ensure new_df index is also UTC (should be from connector)
                if new_df.index.tz is None: new_df.index = new_df.index.tz_localize('UTC')
                else: new_df.index = new_df.index.tz_convert('UTC')

                combined_df = pd.concat([existing_df, new_df])
                # Keep last occurence of duplicate indices (timestamps)
                combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                combined_df.sort_index(inplace=True)
                trimmed_df = combined_df.tail(num_bars_to_maintain)

                # Check if data actually changed
                if not existing_df.equals(trimmed_df):
                    self.historical_data[key] = trimmed_df
                    logger.debug(f"Data updated for {symbol} {timeframe_str}. New length: {len(trimmed_df)}")

                    # Save periodically
                    now = time.time()
                    if now - self.last_save_time.get(key, 0) > self.save_interval_seconds:
                         self._save_data_sync(symbol, timeframe_str, trimmed_df) # Run save
                         self.last_save_time[key] = now
                else:
                     logger.debug(f"No change in data for {symbol} {timeframe_str} after update.")


                self.last_fetch_time[key] = time.time()
                return self.historical_data[key] # Return potentially updated df

            except Exception as e:
                 logger.error(f"Error merging dataframes for {symbol} {timeframe_str}: {e}", exc_info=True)
                 return existing_df # Return old data on merge error

    def get_data(self, symbol: str, timeframe_str: str) -> Optional[pd.DataFrame]:
        """Returns the currently stored dataframe (synchronous)."""
        # Read access under lock? Probably safer.
        # async with self._lock: # Cannot await
        return self.historical_data.get((symbol.upper(), timeframe_str.upper()))

    def _save_data_sync(self, symbol: str, timeframe_str: str, df: pd.DataFrame):
        """Saves the dataframe to a CSV file (synchronous internal helper)."""
        if df is None or df.empty:
            logger.warning(f"Attempted to save empty dataframe for {symbol} {timeframe_str}")
            return
        filepath = self._get_filepath(symbol, timeframe_str)
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            df.to_csv(filepath)
            logger.info(f"Saved data ({len(df)} bars) for {symbol} {timeframe_str} to {filepath}")
        except Exception as e:
            logger.error(f"Error saving data to {filepath}: {e}")

    async def save_all_data(self):
        """Saves all currently held dataframes."""
        logger.info("Saving all cached historical data...")
        # This should run in an executor thread pool to avoid blocking the event loop
        loop = asyncio.get_running_loop()
        async with self._lock: # Lock while iterating keys
            save_tasks = []
            for (symbol, timeframe_str), df in self.historical_data.items():
                # Run synchronous save function in executor
                task = loop.run_in_executor(None, self._save_data_sync, symbol, timeframe_str, df.copy())
                save_tasks.append(task)
            if save_tasks:
                await asyncio.gather(*save_tasks)
            logger.info("Finished saving all data.")



# --- Continuation from Part 2 ---

# ==============================================================================
# 4. TECHNICAL ANALYZER CLASS (Using pandas-ta)
# ==============================================================================
class TechnicalAnalyzer:
    def __init__(self, connector: CTraderConnector, config: Dict[str, Any]):
        self.connector = connector # May need it for symbol details if not passed in data
        self.config = config.get('technical_analysis', {}) # Use TA specific config section
        self.trading_config = config['trading']
        if not self.config:
             logger.warning("Technical Analysis configuration section missing.")
             self.config = {} # Provide empty dict to avoid errors

    def calculate_indicators(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Calculates technical indicators using pandas_ta."""
        if df is None or df.empty:
            logger.debug("TA: DataFrame is empty, skipping indicator calculation.")
            return df # Return empty/none as is

        # Ensure required columns exist
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_cols):
            logger.warning(f"TA: DataFrame missing required columns ({required_cols}). Have: {df.columns.tolist()}")
            # Attempt to fill missing OHLC with close? Risky. Return None for safety.
            return None

        # Drop rows with NaNs in essential columns *before* calculating indicators
        df_cleaned = df.dropna(subset=required_cols).copy()
        if df_cleaned.empty:
             logger.warning("TA: DataFrame is empty after dropping NaNs in OHLCV.")
             return df_cleaned

        cfg = self.config # shortcut

        # --- Determine required length based on config ---
        # Use get() with defaults for safety if config is incomplete
        required_len = 1 # Start with 1
        try:
            required_len = max(required_len,
                           cfg.get('ema_short', 9), cfg.get('ema_medium', 21), cfg.get('ema_slow', 50), cfg.get('ema_trend', 200),
                           cfg.get('rsi_period', 14), cfg.get('stoch_k', 14), cfg.get('stoch_d', 3), # smooth_k handled by stoch
                           cfg.get('macd_fast', 12), cfg.get('macd_slow', 26), cfg.get('macd_signal', 9), # slow is max length needed
                           cfg.get('bbands_period', 20), cfg.get('atr_period', 14),
                           cfg.get('volume_avg_period', 20), cfg.get('fractal_lookback', 2)*2+1 )
        except TypeError as e:
             logger.error(f"TA: Invalid configuration value detected: {e}. Using default lengths.")
             # Use default calculation for required_len if config is bad
             required_len = max(9, 21, 50, 200, 14, 14, 26, 20, 14, 20, 2*2+1)

        if len(df_cleaned) < required_len:
            logger.warning(f"TA: DataFrame too short ({len(df_cleaned)} bars, need {required_len}) for all indicators.")
            # Calculate what we can? Or return original? Let's return original + warning.
            return df # Return original df with potential NaNs

        logger.debug(f"TA: Calculating indicators for DataFrame with {len(df_cleaned)} rows.")
        try:
            # Create a copy to avoid modifying the original DataFrame passed in
            df_out = df_cleaned.copy()

            # Use config values for periods safely with .get()
            # EMA requires close price
            df_out.ta.ema(length=cfg.get('ema_short', 9), append=True)
            df_out.ta.ema(length=cfg.get('ema_medium', 21), append=True)
            df_out.ta.ema(length=cfg.get('ema_slow', 50), append=True)
            df_out.ta.ema(length=cfg.get('ema_trend', 200), append=True)

            # RSI
            df_out.ta.rsi(length=cfg.get('rsi_period', 14), append=True)

            # Stochastic (k, d, smooth_k specified directly)
            df_out.ta.stoch(k=cfg.get('stoch_k', 14), d=cfg.get('stoch_d', 3), smooth_k=cfg.get('stoch_smooth', 3), append=True)

            # MACD
            df_out.ta.macd(fast=cfg.get('macd_fast', 12), slow=cfg.get('macd_slow', 26), signal=cfg.get('macd_signal', 9), append=True)

            # ATR (requires high, low, close)
            df_out.ta.atr(length=cfg.get('atr_period', 14), append=True)

            # Bollinger Bands
            df_out.ta.bbands(length=cfg.get('bbands_period', 20), std=cfg.get('bbands_stddev', 2.0), append=True)

            # Volume SMA (requires volume)
            # Check if volume column is numeric and has non-zero variance
            if pd.api.types.is_numeric_dtype(df_out['volume']) and df_out['volume'].nunique() > 1:
                 # Prefix with VOL_SMA to avoid potential conflicts if other SMAs are added
                 df_out.ta.sma(close='volume', length=cfg.get('volume_avg_period', 20), prefix="VOL", append=True)
            else:
                 logger.warning("TA: Volume data non-numeric or constant, skipping Volume SMA.")
                 df_out[f"VOL_SMA_{cfg.get('volume_avg_period', 20)}"] = np.nan # Add NaN column

            # Patterns
            self._detect_fractals(df_out, n=cfg.get('fractal_lookback', 2))
            self._detect_candlesticks(df_out)

            # Add back rows with NaNs that were dropped, keeping calculated indicators
            # Reindex df_out to match the original df index, then update original df
            # This preserves rows that might only have OHLC for signal checks later
            # df_merged = df.copy() # Start with original
            # df_merged.update(df_out) # Update with calculated indicators where possible

            # Alternative: Return only the cleaned df with indicators
            logger.debug(f"TA: Indicators calculated. Last row:\n{df_out.iloc[-1].to_string()}")
            return df_out # Return the DataFrame with indicators calculated

        except Exception as e:
            logger.error(f"TA: Error calculating indicators: {e}", exc_info=True)
            return None # Return None on critical error during calculation

    def _detect_fractals(self, df: pd.DataFrame, n: int):
        """Detects simple High/Low Fractals using pandas_ta helper (or rolling window)."""
        # pandas_ta doesn't have a direct fractal indicator AFAIK. Manual implementation:
        if n <= 0: return # Lookback must be positive
        try:
            # Shift required to check n bars left and n bars right
            win_size = 2 * n + 1
            # Find local max/min within the rolling window
            local_max = df['high'].rolling(window=win_size, center=True, min_periods=win_size).max()
            local_min = df['low'].rolling(window=win_size, center=True, min_periods=win_size).min()

            # A high fractal occurs at the center index 'i' if high[i] == local_max[i]
            # A low fractal occurs at the center index 'i' if low[i] == local_min[i]
            # Need to shift the result because rolling(center=True) aligns result at window center
            df['fractal_high'] = np.where(df['high'] == local_max, df['high'], np.nan)
            df['fractal_low'] = np.where(df['low'] == local_min, df['low'], np.nan)

            # Shift result to align with the bar where the fractal is confirmed (n bars later)
            # No, fractal is defined at the bar itself. No shift needed if center=True was used.
            # df['fractal_high'] = is_high_fractal.shift(n) # Old logic based on non-centered window?

            logger.debug("Fractals calculated.")
        except Exception as e:
            logger.error(f"Error calculating fractals: {e}")
            df['fractal_high'] = np.nan
            df['fractal_low'] = np.nan


    def find_swing_points(self, df: pd.DataFrame, lookback: int=30) -> Tuple[List[float], List[float]]:
        """Identifies recent swing high/low points using calculated fractals."""
        if df is None or df.empty or 'fractal_high' not in df.columns or 'fractal_low' not in df.columns:
            return [], []

        relevant_df = df.tail(lookback * 3) # Look back further to capture swings before the immediate lookback
        # Drop NaNs and get unique values (optional, depends if multiple bars can have same fractal value)
        highs = relevant_df['fractal_high'].dropna().unique()
        lows = relevant_df['fractal_low'].dropna().unique()

        # Return the last 'lookback' number of *unique* swing points found within the relevant window
        return highs[-lookback:].tolist(), lows[-lookback:].tolist()

    def _detect_candlesticks(self, df: pd.DataFrame):
        """Detects basic candlestick patterns using pandas_ta."""
        try:
            # Select patterns (refer to pandas_ta documentation for available patterns)
            # Using common ones: engulfing, doji, hammer, shootingstar, morningstar, eveningstar
            patterns = ["engulfing", "doji", "hammer", "shootingstar", "morningstar", "eveningstar"]
            df.ta.cdl_pattern(name=patterns, append=True)

            # Create boolean columns for easier use in signal generation
            # Pattern results are typically 100 (bullish), -100 (bearish), or 0 (none)
            for pattern in patterns:
                 col_name = f"CDL_{pattern.upper()}"
                 if col_name in df.columns:
                      # Generic bullish/bearish flags
                      df[f'cdl_{pattern}_bull'] = df[col_name] > 0
                      df[f'cdl_{pattern}_bear'] = df[col_name] < 0
                      # Specific flags if needed (e.g., engulfing)
                      if pattern == "engulfing":
                           df['bullish_engulfing'] = df[col_name] == 100
                           df['bearish_engulfing'] = df[col_name] == -100
                      if pattern == "hammer": df['hammer'] = df[col_name] == 100
                      if pattern == "shootingstar": df['shooting_star'] = df[col_name] == -100
                      if pattern == "doji": df['doji'] = df[col_name] != 0

            logger.debug("Candlestick patterns calculated.")
        except AttributeError:
             logger.warning("Candlestick pattern calculation skipped (likely old pandas_ta version or missing 'talib'). Install TA-Lib and retry.")
        except Exception as e:
             logger.error(f"Error calculating candlestick patterns: {e}")

    def get_trend(self, df_trend_tf: pd.DataFrame) -> str:
        """Determines the major trend using EMAs on the higher timeframe."""
        if df_trend_tf is None or df_trend_tf.empty: return "Undetermined"

        cfg = self.config
        ema_slow_col = f"EMA_{cfg.get('ema_slow', 50)}"
        ema_trend_col = f"EMA_{cfg.get('ema_trend', 200)}"

        if ema_slow_col not in df_trend_tf.columns or ema_trend_col not in df_trend_tf.columns:
            logger.warning(f"Trend Check: Missing required EMA columns ({ema_slow_col}, {ema_trend_col}) in trend DF.")
            return "Undetermined"
        if len(df_trend_tf) < 2:
             logger.warning("Trend Check: Not enough bars in trend DF.")
             return "Undetermined"

        # Use last fully closed bar for trend assessment
        last_closed = df_trend_tf.iloc[-2]
        ema_slow = last_closed.get(ema_slow_col)
        ema_trend = last_closed.get(ema_trend_col)

        if pd.isna(ema_slow) or pd.isna(ema_trend):
             logger.debug("Trend Check: EMA values are NaN on last closed bar.")
             return "Undetermined"

        if ema_slow > ema_trend: return "Uptrend"
        elif ema_slow < ema_trend: return "Downtrend"
        else: return "Sideways"

    def get_entry_timing(self, df_entry_tf: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
         """Checks short-term EMAs for entry timing signals on last closed bar."""
         if df_entry_tf is None or df_entry_tf.empty: return None, None

         cfg = self.config
         ema_short_col = f"EMA_{cfg.get('ema_short', 9)}"
         ema_medium_col = f"EMA_{cfg.get('ema_medium', 21)}"

         if ema_short_col not in df_entry_tf.columns or ema_medium_col not in df_entry_tf.columns:
             logger.warning(f"Entry Timing: Missing required EMA columns ({ema_short_col}, {ema_medium_col}) in entry DF.")
             return None, None
         if len(df_entry_tf) < 3:
              logger.warning("Entry Timing: Not enough bars in entry DF.")
              return None, None

         # Use last two closed bars for crossover check
         prev_closed = df_entry_tf.iloc[-3]
         last_closed = df_entry_tf.iloc[-2]

         ema_s_prev = prev_closed.get(ema_short_col)
         ema_m_prev = prev_closed.get(ema_medium_col)
         ema_s_last = last_closed.get(ema_short_col)
         ema_m_last = last_closed.get(ema_medium_col)

         if pd.isna(ema_s_last) or pd.isna(ema_m_last) or pd.isna(ema_s_prev) or pd.isna(ema_m_prev):
             logger.debug("Entry Timing: EMA values are NaN on recent bars.")
             return None, None

         crossed_bullish = ema_s_prev <= ema_m_prev and ema_s_last > ema_m_last
         crossed_bearish = ema_s_prev >= ema_m_prev and ema_s_last < ema_m_last

         # Determine alignment based on the *last* closed bar
         aligned_bullish = ema_s_last > ema_m_last
         aligned_bearish = ema_s_last < ema_m_last

         if crossed_bullish: return "Crossover", "Bullish"
         elif crossed_bearish: return "Crossover", "Bearish"
         elif aligned_bullish: return "Aligned", "Bullish"
         elif aligned_bearish: return "Aligned", "Bearish"
         else: return "Indeterminate", None


# ==============================================================================
# 5. FUNDAMENTAL ANALYZER CLASS (Largely unchanged)
# ==============================================================================
class FundamentalAnalyzer:
    def __init__(self, config: Dict[str, Any]):
        self.config = config.get('fundamental_analysis', {})
        self.enabled = self.config.get('enable', False) and NEWSAPI_ENABLED
        self.newsapi = None
        self.newsapi_key = self.config.get('newsapi_key')
        self.last_news_check_time = 0
        self.recent_headlines: List[Dict] = []
        self.market_sentiment = "Neutral" # Neutral, Bullish, Bearish
        self.high_impact_event_detected = False
        self.high_impact_event_time: Optional[datetime] = None
        self.volatility_period_active = False
        self.volatility_period_end_time: Optional[datetime] = None
        # DXY/Economic Calendar placeholders
        self.dxy_correlation = None # Placeholder
        self.economic_calendar_events: List[Dict] = [] # Placeholder for calendar events
        self.last_calendar_check_time = 0

        if self.enabled:
            if not self.newsapi_key:
                logger.error("NewsAPI enabled but Key missing. Disabling FA.")
                self.enabled = False
            else:
                try:
                    self.newsapi = NewsApiClient(api_key=self.newsapi_key)
                    logger.info("NewsApiClient initialized.")
                except NameError:
                    logger.error("NewsApiClient class not available (newsapi library import failed?). Disabling FA.")
                    self.enabled = False
                except Exception as e:
                    logger.error(f"Failed to initialize NewsApiClient: {e}. Disabling FA.")
                    self.enabled = False
        else:
            logger.info("Fundamental Analysis (NewsAPI) is disabled.")

        # Economic Calendar specific settings
        self.calendar_enabled = self.config.get('enable_economic_calendar', False)
        self.calendar_check_interval_min = self.config.get('calendar_check_interval_minutes', 60)
        self.calendar_impact_threshold = self.config.get('calendar_impact_threshold', ['high']) # e.g., ['high', 'medium']
        self.calendar_volatility_window_before_min = self.config.get('calendar_volatility_window_before_min', 15)
        self.calendar_volatility_window_after_min = self.config.get('calendar_volatility_window_after_min', 30)

    async def fetch_and_analyze(self):
        """Fetches and analyzes both news and economic calendar data."""
        now_utc = datetime.now(timezone.utc)
        now_ts = time.time()

        # --- News Analysis ---
        if self.enabled:
            news_check_interval = self.config.get('news_check_interval_minutes', 30) * 60
            if now_ts - self.last_news_check_time >= news_check_interval:
                 await self._fetch_newsapi_data(now_utc)
                 self.last_news_check_time = now_ts

        # --- Economic Calendar Analysis ---
        if self.calendar_enabled:
             calendar_check_interval = self.calendar_check_interval_min * 60
             if now_ts - self.last_calendar_check_time >= calendar_check_interval:
                  await self._fetch_economic_calendar_data() # Placeholder
                  self.last_calendar_check_time = now_ts

        # --- Update Volatility State ---
        self._update_volatility_status(now_utc)


    async def _fetch_newsapi_data(self, now_utc: datetime):
        """Internal method to fetch and process news from NewsAPI."""
        logger.info("Fetching news from NewsAPI...")
        sentiment_score = 0
        headlines_count = 0
        new_high_impact_event = False
        detected_event_details = None
        detected_event_time = None
        cfg = self.config

        try:
             from_datetime = now_utc - timedelta(hours=cfg.get('news_lookback_hours', 6))
             from_iso = from_datetime.isoformat()

             # Run blocking NewsAPI call in an executor thread
             loop = asyncio.get_running_loop()
             all_articles = await loop.run_in_executor(
                 None, # Default executor
                 self.newsapi.get_everything, # Blocking function
                 # Arguments for the function:
                 cfg.get('newsapi_query_keywords', 'forex OR gold OR XAUUSD OR dollar OR fed OR inflation OR rates'),
                 cfg.get('newsapi_sources', 'reuters,associated-press,bloomberg,financial-post'),
                 cfg.get('newsapi_language', 'en'),
                 cfg.get('newsapi_sort_by', 'publishedAt'), # 'relevancy' or 'publishedAt'
                 cfg.get('newsapi_page_size', 20),
                 from_iso
             )

             if all_articles['status'] != 'ok':
                 logger.error(f"NewsAPI request failed: {all_articles.get('code')} - {all_articles.get('message')}")
                 return

             total_results = all_articles.get('totalResults', 0)
             fetched_count = len(all_articles.get('articles', []))
             logger.info(f"NewsAPI returned {total_results} articles (fetched {fetched_count}).")
             self.recent_headlines = all_articles.get('articles', [])

             # Define keywords safely using .get()
             bullish_kws = set(cfg.get('sentiment_keywords_bullish', ['positive', 'strong', 'rally', 'up', 'rise', 'boom']))
             bearish_kws = set(cfg.get('sentiment_keywords_bearish', ['negative', 'weak', 'slump', 'down', 'fall', 'crisis']))
             high_impact_kws = set(cfg.get('high_impact_news_keywords', ['breaking', 'alert', 'urgent', 'war', 'crash', 'disaster']))

             for article in self.recent_headlines:
                 title = (article.get('title') or "").lower()
                 description = (article.get('description') or "").lower()
                 content = title + " " + description
                 published_at_str = article.get('publishedAt', '')
                 published_dt = None
                 if published_at_str:
                     try: published_dt = datetime.fromisoformat(published_at_str.replace('Z', '+00:00')).astimezone(timezone.utc)
                     except ValueError: pass # Ignore parse errors

                 # Sentiment Analysis (simple keyword counting)
                 bullish_count = sum(1 for k in bullish_kws if k in content)
                 bearish_count = sum(1 for k in bearish_kws if k in content)
                 if bullish_count > bearish_count: sentiment_score += 1
                 elif bearish_count > bullish_count: sentiment_score -= 1
                 headlines_count += 1

                 # High Impact News Check (recent publication time)
                 if published_dt:
                     time_since = now_utc - published_dt
                     # Check if published within twice the check interval (e.g., last hour if checking every 30 mins)
                     if abs(time_since) < timedelta(minutes=cfg.get('news_check_interval_minutes', 30) * 2):
                        if any(k in content for k in high_impact_kws):
                            # Prioritize this news event if no other high-impact event is active
                            if not self.high_impact_event_detected or (self.high_impact_event_time and published_dt > self.high_impact_event_time):
                                logger.warning(f"!!! High-Impact News Keyword Match: '{title}' (Published: {published_at_str}) !!!")
                                new_high_impact_event = True
                                detected_event_details = article
                                detected_event_time = published_dt
                                # Don't break, process all recent news for sentiment, but keep latest high-impact time

             # Update Market Sentiment State
             if headlines_count > 0:
                 avg_sentiment = sentiment_score / headlines_count
                 thresh = cfg.get('sentiment_score_threshold', 0.1) # Threshold for Bullish/Bearish sentiment
                 if avg_sentiment > thresh: self.market_sentiment = "Bullish"
                 elif avg_sentiment < -thresh: self.market_sentiment = "Bearish"
                 else: self.market_sentiment = "Neutral"
                 logger.info(f"News Sentiment Analysis: Score={sentiment_score}/{headlines_count}, Average={avg_sentiment:.2f}, Overall: {self.market_sentiment}")
             else:
                 self.market_sentiment = "Neutral" # Default if no headlines processed

             # Activate Volatility Period based on latest detected *news* event
             if new_high_impact_event and detected_event_time:
                  self._activate_volatility_period(
                      event_time=detected_event_time,
                      event_type="News",
                      event_details=detected_event_details,
                      window_after_min=cfg.get('news_volatility_window_after_min', 30),
                      window_before_min=0 # News is typically reactive
                  )

        except Exception as e:
            logger.error(f"Error during NewsAPI fetch/analysis: {e}", exc_info=True)

    async def _fetch_economic_calendar_data(self):
         """Placeholder for fetching economic calendar data."""
         if not self.calendar_enabled: return
         logger.info("Fetching economic calendar data... (Placeholder - Not Implemented)")
         # --- Implementation Notes ---
         # 1. Choose a source: Investing.com (scraping - fragile), ForexFactory (scraping), paid API (Finnhub, etc.)
         # 2. Implement fetching logic (consider async HTTP requests with aiohttp).
         # 3. Parse the data: Extract event time (UTC), currency, impact level, event name.
         # 4. Filter by impact level (e.g., 'high', 'medium') and relevant currencies (e.g., 'USD' for XAUUSD).
         # 5. Store upcoming relevant events in self.economic_calendar_events (list of dicts).
         # 6. Check timestamps against current time in _update_volatility_status.

         # Example dummy event for testing volatility period activation:
         dummy_event_time = datetime.now(timezone.utc) + timedelta(minutes=10)
         dummy_event = {
              "time_utc": dummy_event_time,
              "currency": "USD",
              "impact": "high",
              "name": "Dummy High Impact Event (Test)"
         }
         self.economic_calendar_events = [dummy_event] # Replace with real fetched data
         logger.debug(f"Economic calendar updated (using dummy data). Found {len(self.economic_calendar_events)} relevant events.")


    def _activate_volatility_period(self, event_time: datetime, event_type: str, event_details: Any, window_before_min: int, window_after_min: int):
         """Activates the volatility period based on an event."""
         if self.volatility_period_active:
              # If already active, extend the end time if this event is later
              new_end_time = event_time + timedelta(minutes=window_after_min)
              if not self.volatility_period_end_time or new_end_time > self.volatility_period_end_time:
                   logger.info(f"Extending existing volatility period due to {event_type} event until ~{new_end_time.strftime('%H:%M:%S %Z')}")
                   self.volatility_period_end_time = new_end_time
                   # Update high impact event details if this one is more significant/later?
                   self.high_impact_event_time = event_time
                   # Send update notification?
                   # if notifier: notifier.send_message(f"Volatility period extended by {event_type} event: {event_details.get('title', event_details)}")
              return # Don't reactivate if already active

         # Activate new period
         self.high_impact_event_detected = True
         self.high_impact_event_time = event_time
         start_time = event_time - timedelta(minutes=window_before_min)
         end_time = event_time + timedelta(minutes=window_after_min)
         self.volatility_period_end_time = end_time
         self.volatility_period_active = True # Set active flag

         logger.warning(f"!!! High-Impact {event_type} Event Detected !!!")
         logger.warning(f"Activating volatility period: {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M %Z')}")
         logger.warning(f"Event Details: {event_details}") # Log details (might be dict or string)

         # Send Alert (assuming notifier is initialized globally)
         if notifier:
             # Format message based on type
             msg = f"⚠️ *High-Impact {event_type} Event Alert* ⚠️\n\n"
             if isinstance(event_details, dict) and 'title' in event_details: # NewsAPI structure
                  msg += f"*{event_details.get('title', '')}*\n"
                  msg += f"Source: {event_details.get('source', {}).get('name', 'N/A')}\n"
                  msg += f"Published: {event_details.get('publishedAt', '')}\n"
             elif isinstance(event_details, dict): # Calendar structure (example)
                  msg += f"*{event_details.get('name', 'N/A')}* ({event_details.get('currency', '')})\n"
                  msg += f"Impact: {event_details.get('impact', 'N/A')}\n"
                  msg += f"Scheduled Time: {event_time.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
             else: msg += f"{event_details}\n"
             msg += f"\nTrading paused/restricted until approx: *{end_time.strftime('%H:%M:%S %Z')}*"
             notifier.send_message(msg) # Use global notifier instance

    def _update_volatility_status(self, now_utc: datetime):
         """Checks active volatility periods and upcoming calendar events."""

         # 1. Check if current news-based volatility period has ended
         if self.volatility_period_active and self.volatility_period_end_time and now_utc > self.volatility_period_end_time:
             logger.info(f"Volatility period ended at {self.volatility_period_end_time.strftime('%H:%M:%S %Z')}. Resuming normal operations.")
             self.volatility_period_active = False
             self.high_impact_event_detected = False
             self.high_impact_event_time = None
             self.volatility_period_end_time = None
             if notifier: notifier.send_message("✅ Volatility period ended. Trading resumed.")

         # 2. Check upcoming economic calendar events
         if self.calendar_enabled and not self.volatility_period_active: # Only check if not already paused
              for event in self.economic_calendar_events:
                   event_time = event.get('time_utc')
                   if not isinstance(event_time, datetime): continue # Skip invalid events

                   volatility_start_time = event_time - timedelta(minutes=self.calendar_volatility_window_before_min)
                   # Check if we are within the "before" window of an upcoming event
                   if volatility_start_time <= now_utc <= event_time :
                        logger.info(f"Approaching high-impact calendar event: {event.get('name')}")
                        self._activate_volatility_period(
                             event_time=event_time,
                             event_type="Calendar",
                             event_details=event,
                             window_before_min=self.calendar_volatility_window_before_min,
                             window_after_min=self.calendar_volatility_window_after_min
                        )
                        break # Activate for the first upcoming event found

    def is_volatility_period(self) -> bool:
        """Returns true if the bot should avoid trading due to volatility."""
        # Check the flag, _update_volatility_status should keep it current
        return self.volatility_period_active


# ==============================================================================
# 6. RISK MANAGER CLASS (Adapted for cTrader Volume Units & Pip Value)
# ==============================================================================
class RiskManager:
    def __init__(self, connector: CTraderConnector, config: Dict[str, Any]):
        self.connector = connector
        self.config = config.get('risk', {}) # Risk specific config
        self.trading_config = config['trading']

    def calculate_position_size_lots(self, # Now calculates LOTS
                                 account_balance: float,
                                 account_currency: str,
                                 stop_loss_pips: float,
                                 symbol_name: str) -> Optional[float]:
        """Calculates position size in LOTS, respecting lot constraints."""
        symbol_name = symbol_name.upper()
        if account_balance <= 0 or stop_loss_pips <= 0:
            logger.warning(f"Risk Calc: Invalid balance ({account_balance}) or SL pips ({stop_loss_pips}).")
            return None

        # Get Symbol Details from Connector (Crucial for Lot Size & Pip Value)
        symbol_details = self.connector.get_symbol_details(symbol_name) # Sync getter
        if not symbol_details:
            # Cannot await here. Assume details were fetched earlier or rely on defaults.
            logger.error(f"Risk Calc: Missing symbol details for {symbol_name}. Cannot calculate size.")
            # TODO: Maybe trigger an async fetch here and return None for now?
            return None

        # --- Extract required details ---
        pip_position = symbol_details.get('pipPosition')
        digits = symbol_details.get('digits')
        # Volume details from symbol are in API units (lots*100), convert min/max/step to LOTS for comparison
        min_lots_symbol = symbol_details.get('minVolume', 1) / 100.0 # Min lots allowed by symbol
        max_lots_symbol = symbol_details.get('maxVolume', 100000) / 100.0 # Max lots allowed by symbol
        step_lots_symbol = symbol_details.get('stepVolume', 1) / 100.0 # Lot step allowed by symbol

        if None in [pip_position, digits, min_lots_symbol, max_lots_symbol, step_lots_symbol]:
             logger.error(f"Risk Calc: Crucial symbol details missing for {symbol_name}.")
             return None

        # 1. Calculate Risk Amount in Account Currency
        risk_percent = self.trading_config.get('risk_per_trade_percent', 1.0)
        risk_amount = account_balance * (risk_percent / 100.0)
        logger.debug(f"Risk Calc: Balance=${account_balance:.2f}, Risk%={risk_percent}, RiskAmt=${risk_amount:.2f}, SL Pips={stop_loss_pips:.1f}")


        # 2. Calculate Pip Value per Lot in Account Currency
        value_per_pip_per_lot = self._get_pip_value_per_lot(symbol_details, account_currency)
        if value_per_pip_per_lot is None or value_per_pip_per_lot <= 0:
            logger.error(f"Risk Calc: Failed to determine pip value per lot for {symbol_name} -> {account_currency}.")
            return None
        logger.debug(f"Risk Calc: Pip value per lot = {value_per_pip_per_lot:.5f} {account_currency}")


        # 3. Calculate desired LOTS based on risk
        # risk_amount = stop_loss_pips * value_per_pip_per_lot * desired_lots
        desired_lots = risk_amount / (stop_loss_pips * value_per_pip_per_lot)
        logger.debug(f"Risk Calc: Desired lots (raw) = {desired_lots:.4f}")


        # 4. Apply Min/Max LOT Constraints (from Config first)
        min_lots_config = self.trading_config.get('min_trade_lots', 0.01)
        max_lots_config = self.trading_config.get('max_trade_lots', 10.0)

        # Effective min/max is the stricter of config and symbol limits
        effective_min_lots = max(min_lots_config, min_lots_symbol)
        effective_max_lots = min(max_lots_config, max_lots_symbol)

        if desired_lots < effective_min_lots:
             logger.warning(f"Risk Calc: Calculated lots {desired_lots:.4f} below effective minimum {effective_min_lots:.4f} (Config: {min_lots_config}, Symbol: {min_lots_symbol}). Clamping to min.")
             constrained_lots = effective_min_lots
        elif desired_lots > effective_max_lots:
             logger.warning(f"Risk Calc: Calculated lots {desired_lots:.4f} above effective maximum {effective_max_lots:.4f} (Config: {max_lots_config}, Symbol: {max_lots_symbol}). Clamping to max.")
             constrained_lots = effective_max_lots
        else:
             constrained_lots = desired_lots
        logger.debug(f"Risk Calc: Constrained lots (min/max) = {constrained_lots:.4f}")

        # 5. Apply Lot Step Constraint
        if step_lots_symbol > 0:
            # Round *down* to the nearest valid lot step
            final_lots = (constrained_lots // step_lots_symbol) * step_lots_symbol
            # Ensure precision issues don't make it zero if it shouldn't be
            final_lots = round(final_lots, 8) # Use generous rounding for steps
            logger.debug(f"Risk Calc: Lots after step rounding ({step_lots_symbol}) = {final_lots:.4f}")
        else:
            logger.warning(f"Risk Calc: Symbol {symbol_name} has step lots = 0. Cannot apply step constraint.")
            final_lots = constrained_lots


        # 6. Final Check: Ensure size is still within effective min/max after step rounding
        if final_lots < effective_min_lots:
            logger.error(f"Risk Calc: Final lots {final_lots:.4f} is below effective minimum {effective_min_lots:.4f} after constraints/rounding. Cannot trade.")
            return None
        if final_lots > effective_max_lots: # Should only happen due to rounding issues if clamped before step
            logger.warning(f"Risk Calc: Final lots {final_lots:.4f} slightly above effective max {effective_max_lots:.4f} after rounding. Clamping again.")
            final_lots = effective_max_lots


        logger.info(f"Risk Calc: Calculated Position Size: {final_lots:.2f} lots")
        return final_lots

    def _get_pip_value_per_lot(self, symbol_details: Dict[str, Any], account_currency: str) -> Optional[float]:
        """
        Calculates the value of 1 pip for 1 LOT of the symbol in the account currency.
        Requires accurate symbol details and potentially exchange rates.
        """
        symbol_name = symbol_details.get('symbolName')
        pip_position = symbol_details.get('pipPosition')
        digits = symbol_details.get('digits')
        lot_size = symbol_details.get('lotSize') # Base units per lot (e.g., 100000 EUR in EURUSD)
        quote_asset_id = symbol_details.get('quoteAssetId') # e.g., USD in EURUSD
        base_asset_id = symbol_details.get('baseAssetId') # e.g., EUR in EURUSD

        if None in [symbol_name, pip_position, digits, lot_size, quote_asset_id, base_asset_id]:
             logger.error(f"Pip Value Calc: Missing critical details for {symbol_name}.")
             return None

        # 1. Determine Pip Size in Quote Currency
        pip_size_quote_ccy = 10**(-pip_position) # e.g., for EURUSD (pip pos 4), 0.0001 USD

        # 2. Determine Value of 1 Pip per Lot in Quote Currency
        # For direct pairs (XXX/YYY where YYY is quote): Value = pip_size * lot_size
        # For Metals (XAU/USD): Pip size (0.1) * Lot size (100 oz) = 10 USD per pip per lot
        # General formula seems to be: Value = pip_size * lot_size
        value_per_pip_per_lot_quote_ccy = pip_size_quote_ccy * lot_size
        logger.debug(f"Pip Value Calc [{symbol_name}]: PipSize={pip_size_quote_ccy}, LotSize={lot_size} -> ValueInQuoteCcy={value_per_pip_per_lot_quote_ccy}")


        # 3. Convert to Account Currency
        quote_currency = self.connector._get_currency_from_asset_id(quote_asset_id) # Use internal helper (needs improvement)
        if not quote_currency:
            logger.error(f"Pip Value Calc: Could not determine quote currency for asset ID {quote_asset_id}.")
            return None

        if quote_currency == account_currency:
            logger.debug(f"Pip Value Calc: Quote currency ({quote_currency}) matches account currency. No conversion needed.")
            return value_per_pip_per_lot_quote_ccy
        else:
            # --- Currency Conversion Needed ---
            # We need the exchange rate: QuoteCurrency / AccountCurrency
            # Example: Symbol=EURJPY (Quote=JPY), Account=USD. Need JPY/USD rate.
            # Example: Symbol=AUDCAD (Quote=CAD), Account=USD. Need CAD/USD rate.
            conversion_pair = f"{quote_currency}{account_currency}"
            inverse_pair = f"{account_currency}{quote_currency}"
            rate = None

            logger.debug(f"Pip Value Calc: Need conversion rate for {conversion_pair} or {inverse_pair}")

            # Try to get rate from tick data (requires subscription to conversion pair)
            conv_details = self.connector.get_symbol_details(conversion_pair)
            inv_details = self.connector.get_symbol_details(inverse_pair)

            if conv_details:
                 tick = self.connector.get_current_tick(conversion_pair)
                 if tick:
                      rate = (tick['bid'] + tick['ask']) / 2.0 # Use midpoint price
                      logger.debug(f"Pip Value Calc: Found rate for {conversion_pair} = {rate}")
                      value_in_account_ccy = value_per_pip_per_lot_quote_ccy * rate
                      return value_in_account_ccy
            elif inv_details:
                 tick = self.connector.get_current_tick(inverse_pair)
                 if tick:
                      mid_price = (tick['bid'] + tick['ask']) / 2.0
                      if mid_price != 0:
                           rate = 1.0 / mid_price # Use inverse rate
                           logger.debug(f"Pip Value Calc: Found inverse rate for {inverse_pair} = {mid_price}, using rate = {rate}")
                           value_in_account_ccy = value_per_pip_per_lot_quote_ccy * rate
                           return value_in_account_ccy
                      else: logger.warning(f"Pip Value Calc: Mid price for {inverse_pair} is zero.")
                 else: logger.warning(f"Pip Value Calc: No tick data for conversion pair {inverse_pair}")
            else: logger.warning(f"Pip Value Calc: Symbol details not found for conversion pairs {conversion_pair} or {inverse_pair}")


            # --- Fallback / TODO ---
            logger.error(f"Pip Value Calc: Conversion rate from {quote_currency} to {account_currency} not available via ticks. Accurate calculation failed.")
            # Options:
            # 1. Subscribe to necessary pairs.
            # 2. Use an external FX rate API (adds complexity/delay).
            # 3. Store approximate rates in config (inaccurate).
            # Return None to prevent trading with wrong size.
            return None


    def calculate_sl_tp(self, entry_price: float, signal_type: str, atr_pips: float, symbol_name: str) -> Tuple[Optional[float], Optional[float], List[float], Optional[float]]:
        """Calculates Stop Loss and TP levels by PRICE."""
        symbol_name = symbol_name.upper()
        symbol_details = self.connector.get_symbol_details(symbol_name) # Sync getter
        if not symbol_details:
            logger.error(f"SL/TP Calc: Missing symbol details for {symbol_name}.")
            return None, None, [], None # SL price, SL pips, TP levels list, Est Risk/Lot

        pip_position = symbol_details.get('pipPosition')
        digits = symbol_details.get('digits')

        if None in [pip_position, digits] or atr_pips <= 0:
            logger.error(f"SL/TP Calc: Invalid details or ATR ({atr_pips}) for {symbol_name}.")
            return None, None, [], None

        pip_value_price = 10**(-pip_position) # Price change for 1 pip

        # --- Stop Loss ---
        sl_atr_mult = self.config.get('sl_atr_multiplier', 1.5)
        sl_distance_pips = atr_pips * sl_atr_mult
        sl_distance_price = sl_distance_pips * pip_value_price
        stop_loss_price = entry_price - sl_distance_price if signal_type == "BUY" else entry_price + sl_distance_price
        stop_loss_price = round(stop_loss_price, digits)

        # --- Take Profit Levels ---
        take_profit_levels = []
        risk_pips = sl_distance_pips # SL distance defines the risk (1R)
        if risk_pips <= 0:
             logger.error(f"SL/TP Calc: Calculated risk pips is zero or negative ({risk_pips:.1f}).")
             return None, None, [], None

        min_rr = self.trading_config.get('min_rr_ratio', 1.5)
        num_tp = self.trading_config.get('tp_levels', 1)
        rr_step = self.trading_config.get('rr_step_increase', 0.5) # How much RR increases per TP level

        for i in range(1, num_tp + 1):
            # Example R:R progression: TP1=min_rr, TP2=min_rr+step, TP3=min_rr+2*step ...
            tp_ratio = min_rr + (rr_step * (i - 1))
            tp_distance_pips = risk_pips * tp_ratio
            tp_distance_price = tp_distance_pips * pip_value_price
            tp_price = entry_price + tp_distance_price if signal_type == "BUY" else entry_price - tp_distance_price
            take_profit_levels.append(round(tp_price, digits))

        # Estimate risk per lot - uses internal function
        account_currency = self.connector.get_account_currency() or "USD" # Fallback
        est_pip_value_per_lot = self._get_pip_value_per_lot(symbol_details, account_currency)
        est_risk_per_lot = (sl_distance_pips * est_pip_value_per_lot) if est_pip_value_per_lot else 0.0

        logger.debug(f"SL/TP Calc: Entry={entry_price:.{digits}f}, SL={stop_loss_price:.{digits}f} ({sl_distance_pips:.1f} pips), TPs={take_profit_levels}, Est Risk/Lot~${est_risk_per_lot:.2f} {account_currency}")
        return stop_loss_price, sl_distance_pips, take_profit_levels, est_risk_per_lot

    def calculate_trailing_stop(self,
                                position_data: Dict[str, Any],
                                current_tick: Dict[str, Any],
                                entry_atr_pips: float) -> Optional[float]:
        """Calculates the new trailing stop loss PRICE level based on ATR."""
        if not self.config.get('enable_trailing_stop', False) or not position_data or not current_tick or entry_atr_pips <= 0:
            return None

        symbol_name = position_data.get('symbolName')
        if not symbol_name:
             logger.warning(f"Cannot calc TS: Missing symbol name for position {position_data.get('positionId')}")
             return None

        # Use details stored within the position dict if available (added in _protobuf_position_to_dict)
        pip_position = position_data.get('pipPosition') # Assumes it's stored
        digits = position_data.get('digits')

        if pip_position is None or digits is None:
             # Fallback to fetching details if not stored in position data
             symbol_details = self.connector.get_symbol_details(symbol_name)
             if not symbol_details or symbol_details.get('pipPosition') is None or symbol_details.get('digits') is None:
                 logger.warning(f"Cannot calc TS for {symbol_name}: Missing symbol details (pipPosition/digits).")
                 return None
             pip_position = symbol_details['pipPosition']
             digits = symbol_details['digits']

        position_id = position_data['positionId']
        open_price = position_data['entryPrice']
        current_sl_price = position_data.get('slPrice') # SL might not be set initially
        position_type_enum = position_data['tradeSide'] # ProtoOATradeSide

        pip_value_price = 10**(-pip_position)

        # Determine current price relevant for profit calculation
        current_price = current_tick['bid'] if position_type_enum == ProtoOATradeSide.BUY else current_tick['ask']

        # Calculate profit in pips
        profit_pips = 0.0
        if position_type_enum == ProtoOATradeSide.BUY:
            profit_pips = (current_price - open_price) / pip_value_price if pip_value_price > 0 else 0.0
        else: # SELL
            profit_pips = (open_price - current_price) / pip_value_price if pip_value_price > 0 else 0.0

        # --- Trailing Stop Logic ---
        activation_pips = entry_atr_pips * self.config.get('ts_atr_multiplier_activate', 1.0) # e.g., activate after 1*ATR profit
        if profit_pips < activation_pips:
            # logger.debug(f"TS {position_id}: Profit {profit_pips:.1f} < Activation {activation_pips:.1f}. No trail.")
            return None # Not enough profit to activate trailing stop

        trail_distance_pips = entry_atr_pips * self.config.get('ts_atr_multiplier_trail', 1.5) # e.g., trail 1.5*ATR behind price
        trail_distance_price = trail_distance_pips * pip_value_price
        new_tsl_level = 0.0
        breakeven_buffer_pips = self.config.get('ts_breakeven_buffer_pips', 1.0) # Pips beyond BE

        if position_type_enum == ProtoOATradeSide.BUY:
            # Proposed new SL is current price minus trail distance
            proposed_sl = current_price - trail_distance_price
            # Minimum SL should be break-even plus a small buffer
            breakeven_sl = open_price + (breakeven_buffer_pips * pip_value_price)
            # Ensure TSL only moves up
            min_sl_target = breakeven_sl
            if current_sl_price is not None:
                 min_sl_target = max(min_sl_target, current_sl_price) # Must be higher than current SL (or BE)
            # New SL is the higher of the proposed SL and the minimum target
            new_tsl_level = max(proposed_sl, min_sl_target)

        else: # SELL
            # Proposed new SL is current price plus trail distance
            proposed_sl = current_price + trail_distance_price
            # Minimum SL should be break-even minus a small buffer (higher price for SELL)
            breakeven_sl = open_price - (breakeven_buffer_pips * pip_value_price)
            # Ensure TSL only moves down
            max_sl_target = breakeven_sl
            if current_sl_price is not None:
                 max_sl_target = min(max_sl_target, current_sl_price) # Must be lower than current SL (or BE)
            # New SL is the lower of the proposed SL and the maximum target
            new_tsl_level = min(proposed_sl, max_sl_target)

        new_tsl_level = round(new_tsl_level, digits)

        # Only return a new level if it's different from the current SL
        # Handle case where current_sl_price is None
        if current_sl_price is None:
             if new_tsl_level != round(open_price, digits): # Check against open price if no SL exists? Or just apply if activation met.
                  logger.info(f"Trailing Stop Update Triggered for {position_id}: New SL = {new_tsl_level:.{digits}f} (Prev: None)")
                  return new_tsl_level
             else: return None
        elif new_tsl_level != round(current_sl_price, digits):
            logger.info(f"Trailing Stop Update Triggered for {position_id}: New SL = {new_tsl_level:.{digits}f} (Prev: {current_sl_price:.{digits}f})")
            return new_tsl_level
        else:
            # logger.debug(f"TS {position_id}: Proposed TSL {new_tsl_level:.{digits}f} same as current. No update.")
            return None # No update needed


# ==============================================================================
# 7. SIGNAL GENERATOR CLASS (Uses Config, expects Connector)
# ==============================================================================
class SignalGenerator:
    def __init__(self, tech_analyzer: TechnicalAnalyzer, risk_manager: RiskManager,
                 fundamental_analyzer: FundamentalAnalyzer, connector: CTraderConnector,
                 config: Dict[str, Any]):
        self.tech_analyzer = tech_analyzer
        self.risk_manager = risk_manager
        self.fundamental_analyzer = fundamental_analyzer
        self.connector = connector
        self.config = config # Full config
        self.trading_config = config['trading']
        self.ta_config = config.get('technical_analysis', {})
        self.fa_config = config.get('fundamental_analysis', {})
        self.last_signal_time = 0
        self.signal_cooldown_seconds = config.get('timing', {}).get('signal_cooldown_seconds', 60 * 1) # Cooldown between signals

    def generate_signal(self, data_trend: pd.DataFrame, data_entry: pd.DataFrame, current_tick: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # --- Safety & Pre-checks ---
        if data_trend is None or data_entry is None or current_tick is None:
            logger.debug("Signal gen skipped: Missing data.")
            return None
        if self.fundamental_analyzer.is_volatility_period():
            logger.info("Signal generation skipped: Active fundamental volatility period.")
            return None

        symbol = self.trading_config['symbol']
        if current_tick.get('symbol') != symbol:
             logger.warning(f"Signal gen: Tick symbol '{current_tick.get('symbol')}' doesn't match config '{symbol}'. Skipping.")
             return None

        # Spread Check (using tick data)
        max_spread_pips = self.trading_config.get('max_spread_pips', 5.0) # Configurable max spread
        current_spread_pips = current_tick.get('spread_pips', max_spread_pips + 1) # Default high if missing
        if current_spread_pips > max_spread_pips:
             logger.warning(f"Signal gen skipped: Spread too high ({current_spread_pips:.1f} pips > {max_spread_pips}).")
             return None

        # Cooldown Check
        if time.time() - self.last_signal_time < self.signal_cooldown_seconds:
             logger.debug("Signal gen skipped: Cooldown active.")
             return None

        # --- Extract Latest Data (Focus on last closed bar, maybe current for trigger?) ---
        try:
            if len(data_trend) < 2 or len(data_entry) < 3: # Need at least 2 for trend, 3 for entry timing/momentum compare
                logger.warning("Signal gen skipped: Not enough historical bars for analysis.")
                return None
            last_trend_closed = data_trend.iloc[-2]
            last_entry_closed = data_entry.iloc[-2]
            prev_entry_closed = data_entry.iloc[-3]
            # current_entry_bar = data_entry.iloc[-1] # Potentially use for pattern confirmation on developing bar?
        except IndexError:
            logger.warning("Signal gen skipped: DataFrame index error accessing recent bars.")
            return None
        except Exception as e:
            logger.error(f"Signal gen error accessing DF rows: {e}", exc_info=True)
            return None

        # --- Initialize Signal Variables ---
        signal_type = None  # "BUY" or "SELL"
        confidence = 0.0    # Confidence score
        factors = []        # List of contributing factors

        # --- Get Symbol Details ---
        symbol_details = self.connector.get_symbol_details(symbol)
        if not symbol_details:
             logger.error(f"Signal gen: Cannot get symbol details for {symbol}. Aborting.")
             return None
        digits = symbol_details.get('digits', 5)
        pip_position = symbol_details.get('pipPosition', 4)

        # --- 1. Trend Analysis (Higher Timeframe) ---
        trend_direction = self.tech_analyzer.get_trend(data_trend)
        if trend_direction == "Uptrend": confidence += 1.0; factors.append("H1 Trend: Up")
        elif trend_direction == "Downtrend": confidence += 1.0; factors.append("H1 Trend: Down")
        else: factors.append(f"H1 Trend: {trend_direction}")

        # --- 2. Entry Timing & Price Action (Entry Timeframe - Closed Bar) ---
        timing_signal, timing_dir = self.tech_analyzer.get_entry_timing(data_entry)
        ema_alignment_factor = ""
        if timing_signal == "Crossover":
            if timing_dir == "Bullish" and trend_direction == "Uptrend":
                 signal_type = "BUY"; confidence += 2.0; factors.append("M5 Entry: Bullish EMA Cross")
            elif timing_dir == "Bearish" and trend_direction == "Downtrend":
                 signal_type = "SELL"; confidence += 2.0; factors.append("M5 Entry: Bearish EMA Cross")
            else: # Crossover against trend - ignore or reduce confidence?
                 factors.append(f"M5 Entry: {timing_dir} EMA Cross (Against Trend)")
                 confidence -= 0.5
        elif timing_signal == "Aligned":
             # Check if EMAs are aligned with the major trend
             if timing_dir == "Bullish" and trend_direction == "Uptrend":
                 if signal_type is None: signal_type = "BUY" # Tentative signal
                 confidence += 0.5; ema_alignment_factor = "M5 Entry: EMAs Aligned Bullish (w/ Trend)"
             elif timing_dir == "Bearish" and trend_direction == "Downtrend":
                 if signal_type is None: signal_type = "SELL" # Tentative signal
                 confidence += 0.5; ema_alignment_factor = "M5 Entry: EMAs Aligned Bearish (w/ Trend)"
             else:
                 ema_alignment_factor = f"M5 Entry: EMAs Aligned {timing_dir} (Against Trend)"
                 confidence -= 0.5 # Reduce confidence if aligned against trend
             if ema_alignment_factor: factors.append(ema_alignment_factor)

        # Candlestick Patterns (on last closed entry bar)
        if signal_type == "BUY" or (trend_direction == "Uptrend" and signal_type is None):
             if last_entry_closed.get('bullish_engulfing'): confidence += 1.5; factors.append("M5 PA: Bullish Engulfing")
             if last_entry_closed.get('hammer'): confidence += 1.0; factors.append("M5 PA: Hammer")
             # if last_entry_closed.get('cdl_morningstar_bull'): confidence += 1.5; factors.append("M5 PA: Morning Star")
        elif signal_type == "SELL" or (trend_direction == "Downtrend" and signal_type is None):
             if last_entry_closed.get('bearish_engulfing'): confidence += 1.5; factors.append("M5 PA: Bearish Engulfing")
             if last_entry_closed.get('shooting_star'): confidence += 1.0; factors.append("M5 PA: Shooting Star")
             # if last_entry_closed.get('cdl_eveningstar_bear'): confidence += 1.5; factors.append("M5 PA: Evening Star")

        # If only alignment gave a tentative signal, require stronger PA/momentum
        if signal_type and "Aligned" in (ema_alignment_factor or "") and confidence < 2.5: # Threshold tunable
            logger.debug("Signal based only on alignment is weak, discarding.")
            signal_type = None; confidence = 0.0; factors = [] # Reset


        # --- 3. Momentum Confirmation (Entry Timeframe - Closed Bar) ---
        # Column names from pandas_ta (check defaults if config missing)
        rsi_col = f"RSI_{self.ta_config.get('rsi_period', 14)}"
        stochk_col = f"STOCHk_{self.ta_config.get('stoch_k', 14)}_{self.ta_config.get('stoch_d', 3)}_{self.ta_config.get('stoch_smooth', 3)}"
        stochd_col = f"STOCHd_{self.ta_config.get('stoch_k', 14)}_{self.ta_config.get('stoch_d', 3)}_{self.ta_config.get('stoch_smooth', 3)}"
        macdh_col = f"MACDh_{self.ta_config.get('macd_fast', 12)}_{self.ta_config.get('macd_slow', 26)}_{self.ta_config.get('macd_signal', 9)}"

        # Get values safely using .get() on the Series
        rsi = last_entry_closed.get(rsi_col)
        prev_rsi = prev_entry_closed.get(rsi_col)
        stoch_k = last_entry_closed.get(stochk_col)
        prev_stoch_k = prev_entry_closed.get(stochk_col)
        stoch_d = last_entry_closed.get(stochd_col)
        macd_h = last_entry_closed.get(macdh_col)
        prev_macd_h = prev_entry_closed.get(macdh_col)

        # Check if all momentum values are valid numbers
        momentum_values = [rsi, prev_rsi, stoch_k, prev_stoch_k, stoch_d, macd_h, prev_macd_h]
        if any(pd.isna(v) for v in momentum_values):
             logger.debug("Momentum indicators contain NaN on recent bars.")
             factors.append("Momentum: Inconclusive (NaNs)")
        else:
             rsi_ob = self.ta_config.get('rsi_overbought', 70)
             rsi_os = self.ta_config.get('rsi_oversold', 30)
             stoch_ob = self.ta_config.get('stoch_overbought', 80)
             stoch_os = self.ta_config.get('stoch_oversold', 20)

             if signal_type == "BUY":
                 # RSI: Rising and above 50 (but not overbought?)
                 if rsi > 50 and rsi < rsi_ob and rsi > prev_rsi: confidence += 1.0; factors.append("M5 RSI: Rising > 50")
                 # Stochastic: Exiting OS or K > D and not OB
                 if (prev_stoch_k < stoch_os and stoch_k > stoch_os) or (stoch_k > stoch_d and stoch_k < stoch_ob): confidence += 1.0; factors.append("M5 Stoch: Bullish")
                 # MACD: Histogram positive and increasing
                 if macd_h > 0 and macd_h > prev_macd_h: confidence += 1.0; factors.append("M5 MACD: Histo Positive/Increasing")
             elif signal_type == "SELL":
                 # RSI: Falling and below 50 (but not oversold?)
                 if rsi < 50 and rsi > rsi_os and rsi < prev_rsi: confidence += 1.0; factors.append("M5 RSI: Falling < 50")
                 # Stochastic: Exiting OB or K < D and not OS
                 if (prev_stoch_k > stoch_ob and stoch_k < stoch_ob) or (stoch_k < stoch_d and stoch_k > stoch_os): confidence += 1.0; factors.append("M5 Stoch: Bearish")
                 # MACD: Histogram negative and decreasing
                 if macd_h < 0 and macd_h < prev_macd_h: confidence += 1.0; factors.append("M5 MACD: Histo Negative/Decreasing")

        # --- 4. Volume Confirmation (Entry Timeframe - Closed Bar) ---
        vol_sma_col = f"VOL_SMA_{self.ta_config.get('volume_avg_period', 20)}"
        if 'volume' in last_entry_closed and vol_sma_col in last_entry_closed:
             last_vol = last_entry_closed.get('volume')
             avg_vol = last_entry_closed.get(vol_sma_col)
             if pd.notna(last_vol) and pd.notna(avg_vol) and avg_vol > 0:
                 vol_factor = self.ta_config.get('volume_spike_factor', 1.5)
                 if last_vol > avg_vol * vol_factor:
                      confidence += 0.5 # Lower impact?
                      factors.append(f"M5 Volume: Spike (> {vol_factor:.1f}x Avg)")
             else: logger.debug("Volume or Avg Volume is NaN/Zero.")
        else: logger.debug(f"Volume columns ('volume', '{vol_sma_col}') not found.")


        # --- 5. S/R Check (Basic Fractal Check - Optional) ---
        # Check if entry price is near a recent fractal low (for BUY) or high (for SELL)
        # Could add more sophisticated S/R detection here
        # ...

        # --- 6. Fundamental Context ---
        if self.fundamental_analyzer.enabled or self.fundamental_analyzer.calendar_enabled:
             fa_sentiment = self.fundamental_analyzer.market_sentiment
             if self.fa_config.get('enable_news_sentiment_bias', True):
                 if signal_type == "BUY" and fa_sentiment == "Bullish": confidence += 0.5; factors.append("FA Sentiment: Bullish")
                 elif signal_type == "SELL" and fa_sentiment == "Bearish": confidence += 0.5; factors.append("FA Sentiment: Bearish")
                 # Optionally penalize if sentiment opposes signal
                 elif signal_type == "BUY" and fa_sentiment == "Bearish": confidence -= 1.0; factors.append("FA Sentiment: Bearish (Opposes BUY)")
                 elif signal_type == "SELL" and fa_sentiment == "Bullish": confidence -= 1.0; factors.append("FA Sentiment: Bullish (Opposes SELL)")
                 elif fa_sentiment != "Neutral": factors.append(f"FA Sentiment: {fa_sentiment}")


        # --- 7. Signal Strength & Final Check ---
        if signal_type is None:
            logger.debug("Signal gen: No signal type determined after checks.")
            return None

        # Define confidence thresholds in config?
        min_confidence = self.trading_config.get('min_signal_confidence', 4.0) # Tunable threshold
        if confidence < min_confidence:
            logger.info(f"Signal {signal_type} discarded: Confidence ({confidence:.1f}) below threshold ({min_confidence}). Factors: {factors}")
            return None

        # Determine strength label
        strength = "Weak" # Default if only meets minimum
        strong_thresh = self.trading_config.get('strong_signal_confidence', 6.0)
        moderate_thresh = self.trading_config.get('moderate_signal_confidence', min_confidence) # Moderate = meets min
        if confidence >= strong_thresh: strength = "Strong"
        elif confidence >= moderate_thresh: strength = "Moderate"


        # --- 8. Define Entry, SL, TP using Risk Manager ---
        # Get ATR from the last closed entry bar
        atr_col = f"ATRr_{self.ta_config.get('atr_period', 14)}" # pandas_ta default name
        atr_value_price = last_entry_closed.get(atr_col)

        if pd.isna(atr_value_price) or atr_value_price <= 0:
            logger.error(f"Cannot finalize signal: Invalid ATR ({atr_value_price}) on last closed bar."); return None

        # Convert ATR price value to pips
        pip_value_price = 10**(-pip_position) if pip_position is not None else 0.0
        if pip_value_price <= 0: logger.error("Cannot finalize signal: Invalid pip value."); return None
        atr_pips = atr_value_price / pip_value_price

        # Ideal entry is current market price from tick
        entry_price_ideal = current_tick['ask'] if signal_type == "BUY" else current_tick['bid']

        # Calculate SL/TP Prices and SL Pips using RiskManager
        sl_price, sl_pips, tp_levels, est_risk_per_lot = self.risk_manager.calculate_sl_tp(
            entry_price=entry_price_ideal, signal_type=signal_type, atr_pips=atr_pips, symbol_name=symbol
        )
        if sl_price is None or not tp_levels or sl_pips is None or sl_pips <= 0:
            logger.error("Signal gen: SL/TP calculation failed."); return None

        # --- 9. Calculate Position Size (LOTS) ---
        account_balance = self.connector.get_account_balance()
        account_currency = self.connector.get_account_currency()
        if account_balance is None or account_currency is None:
            logger.error("Signal gen: Account details missing for size calculation."); return None

        position_size_lots = self.risk_manager.calculate_position_size_lots(
            account_balance, account_currency, sl_pips, symbol
        )
        if position_size_lots is None or position_size_lots <= 0:
            logger.error("Signal gen: Position sizing failed or resulted in zero lots."); return None

        # Estimate actual risk for this trade size
        actual_sl_risk_usd = 0.0
        pip_val_lot = self.risk_manager._get_pip_value_per_lot(symbol_details, account_currency)
        if pip_val_lot: actual_sl_risk_usd = sl_pips * pip_val_lot * position_size_lots

        # --- 10. Construct Signal Dictionary ---
        signal_id = f"{symbol[:3]}{signal_type[0]}_{pd.Timestamp.now(tz='UTC').strftime('%H%M%S%f')[:-3]}"
        signal_data = {
            "id": signal_id,
            "timestamp_utc": datetime.now(timezone.utc),
            "symbol": symbol,
            "type": signal_type, # "BUY" or "SELL"
            "entry_ideal": round(entry_price_ideal, digits),
            "stop_loss_price": sl_price,
            "sl_pips": round(sl_pips, 1),
            "take_profit_prices": tp_levels, # List of TP prices
            "strength": strength,
            "confidence_score": round(confidence, 1),
            "position_size_lots": round(position_size_lots, 2), # Size in LOTS
            "sl_risk_account_ccy": round(actual_sl_risk_usd, 2), # Use account currency
            "account_currency": account_currency,
            "signal_tf_str": self.trading_config['timeframe_entry'],
            "trend_tf_str": self.trading_config['timeframe_trend'],
            "factors": factors,
            "digits": digits, # Store digits for formatting
            "atr_at_entry_pips": round(atr_pips, 1), # Store ATR for potential trailing stop use
            "status": "New" # Status: New, Sent, Executed, Failed, Closed
        }

        logger.info(f"Generated Signal: {signal_id} ({strength}, Score: {confidence:.1f}) {signal_type} {position_size_lots:.2f} lots")
        self.last_signal_time = time.time() # Update cooldown timer
        return signal_data


# ==============================================================================
# 8. BOT STATE CLASS (Manages active signals and positions)
# ==============================================================================
class BotState:
    def __init__(self, config: Dict[str, Any]):
        self.config = config['logging']
        self.state_file = os.path.join(self.config['data_dir'], self.config['state_file'])
        self.active_signals: Dict[str, Dict] = {} # { signal_id: signal_data }
        self.open_positions: Dict[int, Dict] = {} # { position_id: position_details_dict } - Synced from connector
        self.last_save_time = 0
        self._lock = asyncio.Lock() # Lock for saving/loading state
        self._load_state() # Load synchronously on init

    def _load_state(self):
        # Run synchronously during initialization
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f: state = json.load(f)
                self.active_signals = state.get('active_signals', {})
                # Positions loaded here might be stale, rely on sync from connector
                # self.open_positions = state.get('open_positions', {}) # Commented out - sync should overwrite
                # Convert timestamps back to datetime objects
                for sig_data in self.active_signals.values():
                     ts_str = sig_data.get('timestamp_utc')
                     if isinstance(ts_str, str):
                          try: sig_data['timestamp_utc'] = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
                          except ValueError: logger.warning(f"Could not parse timestamp for signal {sig_data.get('id')}")
                logger.info(f"Loaded bot state. {len(self.active_signals)} active signals found.")
            except json.JSONDecodeError as e:
                 logger.error(f"Error loading state file {self.state_file} (JSON invalid): {e}. Starting fresh state.")
                 self.active_signals = {}; self.open_positions = {}
            except Exception as e:
                logger.error(f"Error loading state file {self.state_file}: {e}. Starting fresh state.")
                self.active_signals = {}; self.open_positions = {}
        else: logger.info(f"State file {self.state_file} not found. Starting fresh state.")

    async def save_state(self):
        """Saves the current bot state asynchronously."""
        now = time.time()
        # Limit saving frequency
        if now - self.last_save_time < 60: return # Save max once per min

        async with self._lock: # Ensure atomic save
            logger.debug("Saving bot state...")
            try:
                # Create serializable copies
                serializable_signals = {}
                for sig_id, sig_data in self.active_signals.items():
                    s_copy = sig_data.copy()
                    # Convert datetime to ISO string
                    ts_dt = s_copy.get('timestamp_utc')
                    if isinstance(ts_dt, datetime):
                         s_copy['timestamp_utc'] = ts_dt.isoformat()
                    serializable_signals[sig_id] = s_copy

                # Store open positions as currently known (synced from connector)
                serializable_positions = self.open_positions.copy()

                state_data = {
                     'active_signals': serializable_signals,
                     'open_positions': serializable_positions # Store positions as dict
                }

                # Run synchronous file I/O in an executor
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._write_state_sync, state_data)

                self.last_save_time = now
                logger.debug("Saved bot state.")
            except Exception as e:
                 logger.error(f"Error saving bot state: {e}", exc_info=True)

    def _write_state_sync(self, state_data: Dict):
         """Synchronous helper to write state to file."""
         try:
             os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
             with open(self.state_file, 'w') as f:
                  json.dump(state_data, f, indent=2)
         except Exception as e:
              logger.error(f"Error writing state file {self.state_file}: {e}")


    async def add_signal(self, signal_data: Dict):
        if signal_data and 'id' in signal_data:
            async with self._lock: # Lock during modification
                self.active_signals[signal_data['id']] = signal_data
                logger.info(f"Added signal {signal_data['id']} (Status: {signal_data['status']}).")
            await self.save_state() # Persist change

    async def update_signal_status(self, signal_id: str, status: str, execution_result: Optional[Dict] = None):
        updated = False
        async with self._lock: # Lock during modification
            if signal_id in self.active_signals:
                 self.active_signals[signal_id]['status'] = status
                 if execution_result and execution_result.get('result'):
                      # Store IDs/details from successful execution if available in result
                      # This depends heavily on what `place_market_order` returns on success
                      self.active_signals[signal_id]['position_id'] = execution_result.get('positionId')
                      self.active_signals[signal_id]['order_id'] = execution_result.get('orderId') # May not be available immediately
                      self.active_signals[signal_id]['fill_price'] = execution_result.get('entryPrice') # May not be available immediately
                 logger.info(f"Updated signal {signal_id} status to: {status}")
                 updated = True
            else:
                 logger.warning(f"Tried to update status for unknown signal ID: {signal_id}")

        if updated:
            await self.save_state()

    async def remove_signal(self, signal_id: str):
        removed = False
        async with self._lock: # Lock during modification
            if signal_id in self.active_signals:
                del self.active_signals[signal_id]
                logger.info(f"Removed signal {signal_id} from active state.")
                removed = True
        if removed:
            await self.save_state()

    def get_signal(self, signal_id: str) -> Optional[Dict]:
        # Synchronous read, assumes dict access is safe or lock is handled elsewhere if needed
        return self.active_signals.get(signal_id)

    def get_active_signals_count(self) -> int:
        # Synchronous read
        return len(self.active_signals)

    async def sync_open_positions(self, current_api_positions: List[Dict]) -> List[Dict]:
        """ Updates internal state based on list of position dicts from connector."""
        closed_positions_data = []
        new_pos_count = 0
        closed_pos_count = 0
        update_count = 0
        state_changed = False

        # Use lock to ensure consistency during sync
        async with self._lock:
             # Get IDs from API data (ensure positionId exists)
             current_ids = {pos['positionId'] for pos in current_api_positions if 'positionId' in pos}
             state_ids = set(self.open_positions.keys())

             # Add New / Update Existing
             for pos_data in current_api_positions:
                 pos_id = pos_data.get('positionId')
                 if not pos_id: continue

                 if pos_id not in state_ids:
                     self.open_positions[pos_id] = pos_data # Add new
                     new_pos_count += 1
                     state_changed = True
                 # Update if data differs (simple dict comparison, might be noisy)
                 elif self.open_positions[pos_id] != pos_data:
                      self.open_positions[pos_id] = pos_data # Update existing
                      update_count += 1
                      state_changed = True # Consider changes like P/L, SL/TP modification as updates

             # Remove Closed positions (IDs in state but not in current API list)
             ids_to_remove = state_ids - current_ids
             for pos_id in ids_to_remove:
                  closed_pos_info = self.open_positions.pop(pos_id) # Remove and get data
                  closed_positions_data.append(closed_pos_info)
                  closed_pos_count += 1
                  state_changed = True

             if state_changed:
                  logger.info(f"Position Sync: {new_pos_count} New, {closed_pos_count} Closed, {update_count} Updated.")
                  # Save state if changes occurred (call save_state outside lock)
             else:
                  logger.debug("Position Sync: No changes detected.")

        if state_changed:
             await self.save_state()

        return closed_positions_data # Return data of positions detected as closed this cycle

    def get_open_positions_by_symbol(self, symbol_name: str) -> List[Dict]:
         """Gets open positions matching the symbol (synchronous read)."""
         symbol_name = symbol_name.upper()
         # Assumes position data includes 'symbolName' matching config symbol
         # Needs lock? Read-only, brief access, probably okay without lock.
         return [pos for pos in self.open_positions.values() if pos.get('symbolName') == symbol_name]

    def get_position_details(self, position_id: int) -> Optional[Dict]:
        # Synchronous read
        return self.open_positions.get(position_id)


# ==============================================================================
# 9. PERFORMANCE TRACKER CLASS (Needs Deal History Integration)
# ==============================================================================
class PerformanceTracker:
    def __init__(self, connector: CTraderConnector, config: Dict[str, Any]):
        self.connector = connector # Needed for symbol details & deal history
        self.config = config['logging']
        self.filepath = os.path.join(self.config['data_dir'], self.config['performance_log_file'])
        self.log_columns = [
            'SignalID', 'PositionID', 'TimestampUTC', 'Symbol', 'Type', 'Lots',
            'EntryPrice', 'ExitPrice', 'StopLossPrice', 'TakeProfitPrice', 'CloseReason',
            'PL_Pips', 'GrossPL_AccCcy', 'Commission', 'Swap', 'NetPL_AccCcy',
            'SignalStrength', 'TriggerFactors', 'Duration_sec' # Added duration
        ]
        self._lock = asyncio.Lock() # Lock for file access
        self.trade_log = self._load_log() # Load sync on init

    def _load_log(self) -> pd.DataFrame:
        # Synchronous load during initialization
        if os.path.exists(self.filepath):
            try:
                log = pd.read_csv(self.filepath)
                # Convert relevant columns to numeric, coercing errors
                num_cols = ['Lots', 'EntryPrice', 'ExitPrice', 'StopLossPrice', 'TakeProfitPrice',
                            'PL_Pips', 'GrossPL_AccCcy', 'Commission', 'Swap', 'NetPL_AccCcy', 'Duration_sec']
                for col in num_cols:
                     if col in log.columns:
                          log[col] = pd.to_numeric(log[col], errors='coerce')
                # Ensure all expected columns exist
                for col in self.log_columns:
                    if col not in log.columns: log[col] = np.nan
                return log[self.log_columns] # Select/reorder
            except Exception as e:
                 logger.error(f"Error loading performance log {self.filepath}: {e}. Creating new log.")
                 return pd.DataFrame(columns=self.log_columns)
        else:
            logger.info(f"Performance log {self.filepath} not found. Creating.")
            return pd.DataFrame(columns=self.log_columns)

    async def log_trade_closure(self, closed_position_info: Dict, close_reason: str, signal_info: Optional[Dict]):
        """
        Logs a completed trade using closed position data and Fetches Deal History for accuracy.
        """
        if not closed_position_info: return

        pos_id = closed_position_info.get('positionId')
        if not pos_id: logger.error("Cannot log trade closure: Missing positionId."); return

        logger.info(f"Logging closure for Position ID: {pos_id}, Reason: {close_reason}")

        # --- Extract basic info from closed position state ---
        symbol_id = closed_position_info.get('symbolId')
        symbol_name = closed_position_info.get('symbolName', 'Unknown')
        digits = closed_position_info.get('digits', 5)
        pip_pos = closed_position_info.get('pipPosition', 4)
        lot_size = closed_position_info.get('lotSize', 100000) # Default if missing
        trade_side_enum = closed_position_info.get('tradeSide')
        trade_type = "BUY" if trade_side_enum == ProtoOATradeSide.BUY else "SELL" if trade_side_enum == ProtoOATradeSide.SELL else "Unknown"
        volume_units = closed_position_info.get('volume') # This is original volume? Or remaining if partial close? Use history.
        entry_price = closed_position_info.get('entryPrice')
        open_timestamp_ms = closed_position_info.get('openTimestamp')
        signal_id_from_comment = closed_position_info.get('comment', '').split('-')[-1] or str(pos_id)
        signal_id = signal_info.get('id') if signal_info else signal_id_from_comment

        # --- Fetch Deal History for this Position ---
        # Need to determine time range. Use open time +/- buffer, up to now.
        close_timestamp_ms = int(time.time() * 1000) # Assume closure happened recently
        history_start_ms = open_timestamp_ms - 60000 if open_timestamp_ms else close_timestamp_ms - 3600000 # 1 min before open or 1hr before close
        history_end_ms = close_timestamp_ms + 60000 # 1 min after close

        deals = await self.connector.get_deal_history(history_start_ms, history_end_ms)
        position_deals = [d for d in deals if d.get('positionId') == pos_id]

        if not position_deals:
             logger.error(f"Could not find any deals in history for closed position {pos_id}. Logging with estimated data.")
             # Fallback to using potentially inaccurate data from the closed position state
             exit_price = closed_position_info.get('currentPrice', entry_price) # Very rough estimate!
             gross_pl = closed_position_info.get('grossProfit', 0.0) # Might be last unrealized P/L
             commission = closed_position_info.get('commission', 0.0)
             swap = closed_position_info.get('swap', 0.0)
             net_pl = gross_pl + commission + swap
             closed_volume_units = volume_units # Assume full closure
             exit_timestamp_ms = close_timestamp_ms
        else:
             logger.info(f"Found {len(position_deals)} deals for position {pos_id}.")
             # --- Aggregate Deal Information ---
             # Find entry deal(s) (status FILLED) and closing deal(s) (status CLOSED)
             entry_deals = [d for d in position_deals if d['dealStatus'] == 2] # DEAL_STATUS_FILLED
             closing_deals = [d for d in position_deals if d['dealStatus'] == 4] # DEAL_STATUS_CLOSED

             if not entry_deals: logger.warning(f"No entry deal found for position {pos_id}?"); entry_price = entry_price # Use state price
             else: entry_price = entry_deals[0]['executionPrice'] # Assume single entry deal for simplicity

             if not closing_deals:
                  logger.warning(f"No closing deal found for position {pos_id} despite being closed? Using estimates.")
                  exit_price = entry_price # Bad estimate
                  gross_pl, commission, swap, net_pl = 0.0, 0.0, 0.0, 0.0
                  closed_volume_units = 0
                  exit_timestamp_ms = close_timestamp_ms
             else:
                  # Aggregate P/L, commission, swap, volume from closing deals
                  # Note: `closePositionDetail` contains the relevant P/L info for the *portion* closed by that deal
                  total_gross_pl = sum(d['closePositionDetail']['grossProfit'] for d in closing_deals if d.get('closePositionDetail'))
                  total_commission = sum(d['commission'] for d in position_deals) # Sum all commissions for the pos
                  total_swap = sum(d['swap'] for d in position_deals) # Sum all swaps for the pos
                  closed_volume_units = sum(d['closePositionDetail']['closedVolume'] for d in closing_deals if d.get('closePositionDetail'))
                  net_pl = total_gross_pl + total_commission + total_swap # Note: These are already cents, sum then divide

                  # Calculate weighted average exit price if multiple closing deals
                  if closed_volume_units > 0:
                       weighted_sum = sum(d['executionPrice'] * d['closePositionDetail']['closedVolume'] for d in closing_deals if d.get('closePositionDetail'))
                       exit_price = weighted_sum / closed_volume_units
                  else: exit_price = entry_price # Fallback if no closing volume

                  exit_timestamp_ms = max(d['executionTimestamp'] for d in closing_deals) # Time of last closing deal

        # --- Calculate Final Metrics ---
        lots = closed_volume_units / 100.0 if closed_volume_units else 0.0 # Lots closed
        pl_pips = 0.0
        if pip_pos is not None and entry_price is not None and exit_price is not None:
            pip_value_price = 10**(-pip_pos)
            if pip_value_price > 0:
                if trade_type == 'BUY': pl_pips = (exit_price - entry_price) / pip_value_price
                elif trade_type == 'SELL': pl_pips = (entry_price - exit_price) / pip_value_price

        duration_sec = (exit_timestamp_ms - open_timestamp_ms) / 1000.0 if open_timestamp_ms and exit_timestamp_ms else 0.0

        # Prepare log entry dictionary
        log_entry_data = {
            'SignalID': signal_id,
            'PositionID': pos_id,
            'TimestampUTC': datetime.fromtimestamp(exit_timestamp_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            'Symbol': symbol_name,
            'Type': trade_type,
            'Lots': round(lots, 2),
            'EntryPrice': round(entry_price, digits) if entry_price is not None else None,
            'ExitPrice': round(exit_price, digits) if exit_price is not None else None,
            'StopLossPrice': closed_position_info.get('slPrice'), # Last known SL
            'TakeProfitPrice': closed_position_info.get('tpPrice'), # Last known TP
            'CloseReason': close_reason,
            'PL_Pips': round(pl_pips, 1),
            'GrossPL_AccCcy': round(total_gross_pl / 100.0, 2) if 'total_gross_pl' in locals() else round(gross_pl, 2),
            'Commission': round(total_commission / 100.0, 2) if 'total_commission' in locals() else round(commission, 2),
            'Swap': round(total_swap / 100.0, 2) if 'total_swap' in locals() else round(swap, 2),
            'NetPL_AccCcy': round(net_pl / 100.0, 2) if 'net_pl' in locals() else round(net_pl, 2),
            'SignalStrength': signal_info.get('strength', 'N/A') if signal_info else 'N/A',
            'TriggerFactors': "; ".join(signal_info.get('factors', [])) if signal_info else 'N/A',
            'Duration_sec': round(duration_sec, 0)
        }

        new_log_entry = pd.DataFrame([log_entry_data])

        # Append to the log DataFrame safely
        async with self._lock:
            self.trade_log = pd.concat([self.trade_log, new_log_entry], ignore_index=True)
            # Save the updated log asynchronously
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._save_log_sync) # Run sync save in executor

        logger.info(f"Logged Trade Closure: PosID={pos_id}, Reason={close_reason}, Net P/L={log_entry_data['NetPL_AccCcy']:.2f} ({log_entry_data['PL_Pips']:.1f} pips)")


    def _save_log_sync(self):
        """Synchronous helper to save the performance log."""
        try:
            os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
            # Ensure consistent column order
            self.trade_log[self.log_columns].to_csv(self.filepath, index=False, encoding='utf-8')
        except Exception as e:
             logger.error(f"Error saving performance log to {self.filepath}: {e}")

    def calculate_stats(self) -> Dict[str, Any]:
        # Run calculation synchronously, potentially blocking if log is huge
        # Consider running in executor if performance becomes an issue
        asyncio.get_running_loop().run_in_executor(None, self._calculate_stats_sync)
        return {} # Return immediately, stats logged by sync func

    def _calculate_stats_sync(self) -> Dict[str, Any]:
        """Synchronous calculation of performance stats."""
        asyncio.run(self._lock.acquire()) # Acquire async lock from sync context (careful!)
        try:
            log = self.trade_log.copy()
            if log.empty:
                 logger.info("Performance Stats: No trades logged yet.")
                 return {"total_trades": 0}

            # Drop rows where essential P/L data is missing
            log = log.dropna(subset=['NetPL_AccCcy', 'PL_Pips'])
            if log.empty:
                logger.info("Performance Stats: No valid trades found after NaN drop.")
                return {"total_trades": 0}

            stats = {}
            stats['total_trades'] = len(log)
            wins = log[log['NetPL_AccCcy'] > 0]
            losses = log[log['NetPL_AccCcy'] <= 0] # Include break-even as losses for win rate

            stats['winning_trades'] = len(wins)
            stats['losing_trades'] = len(losses)
            stats['win_rate_percent'] = (stats['winning_trades'] / stats['total_trades'] * 100) if stats['total_trades'] > 0 else 0

            stats['total_net_pl'] = log['NetPL_AccCcy'].sum()
            stats['total_pips'] = log['PL_Pips'].sum()

            stats['average_win_pl'] = wins['NetPL_AccCcy'].mean() if stats['winning_trades'] > 0 else 0
            stats['average_loss_pl'] = losses['NetPL_AccCcy'].mean() if stats['losing_trades'] > 0 else 0
            stats['average_win_pips'] = wins['PL_Pips'].mean() if stats['winning_trades'] > 0 else 0
            stats['average_loss_pips'] = losses['PL_Pips'].mean() if stats['losing_trades'] > 0 else 0

            # Profit Factor
            total_gross_profit = wins['NetPL_AccCcy'].sum() # Using Net P/L for simplicity
            total_gross_loss = abs(losses['NetPL_AccCcy'].sum())
            stats['profit_factor'] = (total_gross_profit / total_gross_loss) if total_gross_loss > 0 else 9999.0 # Avoid division by zero

            # Average Trade Duration
            stats['average_duration_min'] = log['Duration_sec'].mean() / 60.0 if 'Duration_sec' in log.columns and log['Duration_sec'].notna().any() else 0.0

            # Format stats for logging
            log_stats = f"--- Performance Stats ({stats['total_trades']} Trades) ---\n"
            log_stats += f" Win Rate: {stats['win_rate_percent']:.1f}% ({stats['winning_trades']} W / {stats['losing_trades']} L)\n"
            log_stats += f" Total Net P/L: {stats['total_net_pl']:.2f} | Total Pips: {stats['total_pips']:.1f}\n"
            log_stats += f" Avg Win: {stats['average_win_pl']:.2f} ({stats['average_win_pips']:.1f} pips) | Avg Loss: {stats['average_loss_pl']:.2f} ({stats['average_loss_pips']:.1f} pips)\n"
            log_stats += f" Profit Factor: {stats['profit_factor']:.2f}\n"
            log_stats += f" Avg Duration: {stats['average_duration_min']:.1f} min"

            logger.info(log_stats)
            return stats

        finally:
            self._lock.release() # Release async lock

# ==============================================================================
# 10. NOTIFIER CLASS (Adapted for Telegram, Async)
# ==============================================================================
class Notifier:
    def __init__(self, config: Dict[str, Any]):
        self.config = config.get('notifications', {})
        self.trading_config = config['trading']
        self.exec_config = config['execution']
        self.enabled = self.config.get('enable_telegram', False) and TELEGRAM_AVAILABLE and self.config.get('telegram_bot_token') and self.config.get('admin_user_ids')
        self.bot: Optional[Bot] = None
        self.admin_ids: List[int] = self.config.get('admin_user_ids', [])
        self.token: Optional[str] = self.config.get('telegram_bot_token')
        self.retries = self.config.get('telegram_send_retries', 3)
        self.delay = self.config.get('telegram_retry_delay_s', 5)

        if self.enabled:
            try:
                 # Use ApplicationBuilder for modern python-telegram-bot setup
                 self.application = ApplicationBuilder().token(self.token).build()
                 self.bot = self.application.bot # Get bot instance
                 # asyncio.create_task(self.check_bot_connection()) # Check connection async
                 logger.info(f"Telegram Bot initialized for Admin IDs: {self.admin_ids}")
            except Exception as e:
                 logger.error(f"Failed to initialize Telegram Bot: {e}. Disabling notifications.", exc_info=True)
                 self.enabled = False
        else:
            logger.info("Telegram notifications disabled (check config, library install, token, admin_ids).")

    async def check_bot_connection(self):
         """Verifies bot token and connectivity."""
         if not self.enabled or not self.bot: return
         try:
              bot_info = await self.bot.get_me()
              logger.info(f"Telegram connection successful. Bot Name: {bot_info.full_name}")
         except Exception as e:
              logger.error(f"Telegram connection check failed: {e}. Notifications might not work.")
              self.enabled = False # Disable if check fails

    async def send_message(self, message: str, parse_mode: str = ParseMode.MARKDOWN, chat_id: Optional[int] = None):
        """Sends a message to a specific chat ID or all admin IDs."""
        if not self.enabled or not self.bot: return True # Return true if disabled, as message "sending" succeeded vacuously

        target_ids = [chat_id] if chat_id else self.admin_ids
        if not target_ids:
             logger.warning("Telegram send_message called, but no target chat IDs found.")
             return False

        success = True
        for cid in target_ids:
            attempt = 0
            while attempt < self.retries:
                try:
                    await self.bot.send_message(chat_id=cid, text=message, parse_mode=parse_mode)
                    logger.info(f"Sent Telegram notification to {cid} (attempt {attempt+1}).")
                    break # Success for this ID
                except RetryAfter as e:
                    logger.warning(f"TG flood control for chat {cid}: wait {e.retry_after}s...")
                    await asyncio.sleep(e.retry_after + 1)
                    # Do not increment attempt count for RetryAfter
                except (TelegramError, NetworkError) as e:
                    logger.error(f"TG send to {cid} failed (attempt {attempt+1}/{self.retries}): {e}")
                    attempt += 1
                    if attempt < self.retries: await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.error(f"Unexpected TG error sending to {cid}: {e}", exc_info=True)
                    success = False # Mark failure for this ID
                    break # Don't retry on unexpected errors
            else: # Loop finished without break (all retries failed)
                logger.error(f"Failed to send TG message to {cid} after {self.retries} attempts.")
                success = False # Mark overall failure if any ID fails

        return success

    def format_signal_message(self, signal_data: Dict) -> str:
        trade_mode = "AUTO" if self.exec_config['enable_auto_trading'] else "MANUAL"
        digits = signal_data.get('digits', 5)
        symbol = signal_data['symbol']
        sig_type = signal_data['type']
        strength = signal_data['strength']
        entry = signal_data['entry_ideal']
        sl = signal_data['stop_loss_price']
        sl_pips = signal_data['sl_pips']
        risk_ccy = signal_data['sl_risk_account_ccy']
        ccy = signal_data['account_currency']
        lots = signal_data['position_size_lots']
        tps = signal_data['take_profit_prices']
        factors = signal_data.get('factors', [])
        sig_tf = signal_data['signal_tf_str']
        trend_tf = signal_data['trend_tf_str']

        header = f"🚨 *cTrader Signal ({strength} - {trade_mode})* 🚨\n"
        symbol_line = f"*{symbol}* ({sig_type})"
        entry_line = f"Entry Price: ~{entry:.{digits}f}"
        sl_line = f"Stop Loss: {sl:.{digits}f} ({sl_pips:.1f} pips / ~${risk_ccy:.2f} {ccy})"
        size_line = f"Size: {lots:.2f} lots\n"
        tp_lines = ""
        for i, tp in enumerate(tps): tp_lines += f"TP{i+1}: {tp:.{digits}f}\n"

        rationale = "\n*Rationale:*\n" + "\n".join([f"- {f}" for f in factors]) if factors else ""
        tf_line = f"\nSignal TF: {sig_tf} | Trend TF: {trend_tf}"

        message = f"{header}{symbol_line}\n{entry_line}\n{sl_line}{size_line}{tp_lines}{rationale}{tf_line}"
        return message

    def format_trade_action(self, action_type: str, details: Dict) -> str:
        """Formats messages for trade open, close, modify, or failure."""
        if not self.exec_config['enable_auto_trading'] and action_type in ["OPEN", "CLOSE", "MODIFY", "FAILED"]:
            return "" # Don't notify automated actions if auto-trading is off

        symbol = details.get('symbolName', self.trading_config.get('symbol', 'N/A'))
        pos_id = details.get('positionId', 'N/A')
        order_id = details.get('orderId', pos_id)
        digits = details.get('digits', 5)
        price = details.get('price', None) # Entry or Close price
        volume_lots = details.get('executedLots', None) # From execution result
        if volume_lots is None and 'volume' in details: # Fallback to position volume (units)
             volume_lots = details['volume'] / 100.0

        # Determine type BUY/SELL from tradeSide enum if present
        trade_side_enum = details.get('tradeSide')
        order_type = "N/A"
        if trade_side_enum == ProtoOATradeSide.BUY: order_type = "BUY"
        elif trade_side_enum == ProtoOATradeSide.SELL: order_type = "SELL"

        message = ""
        if action_type == "OPEN":
            sl = details.get('stopLoss') or details.get('slPrice') # Check both keys
            tp = details.get('takeProfit') or details.get('tpPrice')
            icon = "🟢"
            message = f"{icon} *Trade Opened ({symbol} {order_type})* {icon}\n"
            message += f"Pos ID: `{pos_id}` | Order ID: `{order_id}`\n"
            message += f"Size: {volume_lots:.2f} lots\n"
            message += f"Price: {price:.{digits}f}\n" if price else "Price: N/A\n"
            message += f"SL: {sl:.{digits}f}\n" if sl else "SL: Not Set\n"
            message += f"TP: {tp:.{digits}f}\n" if tp else "TP: Not Set\n"
            message += f"Signal ID: `{details.get('signal_id', 'N/A')}`"

        elif action_type == "CLOSE":
             # Requires P/L info passed in 'details' from performance tracker
             net_pl = details.get('netPl', 0.0)
             pl_pips = details.get('plPips', 0.0)
             reason = details.get('reason', 'N/A')
             ccy = details.get('currency', '$')
             profit_str = f"+{ccy}{net_pl:.2f}" if net_pl >= 0 else f"-{ccy}{abs(net_pl):.2f}"
             pips_str = f"+{pl_pips:.1f}" if pl_pips >= 0 else f"{pl_pips:.1f}"
             icon = "✅" if net_pl >= 0 else "🔴"
             message = f"{icon} *Trade Closed ({symbol})* {icon}\n"
             message += f"Pos ID: `{pos_id}`\n"
             message += f"Size: {volume_lots:.2f} lots\n"
             message += f"Close Price: {price:.{digits}f}\n" if price else "Close Price: N/A\n"
             message += f"Net P/L: *{profit_str}* ({pips_str} pips)\n"
             message += f"Reason: {reason}"

        elif action_type == "MODIFY":
             sl = details.get('slPrice')
             tp = details.get('tpPrice')
             reason = details.get('reason', 'SL/TP Updated') # e.g., Trailing Stop
             icon = "⚙️"
             message = f"{icon} *Trade Modified ({symbol})* {icon}\n"
             message += f"Pos ID: `{pos_id}`\n"
             message += f"Reason: {reason}\n"
             message += f"New SL: {sl:.{digits}f}\n" if sl else "SL: Unchanged\n"
             message += f"New TP: {tp:.{digits}f}\n" if tp else "TP: Unchanged\n"

        elif action_type == "FAILED":
             reason = details.get('reason', 'Unknown')
             icon = "❌"
             message = f"{icon} *Order Failed ({symbol})* {icon}\n"
             message += f"Action: {details.get('intended_action', 'N/A')} | Signal ID: `{details.get('signal_id', 'N/A')}`\n"
             message += f"Reason: {reason}"

        else:
            message = f"ℹ️ *Bot Notification:* ℹ️\n {action_type}: {details}"

        return message

# --- Initialize Global Notifier ---
notifier = Notifier(CONFIG) if TELEGRAM_ENABLED else None

# ==============================================================================
# 11. TRADING ENGINE & MAIN LOGIC (Async)
# ==============================================================================

async def handle_trade_execution(signal_data: Dict, connector: CTraderConnector, bot_state: BotState):
    """Attempts to execute a trade based on the signal data using cTrader connector."""
    if not CONFIG['execution']['enable_auto_trading']: return False

    signal_id = signal_data['id']
    symbol = signal_data['symbol']
    signal_type = signal_data['type'] # "BUY" or "SELL"
    lots = signal_data['position_size_lots']
    sl_price = signal_data['stop_loss_price']
    # Use first TP level for initial order placement
    tp_price = signal_data['take_profit_prices'][0] if signal_data['take_profit_prices'] else None

    # Convert signal type string to cTrader Enum
    trade_side_enum = ProtoOATradeSide.BUY if signal_type == "BUY" else ProtoOATradeSide.SELL

    logger.info(f"--- Attempting to Execute Signal {signal_id} via cTrader ---")
    execution_result = await connector.place_market_order(
        symbol_name=symbol,
        trade_side=trade_side_enum,
        volume_lots=lots, # Pass size in LOTS
        stop_loss_price=sl_price, # Pass SL price
        take_profit_price=tp_price, # Pass TP price
        signal_id=signal_id # Pass signal ID for comment/label
    )

    # Process result (note: confirmation is now partly based on checking state after a delay)
    if execution_result and execution_result.get('result'):
        logger.info(f"Signal {signal_id} executed successfully via cTrader (Position ID: {execution_result.get('positionId')}).")
        await bot_state.update_signal_status(signal_id, "Executed", execution_result)
        # Send Telegram notification of successful open
        if notifier:
            # Prepare details for notification
            open_details = {
                'symbolName': symbol,
                'digits': signal_data['digits'],
                'tradeSide': trade_side_enum,
                'price': execution_result.get('entryPrice'), # May be None initially
                'stopLoss': sl_price,
                'takeProfit': tp_price,
                'executedLots': execution_result.get('executedLots'),
                'positionId': execution_result.get('positionId'),
                'orderId': execution_result.get('orderId', execution_result.get('positionId')), # Fallback
                'signal_id': signal_id
            }
            await notifier.send_message(notifier.format_trade_action("OPEN", open_details))
        return True
    else:
        err_code = execution_result.get('errorCode', 'UNKNOWN') if execution_result else 'NONE'
        err_msg = execution_result.get('message', 'No response') if execution_result else 'No response'
        logger.error(f"Signal {signal_id} cTrader execution failed: {err_code} - {err_msg}")
        await bot_state.update_signal_status(signal_id, "Failed")
        # Send Telegram notification of failure
        if notifier:
             fail_details = {
                  'symbolName': symbol,
                  'signal_id': signal_id,
                  'intended_action': f'{signal_type} {lots:.2f} lots',
                  'reason': f"{err_code}: {err_msg}",
                  'digits': signal_data.get('digits', 5)
             }
             await notifier.send_message(notifier.format_trade_action("FAILED", fail_details))
        return False

async def manage_open_positions(connector: CTraderConnector, bot_state: BotState, risk_manager: RiskManager, perf_tracker: PerformanceTracker):
    """Manages open positions: checks closures, updates trailing stops."""
    if not connector._connected or not connector._authenticated:
        logger.warning("Position management skipped: Connector not ready.")
        return

    # 1. Sync internal state with API
    # Fetching positions also updates the internal state via _handle_... callbacks or direct set
    current_api_positions = await connector.fetch_open_positions() # Fetch fresh list
    closed_positions_data = await bot_state.sync_open_positions(current_api_positions) # Syncs state and returns closed ones

    # 2. Log closed positions (using data returned by sync)
    for closed_pos_info in closed_positions_data:
        pos_id = closed_pos_info.get('positionId')
        signal_id_from_comment = closed_pos_info.get('comment', '').split('-')[-1] or str(pos_id)
        # Find original signal data from state (might have been removed if bot restarted)
        signal_info = bot_state.get_signal(signal_id_from_comment)

        # Determine Close Reason - Requires interpreting deal history or execution events. Hardcoded for now.
        # TODO: Enhance this using deal history analysis
        close_reason = "Closed (Sync/Unknown)" # Placeholder

        # Log the closure using deal history for accuracy
        await perf_tracker.log_trade_closure(closed_pos_info, close_reason, signal_info)

        # Remove the signal associated with the closed position from active state
        if signal_info:
             await bot_state.remove_signal(signal_info['id'])
        elif signal_id_from_comment: # If signal not found, try removing by comment ID
             await bot_state.remove_signal(signal_id_from_comment)


    # 3. Manage Active Positions (Trailing Stop)
    if not CONFIG.get('risk', {}).get('enable_trailing_stop', False): return

    active_positions = bot_state.get_open_positions_by_symbol(CONFIG['trading']['symbol']) # Use state after sync
    if not active_positions: return

    symbol = CONFIG['trading']['symbol']
    current_tick = connector.get_current_tick(symbol) # Uses cached tick
    if not current_tick:
        logger.warning("Cannot manage TS: Current tick unavailable.")
        return
    if (datetime.now(timezone.utc) - current_tick['time']) > timedelta(seconds=CONFIG['timing']['tick_staleness_threshold_s']):
        logger.warning(f"Skipping TS check: Tick data for {symbol} is stale (> {CONFIG['timing']['tick_staleness_threshold_s']}s old).")
        return

    for position_state in active_positions:
         pos_id = position_state['positionId']
         signal_id_from_comment = position_state.get('comment', '').split('-')[-1] or str(pos_id)
         original_signal = bot_state.get_signal(signal_id_from_comment)

         if not original_signal or 'atr_at_entry_pips' not in original_signal:
             logger.warning(f"Cannot manage TS for Pos {pos_id}: Original signal or entry ATR missing."); continue

         entry_atr_pips = original_signal['atr_at_entry_pips']

         # Calculate new TSL price
         new_tsl_price = risk_manager.calculate_trailing_stop(
             position_data=position_state, current_tick=current_tick, entry_atr_pips=entry_atr_pips
         )

         if new_tsl_price is not None:
             # Modify position using PRICE
             logger.info(f"Attempting to apply Trailing Stop for Pos {pos_id} to SL={new_tsl_price:.{position_state.get('digits', 5)}f}")
             modify_result = await connector.modify_position_sltp(pos_id, sl_price=new_tsl_price)

             if modify_result and modify_result.get('result'):
                 logger.info(f"Trailing Stop modification sent successfully for Pos {pos_id}.")
                 # Callback (_handle_execution_event) should update the state.
                 # Optionally send notification
                 if notifier:
                      mod_details = {
                           'symbolName': symbol,
                           'positionId': pos_id,
                           'slPrice': new_tsl_price,
                           'tpPrice': position_state.get('tpPrice'), # Existing TP
                           'reason': 'Trailing Stop Update',
                           'digits': position_state.get('digits', 5)
                      }
                      await notifier.send_message(notifier.format_trade_action("MODIFY", mod_details))
             else:
                  logger.error(f"Failed to apply Trailing Stop for Pos {pos_id}: {modify_result}")


async def run_bot_iteration(connector: CTraderConnector, data_manager: DataManager,
                            tech_analyzer: TechnicalAnalyzer, fund_analyzer: FundamentalAnalyzer,
                            risk_manager: RiskManager, sig_generator: SignalGenerator,
                            perf_tracker: PerformanceTracker, bot_state: BotState):
    """Runs a single asynchronous iteration of the bot's check cycle."""
    logger.debug("--- Starting Bot Iteration ---")
    iter_start_time = time.time()

    # 1. Check Connection (includes auto-reconnect attempt)
    if not await connector.check_connection():
        logger.error("cTrader connection lost. Skipping iteration.")
        # Wait longer before next attempt if connection fails
        await asyncio.sleep(CONFIG.get('timing', {}).get('reconnect_attempt_delay_s', 30))
        return

    # 2. Update Fundamentals (News + Calendar)
    await fund_analyzer.fetch_and_analyze()

    # 3. Manage Open Positions (Sync state, log closures, handle TS)
    if CONFIG['execution']['enable_auto_trading']:
        try:
            await manage_open_positions(connector, bot_state, risk_manager, perf_tracker)
        except Exception as e:
            logger.error(f"Error during position management: {e}", exc_info=True)

    # 4. Fetch/Update Market Data
    symbol = CONFIG['trading']['symbol']
    tf_trend = CONFIG['trading']['timeframe_trend']
    tf_entry = CONFIG['trading']['timeframe_entry']
    bars_trend = CONFIG['timing']['bars_trend_tf']
    bars_entry = CONFIG['timing']['bars_entry_tf']
    data_trend, data_entry = None, None
    try:
        # Fetch data concurrently?
        tasks = [
             data_manager.update_data(symbol, tf_trend, bars_trend),
             data_manager.update_data(symbol, tf_entry, bars_entry)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        if isinstance(results[0], Exception): logger.error(f"Error updating trend data: {results[0]}"); return
        if isinstance(results[1], Exception): logger.error(f"Error updating entry data: {results[1]}"); return
        data_trend, data_entry = results

        current_tick = connector.get_current_tick(symbol) # Uses cached tick from callback

        # Validate tick data
        if not current_tick:
             logger.warning(f"Tick data for {symbol} is unavailable. Skipping analysis.")
             return
        tick_staleness_threshold = CONFIG.get('timing', {}).get('tick_staleness_threshold_s', 15)
        tick_age = (datetime.now(timezone.utc) - current_tick['time']).total_seconds()
        if tick_age > tick_staleness_threshold:
            logger.warning(f"Tick data for {symbol} seems stale ({tick_age:.1f}s old > {tick_staleness_threshold}s). Price checks might be inaccurate.")
            # Force re-subscribe? Connector should handle this ideally.
            # await connector.subscribe_to_symbols([symbol]) # Re-subscribe if needed

        if data_trend is None or data_entry is None:
             logger.warning("Failed to get required historical data for analysis. Skipping.")
             return

    except Exception as e:
         logger.error(f"Error fetching/updating data: {e}", exc_info=True)
         return

    # 5. Technical Analysis
    analyzed_trend_df, analyzed_entry_df = None, None
    try:
        # Run TA calculations (consider running in executor if they become CPU bound)
        analyzed_trend_df = tech_analyzer.calculate_indicators(data_trend)
        analyzed_entry_df = tech_analyzer.calculate_indicators(data_entry)
        if analyzed_trend_df is None or analyzed_entry_df is None:
            logger.warning("Failed indicator calculation. Skipping signal gen.")
            return
    except Exception as e:
        logger.error(f"Error during tech analysis: {e}", exc_info=True)
        return

    # 6. Check Trading Conditions & Generate Signal
    open_trades_count = len(bot_state.get_open_positions_by_symbol(symbol))
    max_trades = CONFIG['trading'].get('max_open_trades_per_symbol', 1)
    signal = None

    if open_trades_count >= max_trades:
         logger.info(f"Signal gen skipped: Max open trades ({open_trades_count}/{max_trades}) reached for {symbol}.")
    else:
        try:
             signal = sig_generator.generate_signal(analyzed_trend_df, analyzed_entry_df, current_tick)
        except Exception as e:
             logger.error(f"Error during signal generation: {e}", exc_info=True)

    # 7. Handle Signal (Notify & Execute)
    if signal:
        logger.info(f"!!! NEW SIGNAL DETECTED: {signal['id']} ({signal['type']} {signal['strength']}) !!!")
        await bot_state.add_signal(signal) # Adds to state and saves

        # Notify via Telegram
        if notifier:
             message = notifier.format_signal_message(signal)
             notify_success = await notifier.send_message(message)
             if notify_success:
                  await bot_state.update_signal_status(signal['id'], "Sent")
             else:
                  logger.error(f"Failed to send notification for signal {signal['id']}.")

        # Execute if auto-trading is enabled
        if CONFIG['execution']['enable_auto_trading']:
            await handle_trade_execution(signal, connector, bot_state) # Handles status updates internally
        else:
             logger.info(f"Manual mode: Signal {signal['id']} generated but not executed.")

    # 8. Periodic Tasks (e.g., Stats Calculation) - Handled by scheduler if added, or simple timer
    # Example: Calculate stats every hour
    global last_stats_calc_time # Need global or pass state object for this
    if time.time() - last_stats_calc_time > 3600:
        perf_tracker.calculate_stats() # Runs sync calc in executor
        last_stats_calc_time = time.time()


    logger.debug(f"--- Iteration Finished (Took {time.time() - iter_start_time:.3f}s) ---")


# --- Global variable for periodic stats calculation ---
last_stats_calc_time = 0

# ==============================================================================
# 12. MAIN EXECUTION BLOCK (Async)
# ==============================================================================
async def main():
    """Main asynchronous function to run the bot."""
    global last_stats_calc_time # Allow modification
    if not CONFIG:
        print("FATAL: Config not loaded.")
        return
    if not CTRADER_API_AVAILABLE:
         print("FATAL: Essential cTrader API library not available.")
         return

    logger.info("--- Initializing Bot Components (Async) ---")
    connector = None # Define for finally block
    keep_running = True
    last_stats_calc_time = time.time() # Initialize timer

    try:
        # --- Initialize Components ---
        connector = CTraderConnector(CONFIG)
        data_mgr = DataManager(connector, CONFIG)
        tech_analyzer = TechnicalAnalyzer(connector, CONFIG)
        fund_analyzer = FundamentalAnalyzer(CONFIG)
        risk_mgr = RiskManager(connector, CONFIG)
        perf_tracker = PerformanceTracker(connector, CONFIG)
        bot_state = BotState(CONFIG) # Loads previous state (sync)
        signal_gen = SignalGenerator(tech_analyzer, risk_mgr, fund_analyzer, connector, CONFIG)
        # Notifier already initialized globally if enabled

        # --- Connect to cTrader ---
        if not await connector.connect(): # Handles retries internally
            logger.critical("Failed to connect to cTrader API during startup. Exiting.")
            return # Exit async function

        # --- Load Initial Data ---
        logger.info("--- Loading Initial Historical Data ---")
        symbol = CONFIG['trading']['symbol']
        tf_trend = CONFIG['trading']['timeframe_trend']
        tf_entry = CONFIG['trading']['timeframe_entry']
        bars_trend = CONFIG['timing']['bars_trend_tf']
        bars_entry = CONFIG['timing']['bars_entry_tf']

        # Load concurrently
        initial_load_tasks = [
             data_mgr.load_initial_data(symbol, tf_trend, bars_trend),
             data_mgr.load_initial_data(symbol, tf_entry, bars_entry)
        ]
        results = await asyncio.gather(*initial_load_tasks, return_exceptions=True)
        initial_load_ok = True
        if isinstance(results[0], Exception) or results[0] is None:
             logger.error(f"Failed initial load: {symbol} {tf_trend} - {results[0]}")
             initial_load_ok = False
        if isinstance(results[1], Exception) or results[1] is None:
             logger.error(f"Failed initial load: {symbol} {tf_entry} - {results[1]}")
             initial_load_ok = False

        if not initial_load_ok:
             logger.critical("Failed to load critical initial data. Exiting.")
             if connector: await connector.disconnect()
             return
        logger.info("--- Initial Data Loaded ---")

        # --- Run Initial Sync & FA Check ---
        if CONFIG['execution']['enable_auto_trading']:
            logger.info("--- Performing Initial Position Sync ---")
            await manage_open_positions(connector, bot_state, risk_mgr, perf_tracker) # Initial sync
        logger.info("--- Performing Initial FA Check ---")
        await fund_analyzer.fetch_and_analyze() # Initial check

        # --- Main Loop ---
        check_interval = CONFIG.get('timing', {}).get('check_interval_seconds', 60)
        logger.info(f"--- Starting Main Loop (Check Interval: {check_interval}s) ---")

        while keep_running:
            loop_start_time = time.time()
            try:
                await run_bot_iteration(connector, data_mgr, tech_analyzer, fund_analyzer,
                                        risk_mgr, signal_gen, perf_tracker, bot_state)

            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt detected. Shutting down...")
                keep_running = False
            except asyncio.CancelledError:
                 logger.info("Main loop task cancelled.")
                 keep_running = False
            except Exception as e:
                 logger.critical(f"!!! UNHANDLED EXCEPTION IN MAIN LOOP: {e} !!!", exc_info=True)
                 # Optional: Implement more robust error handling, e.g., limited restarts
                 await asyncio.sleep(10) # Pause before next attempt after major error

            # Interval Sleep (Async)
            elapsed = time.time() - loop_start_time
            sleep_time = max(0.1, check_interval - elapsed)
            if keep_running:
                try:
                    await asyncio.sleep(sleep_time)
                except asyncio.CancelledError:
                     logger.info("Sleep interrupted, exiting loop.")
                     keep_running = False

    except Exception as e:
        logger.critical(f"!!! CRITICAL ERROR DURING STARTUP OR MAIN EXECUTION: {e} !!!", exc_info=True)
    finally:
        logger.info("--- Executing Graceful Shutdown ---")
        # Save state before disconnecting
        if 'bot_state' in locals() and bot_state:
             await bot_state.save_state()
        # Save historical data
        if 'data_mgr' in locals() and data_mgr:
             await data_mgr.save_all_data()
        # Disconnect API connection
        if connector:
             await connector.disconnect()
        logger.info("--- Bot Shutdown Complete ---")
        # Note: Can't call logging.shutdown() in async context easily without more complex handling

if __name__ == "__main__":
    # Setup asyncio event loop
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown initiated by KeyboardInterrupt.")
    except Exception as e:
         logger.critical(f"Unhandled exception in asyncio.run: {e}", exc_info=True)
    finally:
         # Ensure logs are flushed
         logging.shutdown()
         print("Main execution finished.")

# --- End of File gold.py ---
```

**Updated `config.yaml` Structure Sample:**

```yaml
# --- Bot Configuration ---
bot_version: "1.1.0-ctrader-openapi"

# --- cTrader API Connection (using ctrader-open-api) ---
ctrader:
  use_demo_account: true # Set to false for Live
  host_demo: "demo.ctraderapi.com"
  port_demo: 5035
  host_live: "live.ctraderapi.com" # Replace with your broker's live host if different
  port_live: 5035
  app_client_id: "YOUR_APP_CLIENT_ID" # MUST provide via config or env var CTRADER_APP_CLIENT_ID
  app_secret: "YOUR_APP_SECRET"       # MUST provide via config or env var CTRADER_APP_SECRET
  account_id: 1234567                  # MUST provide via config or env var CTRADER_ACCOUNT_ID (This is the ctidTraderAccountId)
  connect_retries: 5
  connect_retry_delay_s: 10
  # enable_heartbeat_check: false # Optional: Periodically fetch balance to check connection validity

# --- Trading Parameters ---
trading:
  symbol: "XAUUSD" # Primary symbol to trade
  timeframe_trend: "H1" # Higher timeframe for trend analysis
  timeframe_entry: "M5" # Lower timeframe for entry signals
  risk_per_trade_percent: 1.0 # e.g., 1.0 for 1% risk
  min_trade_lots: 0.01
  max_trade_lots: 1.0  # Max size per trade based on risk calc
  max_open_trades_per_symbol: 1 # Limit concurrent trades for the symbol
  min_signal_confidence: 4.0 # Minimum score needed to consider a signal
  strong_signal_confidence: 6.0 # Score threshold for "Strong" label
  moderate_signal_confidence: 4.0 # Score threshold for "Moderate" label
  max_spread_pips: 5.0 # Maximum allowed spread in pips for entry
  tp_levels: 1 # Number of take profit levels to calculate
  min_rr_ratio: 1.5 # Minimum Risk:Reward for TP1
  rr_step_increase: 0.5 # How much R:R increases for TP2, TP3 etc.

# --- Technical Analysis Settings ---
technical_analysis:
  ema_short: 9
  ema_medium: 21
  ema_slow: 50
  ema_trend: 200
  rsi_period: 14
  rsi_overbought: 70
  rsi_oversold: 30
  stoch_k: 14
  stoch_d: 3
  stoch_smooth: 3
  stoch_overbought: 80
  stoch_oversold: 20
  macd_fast: 12
  macd_slow: 26
  macd_signal: 9
  atr_period: 14
  bbands_period: 20
  bbands_stddev: 2.0
  volume_avg_period: 20
  volume_spike_factor: 1.5 # e.g., volume > 1.5 * average
  fractal_lookback: 2 # n bars left/right for fractal detection

# --- Fundamental Analysis Settings ---
fundamental_analysis:
  enable: false # Enable/disable NewsAPI fetching
  newsapi_key: "YOUR_NEWSAPI_KEY" # Provide via config or env var NEWSAPI_KEY
  news_check_interval_minutes: 30
  news_lookback_hours: 6
  newsapi_query_keywords: 'forex OR gold OR XAUUSD OR dollar OR fed OR inflation OR rates OR NFP OR FOMC'
  newsapi_sources: 'reuters,bloomberg,financial-post,associated-press' # Comma-separated
  newsapi_language: 'en'
  newsapi_sort_by: 'publishedAt' # 'relevancy' or 'publishedAt'
  newsapi_page_size: 20
  enable_news_sentiment_bias: true
  sentiment_score_threshold: 0.1 # Avg score > this = Bullish, < -this = Bearish
  sentiment_keywords_bullish: ['positive', 'strong', 'rally', 'up', 'rise', 'boom', 'optimistic', 'growth']
  sentiment_keywords_bearish: ['negative', 'weak', 'slump', 'down', 'fall', 'crisis', 'pessimistic', 'recession']
  high_impact_news_keywords: ['breaking', 'alert', 'urgent', 'war', 'crash', 'disaster', 'attack', 'invasion']
  news_volatility_window_after_min: 30 # Pause trading for X mins after high-impact news
  # --- Economic Calendar ---
  enable_economic_calendar: false # Enable/disable checking calendar (requires implementation)
  calendar_check_interval_minutes: 60
  calendar_impact_threshold: ['high'] # List: 'high', 'medium', 'low'
  calendar_volatility_window_before_min: 15 # Pause X mins before high impact event
  calendar_volatility_window_after_min: 30 # Pause X mins after high impact event

# --- Risk Management Settings ---
risk:
  sl_atr_multiplier: 1.5 # Stop loss distance = ATR * multiplier
  enable_trailing_stop: true
  ts_atr_multiplier_activate: 1.0 # Profit in ATRs needed to activate TSL
  ts_atr_multiplier_trail: 1.5 # Trail distance in ATRs
  ts_breakeven_buffer_pips: 1.0 # Move SL to BE + X pips when TSL activates

# --- Execution Settings ---
execution:
  enable_auto_trading: false # Master switch for placing trades
  order_retry_count: 2
  order_retry_delay_s: 3

# --- Timing Settings ---
timing:
  check_interval_seconds: 60 # How often the main loop runs
  bars_trend_tf: 300 # Number of bars to fetch/maintain for trend TF
  bars_entry_tf: 300 # Number of bars to fetch/maintain for entry TF
  signal_cooldown_seconds: 180 # Min seconds between generating new signals
  tick_staleness_threshold_s: 30 # Warn if tick data is older than this
  order_confirmation_wait_s: 2.5 # Seconds to wait for position state update after order
  reconnect_attempt_delay_s: 30 # Delay if full connection check fails

# --- Notifications (Telegram) ---
notifications:
  enable_telegram: false # Master switch for Telegram
  telegram_bot_token: "YOUR_TELEGRAM_BOT_TOKEN" # Provide via config or env var TELEGRAM_BOT_TOKEN
  admin_user_ids: [123456789] # List of integer User IDs who can control the bot. MUST set via config or env var TELEGRAM_ADMIN_USER_IDS (comma-separated)
  # telegram_chat_id: "-100..." # Legacy: Can be used for general notifications if needed, but admin_ids preferred
  telegram_send_retries: 3
  telegram_retry_delay_s: 5

# --- Logging & State ---
logging:
  log_level: "INFO" # DEBUG, INFO, WARNING, ERROR, CRITICAL
  data_dir: "trading_bot_data_ctrader_openapi" # Directory for logs, data, state
  log_file: "ctrader_bot.log"
  performance_log_file: "performance.csv"
  state_file: "bot_state.json"

# --- Timezone ---
timezone:
  local_tz: "UTC" # e.g., "Europe/London", "America/New_York", or "UTC"
