#!/usr/bin/env python3
"""
Enhanced Cryptocurrency Trading Bot for Bybit with UTA Support

Features:
- Unified Trading Account (UTA) integration
- Multi-strategy trading (Technical, Trap Reversal)
- Real-time WebSocket integration
- Advanced risk management
- Interactive Telegram interface
- Backtesting with Plotly visualizations
- Prometheus monitoring
"""

import os
import logging
import logging.handlers
import yaml
import pandas as pd
import numpy as np
import requests
import ta
import time
import json
import datetime
import hmac
import hashlib
import urllib.parse
import random
from cryptography.fernet import Fernet
from typing import Dict, Optional, Any, List, Tuple, Union, Callable
from dataclasses import dataclass, asdict
import aiohttp
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from telebot import TeleBot, types
from textblob import TextBlob
from sklearn.preprocessing import MinMaxScaler
from abc import ABC, abstractmethod
import backtrader as bt
import matplotlib.pyplot as plt
from io import BytesIO
import pydantic
from enum import Enum, auto
from functools import wraps, lru_cache
from prometheus_client import start_http_server, Counter, Gauge, Summary, Histogram
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import websockets
from websockets.exceptions import ConnectionClosed
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ====================== Logging Configuration ======================
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": %(message)s}',
    handlers=[
        logging.handlers.RotatingFileHandler(
            "crypto_trading.log",
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("CryptoTrading")

# ====================== Custom Exceptions ======================
class TradingBotError(Exception):
    """Base exception for trading bot errors"""
    pass

class APIError(TradingBotError):
    """API-related exceptions"""
    pass

class RiskManagementError(TradingBotError):
    """Risk management violations"""
    pass

class StrategyError(TradingBotError):
    """Strategy calculation errors"""
    pass

# ====================== Constants & Enums ======================
class MarketType(Enum):
    """Supported market types"""
    FUTURES = auto()
    SPOT = auto()
    UNIFIED = auto()  # Unified Trading Account

class OrderType(Enum):
    """Order execution types"""
    MARKET = auto()
    LIMIT = auto()
    STOP_MARKET = auto()
    TAKE_PROFIT_MARKET = auto()

class SignalDirection(Enum):
    """Trading signal directions"""
    LONG = 1
    SHORT = -1
    NEUTRAL = 0

class TimeFrame(Enum):
    """Supported timeframes for analysis"""
    MIN1 = '1m'
    MIN5 = '5m'
    MIN15 = '15m'
    HOUR1 = '1h'
    HOUR4 = '4h'
    DAY1 = '1d'

class PositionSizingMethod(Enum):
    """Position sizing calculation methods"""
    FIXED_RISK = auto()
    VOLATILITY_ADJUSTED = auto()
    KELLY_CRITERION = auto()
    MARTINGALE = auto()

class MarginMode(Enum):
    """Margin modes for UTA"""
    ISOLATED = auto()
    CROSS = auto()

# ====================== Configuration Models ======================
class APIConfig(pydantic.BaseModel):
    """API connection configuration"""
    key: str = pydantic.Field(..., min_length=10, env="BYBIT_API_KEY")
    secret: str = pydantic.Field(..., min_length=10, env="BYBIT_API_SECRET")
    futures_base_url: str = "https://api.bybit.com"
    spot_base_url: str = "https://api.bybit.com"
    unified_base_url: str = "https://api.bybit.com"  # UTA endpoint
    websocket_url: str = "wss://stream.bybit.com/v5/public/linear"
    private_websocket_url: str = "wss://stream.bybit.com/v5/private"
    unified_websocket_url: str = "wss://stream.bybit.com/v5/private"  # UTA WS
    rate_limit: int = 1200
    request_timeout: int = 15

class BotConfig(pydantic.BaseModel):
    """Telegram bot configuration"""
    telegram_token: str = pydantic.Field(..., min_length=10, env="TELEGRAM_TOKEN")
    authorized_users: List[int]
    alert_cooldown: int = 60
    max_alert_per_user: int = 5
    enable_plotly: bool = True
    admin_user_ids: List[int] = []

class NewsConfig(pydantic.BaseModel):
    """News sentiment analysis configuration"""
    api_key: str = pydantic.Field(..., min_length=10, env="NEWS_API_KEY")
    max_articles: int = 10
    sentiment_weight: float = 0.1
    sources: List[str] = ['bloomberg', 'coindesk', 'cointelegraph']
    min_article_confidence: float = 0.7

class TradingConfig(pydantic.BaseModel):
    """Trading strategy configuration"""
    enable_futures: bool = True
    enable_spot: bool = False
    enable_auto_trading: bool = False
    max_risk_percentage: float = pydantic.Field(1.0, ge=0.1, le=5.0)
    min_risk_percentage: float = pydantic.Field(0.5, ge=0.1, le=5.0)
    max_leverage: int = pydantic.Field(20, ge=1, le=100)
    min_leverage: int = pydantic.Field(5, ge=1, le=100)
    default_stop_loss_pct: float = pydantic.Field(3.0, ge=0.1, le=20.0)
    default_take_profit_pct: float = pydantic.Field(6.0, ge=0.1, le=20.0)
    auto_trade_interval: int = 300
    min_signal_strength: float = pydantic.Field(0.5, ge=0.1, le=1.0)
    max_positions: int = pydantic.Field(10, ge=1, le=50)
    enable_dynamic_risk: bool = True
    enable_news_sentiment: bool = True
    enable_technical_analysis: bool = True
    dry_run_mode: bool = True
    coins_per_cycle: int = pydantic.Field(20, ge=1, le=100)
    analysis_interval: int = 240
    position_monitor_interval: int = 120
    trailing_stop_enabled: bool = True
    trailing_stop_distance_pct: float = 1.0
    blacklist_cooldown: int = 3600
    enable_trap_detection: bool = True
    trap_detection_threshold: float = 1.5
    trap_volume_ratio: float = 0.7
    trap_oi_spike: float = 1.3
    position_sizing_method: PositionSizingMethod = PositionSizingMethod.FIXED_RISK
    kelly_leverage_factor: float = pydantic.Field(0.5, ge=0.1, le=1.0)
    martingale_factor: float = pydantic.Field(1.5, ge=1.1, le=2.0)
    default_margin_mode: MarginMode = MarginMode.ISOLATED

# ====================== Core Components ======================
class CircuitBreaker:
    """Circuit breaker pattern for API and strategy failures"""
    def __init__(self, max_failures: int = 5, reset_timeout: int = 300):
        self.max_failures = max_failures
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.last_failure_time = 0
        self.tripped = False
        self.lock = threading.Lock()

    def check(self) -> bool:
        """Check if circuit breaker is tripped"""
        with self.lock:
            if self.tripped:
                if time.time() - self.last_failure_time > self.reset_timeout:
                    self.tripped = False
                    self.failures = 0
                    return True
                return False
            return True

    def record_failure(self):
        """Record a failure and potentially trip the circuit"""
        with self.lock:
            self.failures += 1
            self.last_failure_time = time.time()
            if self.failures >= self.max_failures:
                self.tripped = True
                logger.error(f"Circuit breaker tripped after {self.failures} failures")

    def reset(self):
        """Manually reset the circuit breaker"""
        with self.lock:
            self.failures = 0
            self.tripped = False

class DataCache:
    """LRU cache for market data with TTL support"""
    def __init__(self, max_size_mb: int = 50):
        self.cache = {}
        self.expiry = {}
        self.max_size = max_size_mb * 1024 * 1024
        self.current_size = 0
        self.lock = threading.Lock()

    @lru_cache(maxsize=1000)
    def get(self, key: str) -> Optional[Any]:
        """Get cached item if exists and not expired"""
        with self.lock:
            if key in self.expiry and time.time() < self.expiry[key]:
                return self.cache.get(key)
            return None

    def set(self, key: str, value: Any, ttl: int = 300):
        """Add item to cache with TTL"""
        with self.lock:
            value_size = len(str(value).encode())
            if self.current_size + value_size > self.max_size:
                self._evict()
            self.cache[key] = value
            self.expiry[key] = time.time() + ttl
            self.current_size += value_size

    def _evict(self):
        """Evict oldest item when cache is full"""
        oldest_key = min(self.expiry.items(), key=lambda x: x[1])[0]
        self.current_size -= len(str(self.cache[oldest_key]).encode())
        del self.cache[oldest_key]
        del self.expiry[oldest_key]

class TradingMetrics:
    """Prometheus metrics collector for trading operations"""
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(TradingMetrics, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        # Trading metrics
        self.trades_executed = Counter('trades_executed_total', 'Number of trades executed', ['market', 'type'])
        self.api_errors = Counter('api_errors_total', 'API errors encountered')
        self.position_gauge = Gauge('open_positions', 'Current open positions')
        self.balance_gauge = Gauge('account_balance', 'Current account balance', ['currency'])
        
        # Performance metrics
        self.trade_latency = Histogram('trade_execution_latency_seconds', 'Trade execution latency in seconds', buckets=[0.1, 0.5, 1, 2, 5])
        self.signal_strength = Gauge('signal_strength', 'Current signal strength', ['symbol'])
        self.trap_signals = Counter('trap_signals_total', 'Trap signals detected', ['symbol', 'direction'])
        
        # Risk metrics
        self.risk_exposure = Gauge('risk_exposure', 'Current risk exposure percentage')
        self.drawdown = Gauge('max_drawdown', 'Maximum drawdown percentage')
        
        # UTA specific metrics
        self.uta_balance = Gauge('uta_balance', 'UTA account balance', ['coin'])
        self.uta_margin = Gauge('uta_margin', 'UTA margin usage', ['coin'])
        
        self._initialized = True

# ====================== Trading Strategies ======================
class BaseStrategy(ABC):
    """Abstract base class for trading strategies"""
    def __init__(self):
        self.circuit_breaker = CircuitBreaker()
        self.metrics = TradingMetrics()
        self.last_price = {}

    @abstractmethod
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate trading signals from market data"""
        pass

    @abstractmethod
    def calculate_weights(self) -> Dict[str, float]:
        """Calculate indicator weights for signal generation"""
        pass

    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare market data for analysis"""
        df = df.copy()
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df.fillna(method='ffill', inplace=True)
        df.fillna(method='bfill', inplace=True)
        return df

class TechnicalStrategy(BaseStrategy):
    """Technical analysis-based trading strategy"""
    def __init__(self, weights: Optional[Dict[str, float]] = None):
        super().__init__()
        self.weights = weights or self.calculate_weights()
        self.indicators = {
            'rsi': lambda df: ta.momentum.RSIIndicator(df['close']).rsi(),
            'macd': lambda df: ta.trend.MACD(df['close']).macd_diff(),
            'sma': lambda df: (ta.trend.SMAIndicator(df['close'], window=20).sma_indicator(),
                              ta.trend.SMAIndicator(df['close'], window=50).sma_indicator()),
            'bb': lambda df: ta.volatility.BollingerBands(df['close']),
            'volume': lambda df: df['volume'].rolling(20).mean(),
            'atr': lambda df: ta.volatility.AverageTrueRange(df['high'], df['low'], df['close'], window=14).average_true_range(),
            'obv': lambda df: ta.volume.OnBalanceVolumeIndicator(df['close'], df['volume']).on_balance_volume(),
            'stoch': lambda df: ta.momentum.StochasticOscillator(df['high'], df['low'], df['close']).stoch(),
            'ichimoku': lambda df: self._calculate_ichimoku(df),
            'vwap': lambda df: self._calculate_vwap(df)
        }

    def _calculate_ichimoku(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate Ichimoku Cloud components"""
        high = df['high']
        low = df['low']
        close = df['close']
        
        # Tenkan-sen (Conversion Line)
        tenkan_period = 9
        tenkan_high = high.rolling(window=tenkan_period).max()
        tenkan_low = low.rolling(window=tenkan_period).min()
        tenkan_sen = (tenkan_high + tenkan_low) / 2
        
        # Kijun-sen (Base Line)
        kijun_period = 26
        kijun_high = high.rolling(window=kijun_period).max()
        kijun_low = low.rolling(window=kijun_period).min()
        kijun_sen = (kijun_high + kijun_low) / 2
        
        # Senkou Span A (Leading Span A)
        senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(kijun_period)
        
        return tenkan_sen, kijun_sen, senkou_span_a

    def _calculate_vwap(self, df: pd.DataFrame) -> pd.Series:
        """Calculate Volume Weighted Average Price"""
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        vwap = (typical_price * df['volume']).cumsum() / df['volume'].cumsum()
        return vwap

    def calculate_weights(self) -> Dict[str, float]:
        """Default weights for technical indicators"""
        return {
            'rsi': 0.2,
            'macd': 0.2,
            'sma': 0.15,
            'bb': 0.15,
            'volume': 0.1,
            'obv': 0.05,
            'stoch': 0.05,
            'ichimoku': 0.05,
            'vwap': 0.05
        }

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate trading signals using multiple technical indicators"""
        if not self.circuit_breaker.check():
            df['signal'] = SignalDirection.NEUTRAL.value
            return df

        try:
            df = self.preprocess_data(df)
            
            # Calculate all indicators
            for name, func in self.indicators.items():
                if name == 'sma':
                    df['sma_20'], df['sma_50'] = func(df)
                elif name == 'bb':
                    bb = func(df)
                    df['bb_upper'], df['bb_middle'], df['bb_lower'] = bb.bollinger_hband(), bb.bollinger_mavg(), bb.bollinger_lband()
                elif name == 'ichimoku':
                    df['tenkan'], df['kijun'], df['senkou_a'] = func(df)
                else:
                    df[name] = func(df)
            
            # Generate scores for each indicator
            scores = {
                'rsi': np.where(df['rsi'] < 30, 1, np.where(df['rsi'] > 70, -1, 0)),
                'macd': np.where(df['macd'] > 0, 1, np.where(df['macd'] < 0, -1, 0)),
                'sma': np.where(df['sma_20'] > df['sma_50'], 1, np.where(df['sma_20'] < df['sma_50'], -1, 0)),
                'bb': np.where(df['close'] < df['bb_lower'], 1, np.where(df['close'] > df['bb_upper'], -1, 0)),
                'volume': np.where(df['volume'] > df['volume'].rolling(20).mean(), 1, 0),
                'obv': np.where(df['obv'] > df['obv'].shift(1), 1, -1),
                'stoch': np.where((df['stoch'] < 20) & (df['stoch'].shift(1) < 20), 1, 
                                np.where((df['stoch'] > 80) & (df['stoch'].shift(1) > 80), -1, 0)),
                'ichimoku': np.where((df['close'] > df['senkou_a']) & (df['tenkan'] > df['kijun']), 1,
                                    np.where((df['close'] < df['senkou_a']) & (df['tenkan'] < df['kijun']), -1, 0)),
                'vwap': np.where(df['close'] > df['vwap'], 1, np.where(df['close'] < df['vwap'], -1, 0))
            }
            
            # Calculate weighted signal
            df['signal'] = sum(scores[ind] * weight for ind, weight in self.weights.items())
            df['signal_strength'] = abs(df['signal'])
            
            # Apply threshold
            df['signal'] = np.where(
                df['signal'] > self.weights.get('signal_threshold', 0.5), SignalDirection.LONG.value,
                np.where(df['signal'] < -self.weights.get('signal_threshold', 0.5), SignalDirection.SHORT.value, 
                        SignalDirection.NEUTRAL.value)
            )
            
            return df
        except Exception as e:
            logger.error(f"Signal generation failed: {str(e)}", exc_info=True)
            self.circuit_breaker.record_failure()
            df['signal'] = SignalDirection.NEUTRAL.value
            return df

class TrapReversalStrategy(BaseStrategy):
    """Detects and trades trap/reversal patterns"""
    def __init__(self, config: TradingConfig):
        super().__init__()
        self.config = config
        self.normal_volume = {}
        self.normal_oi = {}
        self.trap_signals = {}
        self.lock = threading.Lock()
        self.symbols = [f"{coin}USDT" for coin in config.coins] if hasattr(config, 'coins') else []
        self.initialize_symbol_data()

    def initialize_symbol_data(self):
        """Initialize tracking data for each symbol"""
        for symbol in self.symbols:
            self.normal_volume[symbol] = 0
            self.normal_oi[symbol] = 0
            self.last_price[symbol] = 0

    def calculate_weights(self) -> Dict[str, float]:
        """Trap strategy doesn't use weights"""
        return {}

    def detect_trap(self, symbol: str, price: float, volume: float, oi: float, rsi: float) -> Optional[SignalDirection]:
        """Detect potential trap/reversal patterns"""
        with self.lock:
            if symbol not in self.normal_volume:
                self.normal_volume[symbol] = volume
                self.normal_oi[symbol] = oi
                self.last_price[symbol] = price
                return None

            price_change_pct = abs((price - self.last_price[symbol]) / self.last_price[symbol]) * 100

            if (price_change_pct >= self.config.trap_detection_threshold and
                volume < self.normal_volume[symbol] * self.config.trap_volume_ratio and
                oi > self.normal_oi[symbol] * self.config.trap_oi_spike):

                if price > self.last_price[symbol]:
                    round_number = round(price / 1000) * 1000
                    if abs(price - round_number) < (round_number * 0.001):
                        self.metrics.trap_signals.labels(symbol=symbol, direction='SHORT').inc()
                        return SignalDirection.SHORT
                else:
                    round_number = round(price / 1000) * 1000
                    if abs(price - round_number) < (round_number * 0.001):
                        self.metrics.trap_signals.labels(symbol=symbol, direction='LONG').inc()
                        return SignalDirection.LONG

            # Update normal levels with EMA
            self.normal_volume[symbol] = (self.normal_volume[symbol] * 0.9) + (volume * 0.1)
            self.normal_oi[symbol] = (self.normal_oi[symbol] * 0.9) + (oi * 0.1)
            self.last_price[symbol] = price

            return None

    def generate_signals(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Generate signals based on trap detection"""
        if not self.circuit_breaker.check():
            df['signal'] = SignalDirection.NEUTRAL.value
            return df

        try:
            df = self.preprocess_data(df)
            df['rsi'] = ta.momentum.RSIIndicator(df['close']).rsi()
            df['signal'] = SignalDirection.NEUTRAL.value
            
            for i in range(max(0, len(df)-5), len(df)):
                row = df.iloc[i]
                signal = self.detect_trap(
                    symbol=symbol,
                    price=row['close'],
                    volume=row['volume'],
                    oi=row.get('open_interest', 0),
                    rsi=row['rsi']
                )
                
                if signal is not None:
                    df.at[df.index[i], 'signal'] = signal.value
                    df.at[df.index[i], 'signal_strength'] = 1.0
            
            return df
        except Exception as e:
            logger.error(f"Trap signal generation failed for {symbol}: {str(e)}", exc_info=True)
            self.circuit_breaker.record_failure()
            df['signal'] = SignalDirection.NEUTRAL.value
            return df

# ====================== Risk Manager ======================
class RiskManager:
    """Advanced risk management system"""
    def __init__(self, config: TradingConfig):
        self.config = config
        self.trade_metrics = {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'total_profit': 0.0,
            'total_loss': 0.0,
            'consecutive_wins': 0,
            'consecutive_losses': 0,
            'last_trade_result': None
        }
        self.blacklist = {}
        self.lock = threading.Lock()

    def calculate_position_size(self, balance: float, risk_pct: float, 
                              entry_price: float, stop_loss_price: float,
                              method: PositionSizingMethod = None) -> float:
        """
        Calculate optimal position size based on risk parameters
        
        Args:
            balance: Current account balance
            risk_pct: Risk percentage per trade
            entry_price: Entry price for the trade
            stop_loss_price: Stop loss price
            method: Position sizing method to use
            
        Returns:
            float: Position size in units
        """
        if not method:
            method = self.config.position_sizing_method
        
        risk_amount = balance * (risk_pct / 100)
        risk_per_unit = abs(entry_price - stop_loss_price)
        
        if method == PositionSizingMethod.FIXED_RISK:
            position_size = risk_amount / risk_per_unit
            
        elif method == PositionSizingMethod.VOLATILITY_ADJUSTED:
            volatility = self.calculate_volatility(pd.DataFrame({
                'high': [entry_price * 1.01],
                'low': [entry_price * 0.99],
                'close': [entry_price]
            }))
            adj_factor = max(0.5, min(2.0, 1.5 - (volatility / 10)))
            position_size = (risk_amount / risk_per_unit) * adj_factor
            
        elif method == PositionSizingMethod.KELLY_CRITERION:
            win_rate = self.get_win_rate() / 100
            avg_win = self.trade_metrics['total_profit'] / self.trade_metrics['winning_trades'] if self.trade_metrics['winning_trades'] > 0 else 1
            avg_loss = abs(self.trade_metrics['total_loss'] / self.trade_metrics['losing_trades']) if self.trade_metrics['losing_trades'] > 0 else 1
            kelly_f = win_rate - ((1 - win_rate) / (avg_win / avg_loss))
            kelly_f = max(0, kelly_f) * self.config.kelly_leverage_factor
            position_size = (balance * kelly_f) / risk_per_unit
            
        elif method == PositionSizingMethod.MARTINGALE:
            if self.trade_metrics['last_trade_result'] == 'loss':
                martingale_multiplier = self.config.martingale_factor ** self.trade_metrics['consecutive_losses']
                position_size = (risk_amount / risk_per_unit) * martingale_multiplier
            else:
                position_size = risk_amount / risk_per_unit
        else:
            position_size = risk_amount / risk_per_unit
            
        return max(position_size, 0.0)

    def calculate_dynamic_leverage(self, volatility: float) -> int:
        """Adjust leverage based on market volatility"""
        if volatility < 1.0:
            return min(self.config.max_leverage, 20)
        elif volatility < 3.0:
            return min(self.config.max_leverage, 15)
        else:
            return max(self.config.min_leverage, 5)
    
    def calculate_volatility(self, df: pd.DataFrame) -> float:
        """Calculate market volatility using ATR"""
        atr = ta.volatility.AverageTrueRange(df['high'], df['low'], df['close'], window=14).average_true_range().iloc[-1]
        price = df['close'].iloc[-1]
        return (atr / price) * 100

    def update_trade_metrics(self, profitable: bool, amount: float):
        """Update trade performance metrics"""
        with self.lock:
            self.trade_metrics['total_trades'] += 1
            if profitable:
                self.trade_metrics['winning_trades'] += 1
                self.trade_metrics['total_profit'] += amount
                self.trade_metrics['consecutive_wins'] += 1
                self.trade_metrics['consecutive_losses'] = 0
                self.trade_metrics['last_trade_result'] = 'win'
            else:
                self.trade_metrics['losing_trades'] += 1
                self.trade_metrics['total_loss'] += amount
                self.trade_metrics['consecutive_losses'] += 1
                self.trade_metrics['consecutive_wins'] = 0
                self.trade_metrics['last_trade_result'] = 'loss'

    def get_win_rate(self) -> float:
        """Calculate current win rate percentage"""
        if self.trade_metrics['total_trades'] == 0:
            return 0.0
        return (self.trade_metrics['winning_trades'] / self.trade_metrics['total_trades']) * 100

    def get_profit_factor(self) -> float:
        """Calculate profit factor (gross profit / gross loss)"""
        if self.trade_metrics['total_loss'] == 0:
            return float('inf')
        return self.trade_metrics['total_profit'] / self.trade_metrics['total_loss']

    def add_to_blacklist(self, symbol: str):
        """Add symbol to trading blacklist"""
        with self.lock:
            self.blacklist[symbol] = time.time() + self.config.blacklist_cooldown

    def is_blacklisted(self, symbol: str) -> bool:
        """Check if symbol is currently blacklisted"""
        with self.lock:
            expiry = self.blacklist.get(symbol, 0)
            if time.time() < expiry:
                return True
            if expiry != 0:
                del self.blacklist[symbol]
            return False

# ====================== API Client ======================
class BybitAPIClient:
    """Bybit API client with UTA support"""
    def __init__(self, config: APIConfig, market_type: MarketType):
        self.config = config
        self.market_type = market_type
        # Use unified endpoints for all account types
        self.base_url = config.unified_base_url
        self.ws_url = config.unified_websocket_url if market_type in [MarketType.FUTURES, MarketType.SPOT] else config.websocket_url
        self.api_key = config.key
        self.api_secret = config.secret
        self.rate_limit = config.rate_limit
        self.request_timeout = config.request_timeout
        self.api_calls = 0
        self.last_reset = time.time()
        self.session = requests.Session()
        self.session.headers.update({
            'X-BAPI-API-KEY': self.api_key,
            'Content-Type': 'application/json',
            'X-BAPI-RECV-WINDOW': '5000',
            'User-Agent': 'Bybit-Trading-Bot/1.0'
        })
        self.circuit_breaker = CircuitBreaker()
        self.metrics = TradingMetrics()
    
    def _sign_request(self, params: Dict[str, Any]) -> str:
        """Generate API request signature"""
        timestamp = str(int(time.time() * 1000))
        params['timestamp'] = timestamp
        query_string = urllib.parse.urlencode(sorted(params.items()))
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    @retry(stop=stop_after_attempt(3),
          wait=wait_exponential(multiplier=1, min=2, max=10),
          retry=retry_if_exception_type((requests.RequestException,)))
    def request(self, method: str, endpoint: str, params: Dict = None, 
               auth_required: bool = False) -> Optional[Dict]:
        """Make API request with rate limiting and retry logic"""
        if not self.circuit_breaker.check():
            return None

        current_time = time.time()
        if current_time - self.last_reset > 60:
            self.api_calls = 0
            self.last_reset = current_time
        
        if self.api_calls >= self.rate_limit:
            sleep_time = 60 - (current_time - self.last_reset)
            logger.warning(f"Rate limit reached, sleeping for {sleep_time:.1f}s")
            time.sleep(sleep_time)
            self.api_calls = 0
            self.last_reset = time.time()
        
        self.api_calls += 1
        
        url = f"{self.base_url}{endpoint}"
        params = params or {}
        
        if auth_required:
            params['api_key'] = self.api_key
            params['sign'] = self._sign_request(params)
        
        try:
            start_time = time.time()
            
            if method == 'GET':
                response = self.session.get(
                    url, 
                    params=params, 
                    timeout=self.request_timeout
                )
            else:
                response = self.session.post(
                    url, 
                    json=params, 
                    timeout=self.request_timeout
                )
            
            latency = time.time() - start_time
            self.metrics.trade_latency.observe(latency)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 2))
                logger.warning(f"Rate limited, retrying after {retry_after}s")
                time.sleep(retry_after)
                raise requests.exceptions.RetryError("Rate limited")
            
            if response.status_code != 200:
                self.metrics.api_errors.inc()
                logger.error(f"API error: {response.status_code} - {response.text}")
                return None
            
            return response.json()
        
        except requests.RequestException as e:
            self.metrics.api_errors.inc()
            logger.error(f"Request error: {e}")
            self.circuit_breaker.record_failure()
            raise

    async def get_historical_data(self, symbol: str, interval: str, days: int) -> Optional[pd.DataFrame]:
        """Fetch historical market data asynchronously"""
        try:
            endpoint = "/v5/market/kline"
            params = {
                'category': 'linear',
                'symbol': symbol,
                'interval': interval,
                'limit': min(days * 1440, 1000)  # Max 1000 candles
            }
            
            response = await asyncio.to_thread(
                self.request, 'GET', endpoint, params
            )
            
            if not response or 'result' not in response:
                return None
                
            data = response['result']['list']
            df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
            return df
            
        except Exception as e:
            logger.error(f"Failed to fetch historical data for {symbol}: {e}")
            return None

    def execute_trade(self, symbol: str, side: str, quantity: float, 
                     leverage: int = 10, price: Optional[float] = None,
                     stop_loss: Optional[float] = None, 
                     take_profit: Optional[float] = None,
                     trailing_stop: bool = False,
                     category: str = "linear",
                     reduce_only: bool = False) -> bool:
        """Execute a trade through UTA"""
        if self.config.trading.dry_run_mode:
            logger.info(f"DRY RUN: Would execute {side} {quantity} of {symbol} in {category}")
            return True
            
        try:
            endpoint = "/v5/order/create"
            params = {
                'category': category,
                'symbol': symbol,
                'side': side.capitalize(),
                'orderType': 'Market' if price is None else 'Limit',
                'qty': str(quantity),
                'timeInForce': 'GTC',
                'reduceOnly': reduce_only,
                'closeOnTrigger': False,
                'leverage': str(leverage)
            }
            
            if price is not None:
                params['price'] = str(price)
                
            if stop_loss is not None:
                params['stopLoss'] = str(stop_loss)
                
            if take_profit is not None:
                params['takeProfit'] = str(take_profit)
                
            if trailing_stop:
                params['trailingStop'] = str(self.config.trading.trailing_stop_distance_pct * 100)  # Convert to basis points
                
            response = self.request('POST', endpoint, params, auth_required=True)
            
            if not response or 'retCode' not in response or response['retCode'] != 0:
                logger.error(f"Trade execution failed: {response}")
                return False
                
            logger.info(f"Executed {side} order for {quantity} {symbol} in {category}")
            self.metrics.trades_executed.labels(market=category, type='executed').inc()
            return True
            
        except Exception as e:
            logger.error(f"Trade execution failed: {e}")
            return False

    def get_account_balance(self) -> Dict[str, float]:
        """Get current account balances under UTA"""
        try:
            endpoint = "/v5/account/wallet-balance"
            params = {
                'accountType': 'UNIFIED'
            }
            response = self.request('GET', endpoint, params, auth_required=True)
            
            if not response or 'result' not in response:
                return {}
                
            balances = {}
            for asset in response['result']['list'][0]['coin']:
                if asset['coin'] == 'USDT':
                    balances[asset['coin']] = float(asset['availableToWithdraw'])
                    self.metrics.uta_balance.labels(coin=asset['coin']).set(float(asset['equity']))
                    self.metrics.uta_margin.labels(coin=asset['coin']).set(float(asset['usedMargin']))
                    
            return balances
            
        except Exception as e:
            logger.error(f"Failed to fetch account balance: {e}")
            return {}

    def get_positions(self, category: str = "linear") -> List[Dict]:
        """Get current positions under UTA"""
        try:
            endpoint = "/v5/position/list"
            params = {
                'category': category
            }
            response = self.request('GET', endpoint, params, auth_required=True)
            
            if not response or 'result' not in response:
                return []
                
            return response['result']['list']
            
        except Exception as e:
            logger.error(f"Failed to fetch positions: {e}")
            return []

    def set_margin_mode(self, symbol: str, mode: MarginMode) -> bool:
        """Set margin mode for a position (ISOLATED or CROSS)"""
        try:
            endpoint = "/v5/position/set-auto-add-margin"
            params = {
                'category': 'linear',
                'symbol': symbol,
                'autoAddMargin': 1 if mode == MarginMode.CROSS else 0
            }
            response = self.request('POST', endpoint, params, auth_required=True)
            return response and 'retCode' in response and response['retCode'] == 0
        except Exception as e:
            logger.error(f"Failed to set margin mode: {e}")
            return False

# ====================== News Sentiment Analysis ======================
class NewsSentimentAnalyzer:
    """Analyzes news sentiment for trading signals"""
    def __init__(self, config: NewsConfig):
        self.config = config
        self.cache = DataCache(max_size_mb=10)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Crypto-Trading-Bot/1.0'
        })

    def get_news_sentiment(self, symbol: str) -> float:
        """Get sentiment score for a cryptocurrency (-1 to 1)"""
        cache_key = f"news:{symbol}"
        cached_sentiment = self.cache.get(cache_key)
        if cached_sentiment is not None:
            return cached_sentiment
            
        try:
            coin_name = symbol.replace('USDT', '')
            url = "https://newsapi.org/v2/everything"
            params = {
                'q': coin_name,
                'language': 'en',
                'sortBy': 'relevancy',
                'pageSize': self.config.max_articles,
                'apiKey': self.config.api_key
            }
            
            response = self.session.get(url, params=params, timeout=15)
            if response.status_code != 200:
                logger.error(f"News API error: {response.status_code} - {response.text}")
                return 0.0
                
            news = response.json()
            
            if not news.get('articles'):
                return 0.0
                
            sentiments = []
            for article in news['articles']:
                if article.get('description') and article.get('content'):
                    text = f"{article['title']} {article['description']} {article['content']}"
                    analysis = TextBlob(text)
                    if analysis.sentiment.subjectivity >= self.config.min_article_confidence:
                        sentiments.append(analysis.sentiment.polarity)
            
            if not sentiments:
                return 0.0
                
            avg_sentiment = np.mean(sentiments)
            self.cache.set(cache_key, avg_sentiment, ttl=1800)
            return np.clip(avg_sentiment, -1, 1)
        except Exception as e:
            logger.error(f"News sentiment error: {e}")
            return 0.0

# ====================== WebSocket Manager ======================
class WebSocketManager:
    """Manages real-time WebSocket connections for UTA"""
    def __init__(self, config: APIConfig, market_type: MarketType):
        self.config = config
        self.market_type = market_type
        # Use unified WebSocket URL for all account types
        self.ws_url = config.unified_websocket_url if market_type in [MarketType.FUTURES, MarketType.SPOT] else config.websocket_url
        self.private_ws_url = config.private_websocket_url
        self.connections = {}
        self.subscriptions = set()
        self.callbacks = {}
        self.running = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 5
        self.lock = threading.Lock()
        self.heartbeat_interval = 30
        self.last_heartbeat = time.time()

    async def connect(self, topics: List[str], callback: Callable):
        """Connect to WebSocket and subscribe to topics"""
        if not topics:
            return

        ws_key = ",".join(sorted(topics))
        if ws_key in self.connections:
            return

        try:
            self.callbacks[ws_key] = callback
            self.subscriptions.update(topics)
            
            ws = await websockets.connect(self.ws_url, ping_interval=self.heartbeat_interval)
            self.connections[ws_key] = ws
            self.reconnect_attempts = 0
            
            # UTA subscription format
            subscription_msg = {
                "op": "subscribe",
                "args": topics
            }
            await ws.send(json.dumps(subscription_msg))
            
            asyncio.create_task(self._listen(ws_key))
            
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            await self._reconnect(ws_key)

    async def _listen(self, ws_key: str):
        """Listen for incoming WebSocket messages"""
        ws = self.connections.get(ws_key)
        if not ws:
            return

        try:
            while self.running:
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=self.heartbeat_interval * 2)
                    self.last_heartbeat = time.time()
                    
                    data = json.loads(message)
                    if 'topic' in data:
                        await self.callbacks[ws_key](data)
                    elif 'pong' in data:
                        continue
                    elif 'success' in data and data['success']:
                        logger.debug(f"WebSocket subscription confirmed: {data}")
                    else:
                        logger.debug(f"Unknown WebSocket message: {data}")

                except asyncio.TimeoutError:
                    await ws.send(json.dumps({"op": "ping"}))
                    if time.time() - self.last_heartbeat > self.heartbeat_interval * 3:
                        raise ConnectionError("Heartbeat timeout")

        except ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}")
            await self._reconnect(ws_key)
        except Exception as e:
            logger.error(f"WebSocket listener error: {e}")
            await self._reconnect(ws_key)

    async def _reconnect(self, ws_key: str):
        """Handle WebSocket reconnection logic"""
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            logger.error(f"Max reconnection attempts reached for {ws_key}")
            return

        self.reconnect_attempts += 1
        delay = self.reconnect_delay * self.reconnect_attempts
        logger.info(f"Reconnecting in {delay} seconds (attempt {self.reconnect_attempts})")
        
        await asyncio.sleep(delay)
        topics = ws_key.split(",")
        await self.connect(topics, self.callbacks[ws_key])

    async def close(self):
        """Close all WebSocket connections"""
        self.running = False
        for ws in self.connections.values():
            await ws.close()
        self.connections.clear()
        self.subscriptions.clear()

# ====================== Core Trading Engine ======================
class CryptoTradingEngine:
    """Main trading engine coordinating all components with UTA support"""
    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self._initialize_encryption()
        
        # Initialize components
        self.cache = DataCache(max_size_mb=100)
        self.risk_manager = RiskManager(self.config.trading)
        self.strategy = TechnicalStrategy()
        self.trap_strategy = TrapReversalStrategy(self.config.trading) if self.config.trading.enable_trap_detection else None
        self.news_analyzer = NewsSentimentAnalyzer(self.config.news)
        self.metrics = TradingMetrics()
        
        # Initialize API client with UTA support
        self.client = BybitAPIClient(self.config.api, MarketType.UNIFIED)
        
        # Initialize WebSocket manager with UTA support
        self.ws_manager = WebSocketManager(self.config.api, MarketType.UNIFIED)
        
        # Active positions and metrics
        self.active_positions = {}  # Simplified under UTA
        self.trade_history = []
        self.trading_enabled = False
        self.last_analysis_time = 0
        self.last_position_check_time = 0
        self.lock = threading.Lock()
        
        # Start monitoring threads
        if self.config.trading.enable_auto_trading:
            self.executor = ThreadPoolExecutor(max_workers=4)
            self.executor.submit(self.auto_trade_loop)
            self.executor.submit(self.monitor_market_conditions)
            if self.trap_strategy:
                self.executor.submit(self.monitor_trap_signals)
            
            # Start WebSocket connections
            asyncio.run(self.initialize_websockets())

    def _load_config(self, config_path: str) -> pydantic.BaseModel:
        """Load and validate configuration"""
        class FullConfig(pydantic.BaseModel):
            api: APIConfig
            bot: BotConfig
            trading: TradingConfig
            news: NewsConfig
            coins: List[str]
        
        if not os.path.exists(config_path):
            default_config = {
                'api': {
                    'key': os.getenv('BYBIT_API_KEY', 'your_api_key_here'),
                    'secret': os.getenv('BYBIT_API_SECRET', 'your_api_secret_here'),
                    'futures_base_url': "https://api.bybit.com",
                    'spot_base_url': "https://api.bybit.com",
                    'unified_base_url': "https://api.bybit.com",
                    'websocket_url': "wss://stream.bybit.com/v5/public/linear",
                    'private_websocket_url': "wss://stream.bybit.com/v5/private",
                    'unified_websocket_url': "wss://stream.bybit.com/v5/private",
                    'rate_limit': 1200,
                    'request_timeout': 15
                },
                'bot': {
                    'telegram_token': os.getenv('TELEGRAM_TOKEN', 'your_telegram_token'),
                    'authorized_users': [int(os.getenv('AUTHORIZED_USER_ID', '0'))],
                    'alert_cooldown': 60,
                    'max_alert_per_user': 5,
                    'enable_plotly': True,
                    'admin_user_ids': []
                },
                'trading': {
                    'enable_futures': True,
                    'enable_spot': False,
                    'enable_auto_trading': False,
                    'max_risk_percentage': 1.0,
                    'min_risk_percentage': 0.5,
                    'max_leverage': 20,
                    'min_leverage': 5,
                    'default_stop_loss_pct': 3.0,
                    'default_take_profit_pct': 6.0,
                    'auto_trade_interval': 300,
                    'min_signal_strength': 0.5,
                    'max_positions': 10,
                    'enable_dynamic_risk': True,
                    'enable_news_sentiment': True,
                    'enable_technical_analysis': True,
                    'dry_run_mode': True,
                    'coins_per_cycle': 20,
                    'analysis_interval': 240,
                    'position_monitor_interval': 120,
                    'trailing_stop_enabled': True,
                    'trailing_stop_distance_pct': 1.0,
                    'blacklist_cooldown': 3600,
                    'enable_trap_detection': True,
                    'trap_detection_threshold': 1.5,
                    'trap_volume_ratio': 0.7,
                    'trap_oi_spike': 1.3,
                    'position_sizing_method': "FIXED_RISK",
                    'kelly_leverage_factor': 0.5,
                    'martingale_factor': 1.5,
                    'default_margin_mode': "ISOLATED"
                },
                'news': {
                    'api_key': os.getenv('NEWS_API_KEY', 'your_news_api_key'),
                    'max_articles': 10,
                    'sentiment_weight': 0.1,
                    'sources': ['bloomberg', 'coindesk', 'cointelegraph'],
                    'min_article_confidence': 0.7
                },
                'coins': [
                    "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "DOT", "SHIB", "MATIC",
                    "TRX", "AVAX", "LINK", "ATOM", "UNI", "XLM", "XMR", "ETC", "ICP", "FIL",
                    "LTC", "BCH", "XMR", "EOS", "AAVE", "ALGO", "AXS", "BAT", "COMP", "CRV",
                    "DASH", "ENJ", "FTM", "GRT", "HBAR", "HNT", "KSM", "MANA", "MKR", "NEAR",
                    "OCEAN", "OMG", "QTUM", "REN", "SNX", "SUSHI", "SXP", "THETA", "UMA", "YFI",
                    "ZEC", "ZIL", "ZRX", "1INCH", "AUDIO", "BAL", "BAND", "BNT", "CELO", "CHZ",
                    "COTI", "CVC", "DENT", "DODO", "EGLD", "FET", "FLOW", "GALA", "GLM", "IOST",
                    "IOTA", "JST", "KAVA", "KNC", "LINA", "LRC", "LUNA", "MIR", "NKN", "NMR",
                    "OGN", "ONT", "OXT", "PERP", "POLY", "POWR", "QUICK", "REEF", "RLC", "ROSE",
                    "RSR", "RUNE", "SC", "SKL", "SLP", "SNT", "STORJ", "SUN", "SUSHI", "SWAP",
                    "SXP", "TOMO", "TRB", "TWT", "UMA", "UNFI", "WAVES", "WAXP", "XEM", "XVS",
                    "YFII", "ZEN", "ANKR", "AR", "BADGER", "BETA", "CELR", "DGB", "ELF", "FORTH",
                    "ICX", "KEY", "LIT", "MDX", "NEO", "ORN", "POND", "PUNDIX", "QI", "RAMP",
                    "REP", "STMX", "SUPER", "SYS", "TLM", "TRIBE", "VET", "WRX"
                ]
            }
            with open(config_path, 'w') as f:
                yaml.dump(default_config, f)
            logger.warning(f"Created default config at {config_path}")
        
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
            return FullConfig(**config_data)

    def _initialize_encryption(self):
        """Initialize encryption for sensitive data"""
        key = Fernet.generate_key()
        self.cipher = Fernet(key)

    async def initialize_websockets(self):
        """Initialize WebSocket connections for market data"""
        topics = []
        for coin in self.config.coins[:self.config.trading.coins_per_cycle]:
            symbol = f"{coin}USDT"
            topics.extend([
                f"tickers.{symbol}",
                f"kline.1.{symbol}"
            ])
        
        chunk_size = 20
        for i in range(0, len(topics), chunk_size):
            chunk = topics[i:i + chunk_size]
            await self.ws_manager.connect(chunk, self.handle_websocket_message)
            await asyncio.sleep(1)

    async def handle_websocket_message(self, message: Dict):
        """Process incoming WebSocket messages"""
        try:
            topic = message.get('topic', '')
            data = message.get('data', {})
            
            if 'tickers' in topic:
                symbol = topic.split('.')[-1]
                last_price = float(data.get('lastPrice', 0))
                if last_price > 0:
                    self.strategy.last_price[symbol] = last_price
                    if self.trap_strategy:
                        self.trap_strategy.last_price[symbol] = last_price
            
            elif 'kline' in topic:
                symbol = topic.split('.')[-1]
                candle = data[-1] if isinstance(data, list) else data
                df = pd.DataFrame([{
                    'timestamp': pd.to_datetime(candle['start'], unit='ms'),
                    'open': float(candle['open']),
                    'high': float(candle['high']),
                    'low': float(candle['low']),
                    'close': float(candle['close']),
                    'volume': float(candle['volume'])
                }])
                
                cache_key = f"{symbol}:1m:realtime"
                self.cache.set(cache_key, df, ttl=300)
                
                if self.trap_strategy:
                    trap_signals = self.trap_strategy.generate_signals(df.copy(), symbol)
                    if trap_signals is not None and trap_signals.iloc[-1]['signal'] != SignalDirection.NEUTRAL.value:
                        self.process_realtime_signal(symbol, trap_signals.iloc[-1])
                        return
                
                signals = self.generate_trading_signals(df, symbol)
                if signals is not None and signals.iloc[-1]['signal'] != SignalDirection.NEUTRAL.value:
                    self.process_realtime_signal(symbol, signals.iloc[-1])
            
        except Exception as e:
            logger.error(f"WebSocket message handling error: {e}")

    def process_realtime_signal(self, symbol: str, signal_data: pd.Series):
        """Process real-time trading signals"""
        if not self.trading_enabled:
            return
            
        if symbol in self.active_positions:
            return
            
        if self.risk_manager.is_blacklisted(symbol):
            return
            
        balance = self.get_account_balance().get('USDT', 0)
        if balance <= 0:
            return
            
        price = signal_data['close']
        volatility = self.risk_manager.calculate_volatility(pd.DataFrame([{
            'high': price * 1.01,
            'low': price * 0.99,
            'close': price,
            'volume': signal_data.get('volume', 0)
        }]))
        
        signal_strength = signal_data.get('signal_strength', 0)
        risk_pct = min(
            self.config.trading.max_risk_percentage * (1 + signal_strength),
            self.config.trading.max_risk_percentage * 2
        )
        
        stop_loss_pct = self.config.trading.default_stop_loss_pct
        take_profit_pct = self.config.trading.default_take_profit_pct
        
        if volatility > 3:
            stop_loss_pct *= 1.5
            take_profit_pct *= 1.5
        elif volatility < 1:
            stop_loss_pct *= 0.8
            take_profit_pct *= 0.8
            
        quantity = self.risk_manager.calculate_position_size(
            balance, risk_pct, price,
            price * (1 - stop_loss_pct/100) if signal_data['signal'] == SignalDirection.LONG.value 
            else price * (1 + stop_loss_pct/100),
            method=self.config.trading.position_sizing_method
        )
        
        action = 'Buy' if signal_data['signal'] == SignalDirection.LONG.value else 'Sell'
        success = self.execute_trade(
            symbol, action, quantity,
            leverage=self.risk_manager.calculate_dynamic_leverage(volatility),
            price=price,
            stop_loss=price * (1 - stop_loss_pct/100) if action == 'Buy' else price * (1 + stop_loss_pct/100),
            take_profit=price * (1 + take_profit_pct/100) if action == 'Buy' else price * (1 - take_profit_pct/100),
            trailing_stop=self.config.trading.trailing_stop_enabled,
            category="linear"
        )
        
        if success:
            logger.info(f"Executed real-time trade for {symbol} based on WebSocket signal")

    def generate_trading_signals(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Generate trading signals using multiple strategies"""
        try:
            df = self.strategy.generate_signals(df)
            
            if self.config.trading.enable_news_sentiment:
                sentiment = self.news_analyzer.get_news_sentiment(symbol)
                df['signal'] = df['signal'] * (1 + sentiment * self.config.news.sentiment_weight)
            
            return df
        except Exception as e:
            logger.error(f"Signal generation failed for {symbol}: {e}")
            return None

    def execute_trade(self, symbol: str, side: str, quantity: float, 
                     leverage: int = 10, price: Optional[float] = None,
                     stop_loss: Optional[float] = None, 
                     take_profit: Optional[float] = None,
                     trailing_stop: bool = False,
                     category: str = "linear",
                     reduce_only: bool = False) -> bool:
        """Execute a trade through the UTA client"""
        if self.config.trading.dry_run_mode:
            logger.info(f"DRY RUN: Would execute {side} {quantity} of {symbol}")
            return True
            
        return self.client.execute_trade(
            symbol, side, quantity, leverage, price,
            stop_loss, take_profit, trailing_stop,
            category, reduce_only
        )

    def get_account_balance(self) -> Dict[str, float]:
        """Get current account balance"""
        return self.client.get_account_balance()

    def auto_trade_loop(self):
        """Main trading loop for automated trading"""
        while self.config.trading.enable_auto_trading:
            try:
                self.trading_enabled = True
                self.analyze_markets()
                time.sleep(self.config.trading.auto_trade_interval)
            except Exception as e:
                logger.error(f"Auto-trade loop error: {e}")
                time.sleep(60)

    def analyze_markets(self):
        """Analyze all configured markets for trading opportunities"""
        try:
            for coin in self.config.coins[:self.config.trading.coins_per_cycle]:
                symbol = f"{coin}USDT"
                if self.risk_manager.is_blacklisted(symbol):
                    continue
                
                df = asyncio.run(self.get_historical_data(symbol, '1h', 7))
                if df is None:
                    continue
                
                signals = self.generate_trading_signals(df, symbol)
                if signals is None or signals.iloc[-1]['signal'] == SignalDirection.NEUTRAL.value:
                    continue
                
                self.process_realtime_signal(symbol, signals.iloc[-1])
                time.sleep(1)  # Rate limit
                
        except Exception as e:
            logger.error(f"Market analysis failed: {e}")

    def monitor_trap_signals(self):
        """Monitor markets for trap/reversal patterns"""
        while self.config.trading.enable_trap_detection:
            try:
                batch_size = 10
                for i in range(0, len(self.config.coins), batch_size):
                    batch = self.config.coins[i:i + batch_size]
                    symbols = [f"{coin}USDT" for coin in batch]
                    dfs = {}
                    
                    for symbol in symbols:
                        df = asyncio.run(self.get_historical_data(symbol, '1m', 1))
                        if df is not None and not df.empty:
                            dfs[symbol] = df
                    
                    for symbol, df in dfs.items():
                        try:
                            if symbol in self.active_positions:
                                continue
                            
                            signals = self.trap_strategy.generate_signals(df, symbol)
                            last_signal = signals.iloc[-1]
                            
                            if last_signal['signal'] == SignalDirection.NEUTRAL.value:
                                continue
                            
                            balance = self.get_account_balance().get('USDT', 0)
                            if balance <= 0:
                                continue
                            
                            price = float(df.iloc[-1]['close'])
                            risk_pct = min(self.config.trading.max_risk_percentage * 2, 5.0)
                            stop_loss_pct = 1.5
                            take_profit_pct = 3.0
                            
                            quantity = self.risk_manager.calculate_position_size(
                                balance, risk_pct, price, 
                                price * (1 - stop_loss_pct/100) if last_signal['signal'] == SignalDirection.LONG.value 
                                else price * (1 + stop_loss_pct/100)
                            )
                            
                            action = 'Buy' if last_signal['signal'] == SignalDirection.LONG.value else 'Sell'
                            success = self.execute_trade(
                                symbol, action, quantity,
                                leverage=self.risk_manager.calculate_dynamic_leverage(
                                    self.risk_manager.calculate_volatility(df)
                                ),
                                price=price,
                                stop_loss=price * (1 - stop_loss_pct/100) if action == 'Buy' else price * (1 + stop_loss_pct/100),
                                take_profit=price * (1 + take_profit_pct/100) if action == 'Buy' else price * (1 - take_profit_pct/100),
                                trailing_stop=False,
                                category="linear"
                            )
                            if success:
                                time.sleep(1)
                        
                        except Exception as e:
                            logger.error(f"Error processing trap signal for {symbol}: {e}")
                            continue
                
                time.sleep(10)
            except Exception as e:
                logger.error(f"Trap signal monitoring error: {e}")
                time.sleep(60)

    def monitor_market_conditions(self):
        """Monitor open positions and market conditions"""
        while True:
            try:
                self.check_open_positions()
                time.sleep(self.config.trading.position_monitor_interval)
            except Exception as e:
                logger.error(f"Position monitoring error: {e}")
                time.sleep(60)

    def check_open_positions(self):
        """Check and manage open positions under UTA"""
        try:
            positions = self.client.get_positions(category="linear")
            
            for pos in positions:
                symbol = pos['symbol']
                entry_price = float(pos['avgPrice'])
                current_price = float(pos['markPrice'])
                pnl_pct = (current_price - entry_price) / entry_price * 100
                size = float(pos['size'])
                
                if size == 0:
                    if symbol in self.active_positions:
                        del self.active_positions[symbol]
                    continue
                
                # Update active positions
                self.active_positions[symbol] = {
                    'entry_price': entry_price,
                    'current_price': current_price,
                    'size': size,
                    'side': pos['side'].upper(),
                    'leverage': int(pos['leverage'])
                }
                
                if self.config.trading.trailing_stop_enabled:
                    self.update_trailing_stop(symbol, current_price, pnl_pct)
                
                if abs(pnl_pct) >= self.config.trading.default_take_profit_pct:
                    self.close_position(symbol, "Take Profit")
                elif abs(pnl_pct) >= self.config.trading.default_stop_loss_pct:
                    self.close_position(symbol, "Stop Loss")
                    
        except Exception as e:
            logger.error(f"Failed to check positions: {e}")

    def update_trailing_stop(self, symbol: str, current_price: float, pnl_pct: float):
        """Update trailing stop for an open position"""
        if symbol not in self.active_positions:
            return
            
        position = self.active_positions[symbol]
        if not position:
            return
            
        if pnl_pct > 0:
            new_stop = current_price * (1 - self.config.trading.trailing_stop_distance_pct / 100)
            if new_stop > position.get('stop_loss', 0):
                position['stop_loss'] = new_stop
                self.execute_trade(
                    symbol, 
                    'Sell' if position['side'] == 'BUY' else 'Buy',
                    position['size'],
                    stop_loss=new_stop,
                    category="linear",
                    reduce_only=True
                )

    def close_position(self, symbol: str, reason: str):
        """Close an open position under UTA"""
        if symbol not in self.active_positions:
            return
            
        position = self.active_positions[symbol]
        if not position:
            return
            
        self.execute_trade(
            symbol, 
            'Sell' if position['side'] == 'BUY' else 'Buy',
            position['size'],
            category="linear",
            reduce_only=True
        )
        logger.info(f"Closed position for {symbol} ({reason})")

    def backtest_strategy(self, symbol: str, period: str, amount: float) -> Dict[str, Any]:
        """Backtest trading strategy on historical data"""
        try:
            period_map = {
                '1w': ('1h', 7),
                '1m': ('4h', 30),
                '3m': ('1d', 90),
                '6m': ('1d', 180),
                '1y': ('1d', 365)
            }
            
            interval, days = period_map.get(period.lower(), ('1h', 7))
            df = asyncio.run(self.get_historical_data(symbol, interval, days))
            if df is None:
                return {'error': 'Failed to fetch historical data'}
            
            df = self.generate_trading_signals(df, symbol)
            if df is None:
                return {'error': 'Failed to generate signals'}
            
            balance = amount
            position = None
            trades = []
            equity_curve = [{'time': df.iloc[0]['timestamp'], 'balance': amount}]
            
            for i, row in df.iterrows():
                if position is None and row['signal'] != SignalDirection.NEUTRAL.value:
                    position = {
                        'entry_price': row['close'],
                        'entry_time': row['timestamp'],
                        'direction': 'LONG' if row['signal'] == SignalDirection.LONG.value else 'SHORT',
                        'size': balance * 0.99
                    }
                    trades.append({
                        'type': 'entry',
                        'time': row['timestamp'],
                        'price': row['close'],
                        'direction': position['direction'],
                        'size': position['size']
                    })
                elif position is not None:
                    exit_condition = False
                    exit_price = None
                    exit_reason = ""
                    
                    if position['direction'] == 'LONG':
                        if row['low'] <= position['entry_price'] * 0.97:
                            exit_price = position['entry_price'] * 0.97
                            exit_condition = True
                            exit_reason = "Stop Loss"
                        elif row['high'] >= position['entry_price'] * 1.06:
                            exit_price = position['entry_price'] * 1.06
                            exit_condition = True
                            exit_reason = "Take Profit"
                    else:
                        if row['high'] >= position['entry_price'] * 1.03:
                            exit_price = position['entry_price'] * 1.03
                            exit_condition = True
                            exit_reason = "Stop Loss"
                        elif row['low'] <= position['entry_price'] * 0.94:
                            exit_price = position['entry_price'] * 0.94
                            exit_condition = True
                            exit_reason = "Take Profit"
                    
                    if exit_condition:
                        if position['direction'] == 'LONG':
                            pnl = (exit_price - position['entry_price']) / position['entry_price']
                        else:
                            pnl = (position['entry_price'] - exit_price) / position['entry_price']
                        
                        balance += position['size'] * pnl
                        
                        trades.append({
                            'type': 'exit',
                            'time': row['timestamp'],
                            'price': exit_price,
                            'direction': position['direction'],
                            'size': position['size'],
                            'pnl': pnl,
                            'reason': exit_reason
                        })
                        
                        equity_curve.append({
                            'time': row['timestamp'],
                            'balance': balance
                        })
                        position = None
            
            winning_trades = [t for t in trades if t.get('pnl', 0) > 0]
            losing_trades = [t for t in trades if t.get('pnl', 0) < 0]
            max_drawdown = self.calculate_max_drawdown([e['balance'] for e in equity_curve])
            sharpe_ratio = self.calculate_sharpe_ratio([e['balance'] for e in equity_curve])
            
            result = {
                'initial_amount': amount,
                'final_balance': balance,
                'profit': balance - amount,
                'profit_pct': (balance - amount) / amount * 100,
                'total_trades': len(trades) // 2,
                'winning_trades': len(winning_trades),
                'losing_trades': len(losing_trades),
                'win_rate': len(winning_trades) / (len(trades) // 2) * 100 if trades else 0,
                'profit_factor': sum(t['pnl'] for t in winning_trades) / abs(sum(t['pnl'] for t in losing_trades)) if losing_trades else float('inf'),
                'max_drawdown': max_drawdown,
                'sharpe_ratio': sharpe_ratio,
                'trades': trades,
                'equity_curve': equity_curve,
                'price_data': df.to_dict('records')
            }
            
            if self.config.bot.enable_plotly:
                result['visualization'] = self.generate_backtest_visualization(result, symbol, period)
            
            return result
            
        except Exception as e:
            logger.error(f"Backtest failed: {e}")
            return {'error': str(e)}

    def generate_backtest_visualization(self, backtest_result: Dict, symbol: str, period: str) -> str:
        """Generate interactive visualization of backtest results"""
        try:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            price_data = backtest_result['price_data']
            times = [pd.to_datetime(d['timestamp']) for d in price_data]
            prices = [d['close'] for d in price_data]
            
            fig.add_trace(
                go.Scatter(
                    x=times, 
                    y=prices,
                    name=f"{symbol} Price",
                    line=dict(color='blue')
                ),
                secondary_y=False
            )
            
            long_signals = [d['timestamp'] for d in price_data if d.get('signal') == SignalDirection.LONG.value]
            short_signals = [d['timestamp'] for d in price_data if d.get('signal') == SignalDirection.SHORT.value]
            
            if long_signals:
                fig.add_trace(
                    go.Scatter(
                        x=long_signals,
                        y=[prices[times.index(pd.to_datetime(t))] for t in long_signals],
                        mode='markers',
                        name='Buy Signal',
                        marker=dict(color='green', size=10, symbol='triangle-up')
                    ),
                    secondary_y=False
                )
            
            if short_signals:
                fig.add_trace(
                    go.Scatter(
                        x=short_signals,
                        y=[prices[times.index(pd.to_datetime(t))] for t in short_signals],
                        mode='markers',
                        name='Sell Signal',
                        marker=dict(color='red', size=10, symbol='triangle-down')
                    ),
                    secondary_y=False
                )
            
            trades = backtest_result['trades']
            entry_times = [t['time'] for t in trades if t['type'] == 'entry']
            entry_prices = [t['price'] for t in trades if t['type'] == 'entry']
            exit_times = [t['time'] for t in trades if t['type'] == 'exit']
            exit_prices = [t['price'] for t in trades if t['type'] == 'exit']
            
            fig.add_trace(
                go.Scatter(
                    x=entry_times,
                    y=entry_prices,
                    mode='markers',
                    name='Entry',
                    marker=dict(color='lime', size=12, symbol='circle')
                ),
                secondary_y=False
            )
            
            fig.add_trace(
                go.Scatter(
                    x=exit_times,
                    y=exit_prices,
                    mode='markers',
                    name='Exit',
                    marker=dict(color='magenta', size=12, symbol='circle')
                ),
                secondary_y=False
            )
            
            equity_curve = backtest_result['equity_curve']
            equity_times = [pd.to_datetime(e['time']) for e in equity_curve]
            equity_values = [e['balance'] for e in equity_curve]
            
            fig.add_trace(
                go.Scatter(
                    x=equity_times,
                    y=equity_values,
                    name='Equity Curve',
                    line=dict(color='purple', width=2)
                ),
                secondary_y=True
            )
            
            final_balance = backtest_result['final_balance']
            profit_pct = backtest_result['profit_pct']
            win_rate = backtest_result['win_rate']
            
            fig.add_annotation(
                x=equity_times[-1],
                y=final_balance,
                text=f"Final: ${final_balance:,.2f} ({profit_pct:.2f}%)<br>Win Rate: {win_rate:.2f}%",
                showarrow=True,
                arrowhead=1
            )
            
            fig.update_layout(
                title=f"Backtest Results for {symbol} ({period})",
                xaxis_title='Date',
                yaxis_title='Price',
                yaxis2_title='Account Balance',
                hovermode='x unified',
                height=800
            )
            
            return fig.to_html(full_html=False, include_plotlyjs='cdn')
            
        except Exception as e:
            logger.error(f"Failed to generate visualization: {e}")
            return None

    def calculate_max_drawdown(self, equity_curve: List[float]) -> float:
        """Calculate maximum drawdown percentage"""
        peak = equity_curve[0]
        max_dd = 0.0
        
        for value in equity_curve:
            if value > peak:
                peak = value
            dd = (peak - value) / peak
            if dd > max_dd:
                max_dd = dd
                
        return max_dd * 100

    def calculate_sharpe_ratio(self, equity_curve: List[float], risk_free_rate: float = 0.0) -> float:
        """Calculate annualized Sharpe ratio"""
        returns = []
        for i in range(1, len(equity_curve)):
            ret = (equity_curve[i] - equity_curve[i-1]) / equity_curve[i-1]
            returns.append(ret)
            
        if not returns:
            return 0.0
            
        avg_return = np.mean(returns)
        std_dev = np.std(returns)
        
        if std_dev == 0:
            return float('inf')
            
        return (avg_return - risk_free_rate) / std_dev * np.sqrt(365)

    def get_historical_data(self, symbol: str, interval: str, days: int) -> Optional[pd.DataFrame]:
        """Get historical market data"""
        return asyncio.run(self.client.get_historical_data(symbol, interval, days))

# ====================== Telegram Bot ======================
class CryptoTradingTelegramBot:
    """Interactive Telegram interface for the trading bot"""
    def __init__(self, engine: CryptoTradingEngine):
        self.engine = engine
        self.bot = TeleBot(self.engine.config.bot.telegram_token)
        self.command_cooldowns = {}
        self.alerts = {}
        self.register_handlers()
        
        threading.Thread(target=self.monitor_alerts, daemon=True).start()
        threading.Thread(target=self.send_daily_report, daemon=True).start()
        threading.Thread(target=self.check_bot_health, daemon=True).start()

    def register_handlers(self):
        """Register Telegram command handlers"""
        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
            if not self.is_authorized(message.from_user.id):
                self.bot.reply_to(message, "🚫 Unauthorized access")
                return
                
            help_text = """
            🤖 *Crypto Trading Bot Commands* 🤖
            
            /status - Show bot status
            /positions - Show open positions
            /balance - Show account balance
            /backtest SYMBOL PERIOD [AMOUNT] - Run backtest
            /alert SYMBOL PRICE - Set price alert
            /sizing METHOD - Set position sizing method
            /margin MODE - Set margin mode (ISOLATED/CROSS)
            
            *Methods*: fixed, volatility, kelly, martingale
            *Example*: /backtest BTC 1m 1000
            """
            self.bot.reply_to(message, help_text, parse_mode='Markdown')

        @self.bot.message_handler(commands=['trap_stats'])
        def handle_trap_stats(message):
            if not self.is_authorized(message.from_user.id):
                return
                
            if not self.engine.trap_strategy:
                self.bot.reply_to(message, "Trap detection is not enabled")
                return
                
            stats = []
            for symbol in self.engine.config.coins[:20]:
                symbol = f"{symbol}USDT"
                if symbol in self.engine.trap_strategy.normal_volume:
                    stats.append(
                        f"{symbol}: Vol={self.engine.trap_strategy.normal_volume[symbol]:.2f}, "
                        f"OI={self.engine.trap_strategy.normal_oi[symbol]:.2f}, "
                        f"Price={self.engine.trap_strategy.last_price.get(symbol, 0):.2f}"
                    )
            
            self.bot.reply_to(message, "🔍 *Trap Strategy Stats*\n\n" + "\n".join(stats), parse_mode='Markdown')

        @self.bot.message_handler(commands=['backtest'])
        def handle_backtest(message):
            if not self.is_authorized(message.from_user.id):
                return
                
            parts = message.text.split()
            if len(parts) < 3:
                self.bot.reply_to(message, "Usage: /backtest SYMBOL PERIOD [AMOUNT]\nExample: /backtest BTC 1y 1000")
                return
                
            symbol = parts[1].upper() + 'USDT'
            period = parts[2].lower()
            amount = float(parts[3]) if len(parts) > 3 else 10000.0
            
            self.bot.reply_to(message, "⏳ Running backtest...")
            
            try:
                result = self.engine.backtest_strategy(symbol, period, amount)
                if result.get('error'):
                    self.bot.reply_to(message, f"❌ Error: {result['error']}")
                    return
                
                summary_msg = (
                    f"📊 *Backtest Results for {symbol} ({period})*\n\n"
                    f"• Initial Amount: ${result['initial_amount']:,.2f}\n"
                    f"• Final Balance: ${result['final_balance']:,.2f}\n"
                    f"• Profit: ${result['profit']:,.2f} ({result['profit_pct']:.2f}%)\n"
                    f"• Total Trades: {result['total_trades']}\n"
                    f"• Win Rate: {result['win_rate']:.2f}%\n"
                    f"• Profit Factor: {result['profit_factor']:.2f}\n"
                    f"• Max Drawdown: {result['max_drawdown']:.2f}%\n"
                    f"• Sharpe Ratio: {result['sharpe_ratio']:.2f}"
                )
                
                self.bot.send_message(message.chat.id, summary_msg, parse_mode='Markdown')
                
                if result.get('visualization'):
                    self.bot.send_message(
                        message.chat.id,
                        "📈 Interactive chart loading...",
                        parse_mode='Markdown'
                    )
                    self.bot.send_message(
                        message.chat.id,
                        result['visualization'],
                        parse_mode='HTML'
                    )
                else:
                    fig, ax = plt.subplots(figsize=(10, 6))
                    equity_curve = result['equity_curve']
                    times = [pd.to_datetime(e['time']) for e in equity_curve]
                    balances = [e['balance'] for e in equity_curve]
                    
                    ax.plot(times, balances, marker='o', linestyle='-')
                    ax.set_title(f"Backtest Results for {symbol} ({period})")
                    ax.set_ylabel("Balance (USDT)")
                    ax.grid(True)
                    
                    buf = BytesIO()
                    plt.savefig(buf, format='png')
                    buf.seek(0)
                    
                    self.bot.send_photo(
                        message.chat.id,
                        buf,
                        caption="Equity Curve",
                        parse_mode='Markdown'
                    )
                    plt.close()
                    
            except Exception as e:
                self.bot.reply_to(message, f"❌ Backtest failed: {str(e)}")

        @self.bot.message_handler(commands=['sizing'])
        def handle_position_sizing(message):
            if not self.is_authorized(message.from_user.id):
                return
                
            parts = message.text.split()
            if len(parts) < 2:
                self.bot.reply_to(message, "Usage: /sizing METHOD\nMethods: fixed, volatility, kelly, martingale")
                return
                
            method_map = {
                'fixed': PositionSizingMethod.FIXED_RISK,
                'volatility': PositionSizingMethod.VOLATILITY_ADJUSTED,
                'kelly': PositionSizingMethod.KELLY_CRITERION,
                'martingale': PositionSizingMethod.MARTINGALE
            }
            
            method = method_map.get(parts[1].lower())
            if not method:
                self.bot.reply_to(message, "Invalid method. Options: fixed, volatility, kelly, martingale")
                return
                
            self.engine.config.trading.position_sizing_method = method
            self.bot.reply_to(message, f"✅ Position sizing method set to {method.name}")

        @self.bot.message_handler(commands=['margin'])
        def handle_margin_mode(message):
            if not self.is_authorized(message.from_user.id):
                return
                
            parts = message.text.split()
            if len(parts) < 2:
                self.bot.reply_to(message, "Usage: /margin MODE\nModes: isolated, cross")
                return
                
            mode_map = {
                'isolated': MarginMode.ISOLATED,
                'cross': MarginMode.CROSS
            }
            
            mode = mode_map.get(parts[1].lower())
            if not mode:
                self.bot.reply_to(message, "Invalid mode. Options: isolated, cross")
                return
                
            self.engine.config.trading.default_margin_mode = mode
            self.bot.reply_to(message, f"✅ Default margin mode set to {mode.name}")

    def is_authorized(self, user_id: int) -> bool:
        """Check if user is authorized to use the bot"""
        return user_id in self.engine.config.bot.authorized_users

    def monitor_alerts(self):
        """Monitor and trigger price alerts"""
        while True:
            try:
                current_time = time.time()
                alerts_to_remove = []
                
                for alert_id, alert in list(self.alerts.items()):
                    symbol = alert['symbol']
                    target_price = alert['price']
                    current_price = self.engine.strategy.last_price.get(symbol)
                    
                    if current_price is None:
                        continue
                        
                    if (alert['direction'] == 'above' and current_price >= target_price) or \
                       (alert['direction'] == 'below' and current_price <= target_price):
                        self.bot.send_message(
                            alert['chat_id'],
                            f"🚨 Alert: {symbol} is now {alert['direction']} {target_price} (current: {current_price})"
                        )
                        alerts_to_remove.append(alert_id)
                    elif current_time - alert['created_at'] > 86400:  # 24h expiry
                        alerts_to_remove.append(alert_id)
                
                for alert_id in alerts_to_remove:
                    del self.alerts[alert_id]
                
                time.sleep(10)
            except Exception as e:
                logger.error(f"Alert monitoring error: {e}")
                time.sleep(60)

    def send_daily_report(self):
        """Send daily performance report to authorized users"""
        while True:
            try:
                now = datetime.datetime.now()
                if now.hour == 8 and now.minute == 0:  # 8 AM daily
                    balance = self.engine.get_account_balance().get('USDT', 0)
                    positions = len(self.engine.active_positions)
                    
                    report = (
                        f"📊 *Daily Trading Report*\n\n"
                        f"• Account Balance: ${balance:,.2f}\n"
                        f"• Open Positions: {positions}\n"
                        f"• Today's Trades: {self.engine.risk_manager.trade_metrics['total_trades']}\n"
                        f"• Win Rate: {self.engine.risk_manager.get_win_rate():.2f}%\n"
                        f"• Profit Factor: {self.engine.risk_manager.get_profit_factor():.2f}"
                    )
                    
                    for user_id in self.engine.config.bot.authorized_users:
                        self.bot.send_message(user_id, report, parse_mode='Markdown')
                    
                    time.sleep(60)  # Prevent multiple sends
                time.sleep(60)
            except Exception as e:
                logger.error(f"Daily report error: {e}")
                time.sleep(60)

    def check_bot_health(self):
        """Periodically check and report bot health"""
        while True:
            try:
                if not self.engine.trading_enabled:
                    for user_id in self.engine.config.bot.authorized_users:
                        self.bot.send_message(user_id, "⚠️ Warning: Auto-trading is currently disabled")
                time.sleep(3600)  # Check hourly
            except Exception as e:
                logger.error(f"Health check error: {e}")
                time.sleep(60)

    def start(self):
        """Start the Telegram bot"""
        self.bot.polling(none_stop=True)

# ====================== Main Execution ======================
def main():
    """Entry point for the trading bot"""
    # Start Prometheus metrics server
    start_http_server(8000)
    
    try:
        engine = CryptoTradingEngine()
        bot = CryptoTradingTelegramBot(engine)
        bot.start()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    main()