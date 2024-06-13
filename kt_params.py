#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project: Klines Toolkit
File:    global parameters

Created on Mon Oct 16 09:31:11 2023
@author: IGOR POLEV
"""

_MIN_MONITOR_RANK = 1.5

_INTERVAL   = 5
_PARAM_SET  = 'MAIN_SET5'
_DATA_CHUNK = 120 * 24 * 3600 * 1000 # in ms

_USE_BOTS              = True
_SEND_OPEN_GROUPS      = True
_GET_NEW_DATA          = True
_PROCESS_DATA          = True
_CHECK_NEW_DATA        = True
_CHECK_TICK_SIZE       = True
_CHECK_ZERO_TRADES     = True
_RECLASSIFY            = False
_MIN_NEW_KLINES        = 40 // _INTERVAL
_NEIGHBOUR_ZERO_TRADES = 4 * 60 // _INTERVAL
_SYM_REQ_INTERVAL      = _INTERVAL * 60.0 # in seconds
_BOT_CHECK_INTERVAL    = 10.0             # in seconds
_ARCH_CHECK_INTERVAL   = 10.0             # in seconds
_MONITOR_TIMES         = [60, 30, 15, -1] # in minutes, last value must be -1

if _RECLASSIFY:
    _USE_BOTS          = False
    _GET_NEW_DATA      = False
    _CHECK_TICK_SIZE   = False
    _CHECK_ZERO_TRADES = False
if not _USE_BOTS:
    _SEND_OPEN_GROUPS  = False
if not _GET_NEW_DATA:
    _CHECK_NEW_DATA    = False

_CLEAR_PATTERNS        = False # ATTENTION! Use with coution! Requires input - don't run in service mode!

_PIC_WIDTH  = 1920
_PIC_HEIGHT = 1080
_GROUP_SHOW_DELTA_HOURS = 12

_IGNORE_SETTING_WITH_COPY_WARNING = True

_BINANCE_URL     = "https://fapi.binance.com"
_KLINES_URL      = "/fapi/v1/klines"
_SERVER_TIME_URL = "/fapi/v1/time"
_INFO_URL        = "/fapi/v1/exchangeInfo"

_BINANCE_FUTURES_URL = "https://www.binance.com/en/futures/"
_TRADINGVIEW_URL     = "https://ru.tradingview.com/chart/aln9b0qK/?symbol="
_BOT_TOKEN_MONITOR   = '6910755004:AAGfi1OIKZ7IAXSDHB5IR9SKxoqsfPDJ0no'
_BOT_TOKEN_ALERTS    = '6789740433:AAHWsgIRkpw-SoBFx6nxtCePGUo7s4dZT-s'
_BOT_CHAT_ID         = 6349677666

_HOME_DIR            = "/home/igor/Crypto/"
_DB_FILE             = _HOME_DIR + "db/binance_data.db"
_SERVICE_DIR         = _HOME_DIR + "kt/"
_LOG_ZERO_TRADES     = _SERVICE_DIR + "zero_trades.log"
_LOG_TICK_CHANGES    = _SERVICE_DIR + "tick_changes.log"
_LOG_URL_ERRORS      = _SERVICE_DIR + "url_errors.log"
_LOG_TELEGRAM_ERRORS = _SERVICE_DIR + "telegram_errors.log"
_LOG_UNKNOWN_ERRORS  = _SERVICE_DIR + "unknown_errors.log"
_STOP_FILE           = _SERVICE_DIR + "script.stop"

_SYMBOL_FILTER = []