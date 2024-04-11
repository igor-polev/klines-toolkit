#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project: Klines Toolkit
File:    global parameters

Created on Mon Oct 16 09:31:11 2023
@author: IGOR POLEV
"""

_INTERVAL   = 5
_PARAM_SET  = 'NEXT_SET'
_DATA_CHUNK = 120 * 24 * 3600 * 1000 # in ms

_SEND_OPEN_GROUPS      = False
_GET_NEW_DATA          = True
_CHECK_NEW_DATA        = True
_CHECK_TICK_SIZE       = True
_CHECK_ZERO_TRADES     = True
_OFF_LINE              = False
_RECLASSIFY            = False
_MIN_NEW_KLINES        = 40 // _INTERVAL
_NEIGHBOUR_ZERO_TRADES = 4 * 60 // _INTERVAL
_SYM_REQ_INTERVAL      = _INTERVAL * 60.0 # in seconds
_BOT_CHECK_INTERVAL    = 10.0             # in seconds
_ARCH_CHECK_INTERVAL   = 10.0             # in seconds
_MONITOR_TIMES         = [30, 15, -1]  # in minutes, last value must be -1

_CLEAR_PATTERNS        = False           # ATTENTION! Use with coution!

_PIC_WIDTH  = 1920
_PIC_HEIGHT = 1080
_GROUP_SHOW_DELTA_HOURS = 12
_SELECT_GROUPS_REQ = """
    SELECT
        open_time,
        minimum,
        e_count,
        price,
        posted_id,
        monitor
    FROM
        open_groups
    WHERE
        archived     = 0.0        AND
        symbol_id    = {sym_id}   AND
        interval     = {interval} AND
        param_set_id = {p_id}     AND
        (
            class_id = 2 AND
            rank    >= 1.5
            OR monitor != 0
        )
    ORDER BY
        rank DESC
"""
_SELECT_ALL_GROUPS_REQ = """
    SELECT
        symbol_id,
        open_time,
        minimum,
        e_count,
        price,
        posted_id,
        monitor
    FROM
        open_groups
    WHERE
        archived     = 0.0        AND
        interval     = {interval} AND
        param_set_id = {p_id}     AND
        (
            class_id = 2 AND
            rank    >= 1.5
            OR monitor != 0
        )
    ORDER BY
        rank DESC
"""

_IGNORE_SETTING_WITH_COPY_WARNING = True

_BINANCE_URL     = "https://fapi.binance.com"
_KLINES_URL      = "/fapi/v1/klines"
_SERVER_TIME_URL = "/fapi/v1/time"
_DB_FILE         = "/home/igor/Crypto/Binance/binance_data.db"

_BINANCE_FUTURES_URL = "https://www.binance.com/en/futures/"
_TRADINGVIEW_URL     = "https://ru.tradingview.com/chart/aln9b0qK/?symbol="
_BOT_TOKEN_MONITOR   = '6910755004:AAGfi1OIKZ7IAXSDHB5IR9SKxoqsfPDJ0no'
_BOT_TOKEN_ALERTS    = '6789740433:AAHWsgIRkpw-SoBFx6nxtCePGUo7s4dZT-s'
_BOT_CHAT_ID         = 6349677666

_LOG_ZERO_TRADES       = "ktlog_zero_trades.txt"
_LOG_TICK_CHANGES      = "ktlog_tick_changes.txt"
_LOG_URL_ERRORS        = "ktlog_url_errors.txt"
_LOG_TELEGRAM_ERRORS   = "ktlog_telegram_errors.txt"
_LOG_UNKNOWN_ERRORS    = "ktlog_unknown_errors.txt"

_SYMBOL_FILTER = []