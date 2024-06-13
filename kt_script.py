#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project: Klines Toolkit
File:    Binance data processing script

Created on Mon Oct 16 09:31:11 2023
@author: IGOR POLEV
"""

import pandas  as pd
import asyncio
import os

from math     import floor, ceil, log10
from datetime import datetime
from sys      import argv    as sys_argv
from getopt   import getopt  as sys_getopt
from telegram import Bot     as telegram_bot
from telegram import error   as telegram_error

from kt_utils     import file_lock, file_unlock, log_write
from kt_binancedb import BinanceDB
from kt_kanalizer import KAnalizer
from kt_graph     import KGraph

from kt_params import _IGNORE_SETTING_WITH_COPY_WARNING
from kt_params import _DATA_CHUNK
from kt_params import _SYM_REQ_INTERVAL
from kt_params import _GET_NEW_DATA
from kt_params import _SEND_OPEN_GROUPS
from kt_params import _CHECK_TICK_SIZE
from kt_params import _CHECK_ZERO_TRADES
from kt_params import _NEIGHBOUR_ZERO_TRADES
from kt_params import _CHECK_NEW_DATA
from kt_params import _MIN_NEW_KLINES
from kt_params import _RECLASSIFY
from kt_params import _SYMBOL_FILTER
from kt_params import _LOG_ZERO_TRADES
from kt_params import _LOG_TICK_CHANGES
from kt_params import _LOG_TELEGRAM_ERRORS
from kt_params import _LOG_UNKNOWN_ERRORS
from kt_params import _BOT_TOKEN_MONITOR
from kt_params import _BOT_TOKEN_ALERTS
from kt_params import _BOT_CHAT_ID
from kt_params import _BOT_CHECK_INTERVAL
from kt_params import _ARCH_CHECK_INTERVAL
from kt_params import _MONITOR_TIMES
from kt_params import _PIC_WIDTH
from kt_params import _PIC_HEIGHT
from kt_params import _CLEAR_PATTERNS
from kt_params import _TRADINGVIEW_URL
from kt_params import _BINANCE_FUTURES_URL
from kt_params import _PARAM_SET
from kt_params import _USE_BOTS
from kt_params import _PROCESS_DATA
from kt_params import _STOP_FILE
from kt_params import _SERVICE_DIR

class KTscript:

    def __init__(self, cmdl_params):

        self.exe_parts    = int(os.environ.get('KT_EXE_COUNT'))
        if not self.exe_parts:
            raise Exception("Environment variable KT_EXE_COUNT is undefined.")
        self.thread_num   = 0
        self.symbol_list  = _SYMBOL_FILTER
        self.param_set    = _PARAM_SET
        self.run_mode     = False
        self.verbosity    = 1
        opts, args = sys_getopt(cmdl_params, "p:t:s:v:r")
        try:
            for opt, arg in opts:
                if   opt == '-t': self.thread_num   = int(arg) # id of current executing processes (staring from 0)
                elif opt == '-s': self.symbol_list  = [arg]    # list of symbols to process
                elif opt == '-p': self.param_set    = arg      # redefinition of param_set_id
                elif opt == '-v': self.verbosity    = int(arg) # level of verbosity
                elif opt == '-r': self.run_mode     = True     # start processing immediately
            if self.exe_parts < 1 or self.thread_num >= self.exe_parts or self.thread_num < 0:
                raise Exception()
        except:
            raise Exception("Bad command line arguments.")
        if not self.run_mode and not os.path.exists(_STOP_FILE):
            open(_STOP_FILE, 'x').close()

        if self.verbosity >= 1:
            print("\n--- Binance data processing script ---\n")
            print("Thread {} of expected {}.".format(self.thread_num + 1, self.exe_parts))

        if _IGNORE_SETTING_WITH_COPY_WARNING:
            pd.set_option('mode.chained_assignment', None)
        if _CHECK_TICK_SIZE:
            self.f_check_tick = True

        if self.exe_parts != 1:
            subset = {'rem' : self.thread_num, 'div' : self.exe_parts}
        else:
            subset = None
        self.db = BinanceDB(self.symbol_list, subset, self.param_set)
        self.db.verbosity = self.verbosity

        if _USE_BOTS:
            self.bot_monitor = telegram_bot(_BOT_TOKEN_MONITOR)
            self.bot_alerts  = telegram_bot(_BOT_TOKEN_ALERTS)
            self.BOT_MONITOR_LOCK = _SERVICE_DIR + 'kt_bot_monitor'
            self.BOT_ALERTS_LOCK  = _SERVICE_DIR + 'kt_bot_alerts'

        self.data_chunk  = _DATA_CHUNK
        self.new_data    = None
        self.symbol      = None

    def init_after_db(self):
        self.ka = KAnalizer(self.db.params)
        self.ka.verbosity = self.verbosity
        self.graph = KGraph(
            self.db.classes,
            self.db.statuses,
            pic_size=(_PIC_WIDTH, _PIC_HEIGHT),
            scale=10,
            renderer='png'
        )
        self.graph.verbosity = self.verbosity
        self.DISTANCE    = self.db.params['EXTR_FRAME_SIZE']
        self.TIME_STEP   = self.db.params['TIME_STEP']
        self.MAX_D_FRAME = self.TIME_STEP * max(
            self.db.params['TOO_OLD_LIMIT'],
            self.db.params['TOLERANCE_FRAME_SIZE'],
            self.db.params['VOLUME_FRAME_SIZE']
        )

    async def telegram_message(self, msg_type, msg, rep_id=None, caption=None, retry=3, wait_sec=5):

        if not _USE_BOTS: return

        if msg_type in ['alert', 'price']:
            await file_lock(self.BOT_ALERTS_LOCK, raise_on_failure=False)
        else:
            await file_lock(self.BOT_MONITOR_LOCK, raise_on_failure=False)
        retry_left = retry
        while retry_left:
            try:
                async with self.bot_monitor:
                    if   msg_type == 'text':
                        result = await self.bot_monitor.send_message(
                            _BOT_CHAT_ID, msg, reply_to_message_id=rep_id)
                    elif msg_type == 'alert':
                        result = await self.bot_alerts.send_message(
                            _BOT_CHAT_ID, msg, reply_to_message_id=rep_id)
                    elif msg_type == 'photo':
                        result = await self.bot_monitor.send_photo(
                            _BOT_CHAT_ID, msg, caption=caption, reply_to_message_id=rep_id)
                    elif msg_type == 'price':
                        result = await self.bot_alerts.send_photo(
                            _BOT_CHAT_ID, msg, caption=caption, reply_to_message_id=rep_id)
                    elif msg_type == 'fwd':
                        result = await self.bot_monitor.forward_message(
                            _BOT_CHAT_ID, _BOT_CHAT_ID, msg)
                    else:
                        raise Exception("Telegram bot failure: unknown message type.")
                break
            except telegram_error.TelegramError as error:
                await log_write(_LOG_TELEGRAM_ERRORS, "{} : {}".format(type(error), error))
                retry_left -= 1
                if retry_left:
                    await asyncio.sleep(wait_sec)
        if msg_type == 'alert':
            file_unlock(self.BOT_ALERTS_LOCK)
        else:
            file_unlock(self.BOT_MONITOR_LOCK)
        if retry_left:
            return result
        else:
            return -1

    async def check_bot(self):

        if not _USE_BOTS: return

        if self.verbosity >= 3:
            print("Checking Telegram bot...                     ", end=' ', flush=True)
        replies = []
        texts   = []
        await file_lock(self.BOT_MONITOR_LOCK, raise_on_failure=False)
        try:
            async with self.bot_monitor:
                updates = await self.bot_monitor.get_updates(
                    offset          = int(await self.db.get_variable('telegram_offset')),
                    allowed_updates = ['message']
                )
            if updates:
                await self.db.set_variable('telegram_offset', updates[len(updates)-1].update_id + 1)
                for u in updates:
                    if u.message.reply_to_message:
                        replies.append(u.message.reply_to_message.message_id)
                    elif u.message.text:
                        texts.append(u.message.text.upper())
        except telegram_error.TelegramError as error:
            await log_write(_LOG_TELEGRAM_ERRORS, "{} : {}".format(type(error), error))

        if self.run_mode and 'STOP' in texts:
            self.run_mode = False
            if not os.path.exists(_STOP_FILE):
                open(_STOP_FILE, 'x').close()
                msg = "Stopping data processing..."
            texts.remove('STOP')
        elif not self.run_mode and 'START' in texts:
            self.run_mode = True
            if os.path.exists(_STOP_FILE):
                os.remove(_STOP_FILE)
                msg = "Starting data processing..."
            texts.remove('START')
        else:
            msg = ''
        if 'START' in texts: texts.remove('START')
        if 'STOP'  in texts: texts.remove('STOP')
        file_unlock(self.BOT_MONITOR_LOCK)
        if msg:
            await self.telegram_message('text', msg)

        if not replies and not texts:
            if self.verbosity >= 3:
                print("nothing new.")
            return
        open_g = await self.db.get_open_groups()
        if replies:
            posted_ids = open_g.posted_id.to_list()
            for r in replies:
                if r in posted_ids:
                    if open_g.monitor.iat[posted_ids.index(r)]:
                        msg = "Monitoring stopped."
                    else:
                        msg = "Monitoring started."
                else:
                    msg = "Case outdated."
                await self.telegram_message('text', msg, rep_id=r)
            open_g = open_g.loc[open_g.posted_id.isin(replies)]
            if not open_g.empty:
                await self.db.mark_open_groups(open_g.loc[:,[]], 'mark_monitor')
        if texts:
            texts = list(map(lambda x: x + 'USDT', texts))
            open_g = open_g.loc[open_g.monitor != 0]
            if not open_g.empty:
                if 'ALLUSDT' in texts:
                    resend = open_g.posted_id.to_list()
                else:
                    open_g = open_g.merge(self.db.symbols.symbol_name, on='symbol_id')
                    resend = open_g.loc[open_g.symbol_name.isin(texts), 'posted_id'].to_list()
                if resend:
                    for idx in resend: await self.telegram_message('fwd', idx)

        if self.verbosity >= 3:
            print("{} replies processed.".format(len(replies)))

    async def monitor_prices(self, symbol_id):

        if not _USE_BOTS: return

        open_g  = await self.db.get_open_groups(symbol_id)
        if open_g.empty: return

        end_time = self.db.symbols.at[symbol_id, 'max_data_time']
        self.db.verbosity = 1
        k_data   = await self.db.load_data(
            symbol_id,
            end_time - self.DISTANCE * self.TIME_STEP,
            end_time,
            klines_only = True
        )
        self.db.verbosity = self.verbosity
        high_price = k_data.high.iat[-1]
        low_price  = k_data.low.iat[-1]
        await self.db.mark_open_groups(pd.DataFrame(
            data = [
                [symbol_id, True,  low_price],
                [symbol_id, False, high_price]
            ],
            columns = ['symbol_id', 'minimum', 'last_price'],
        ), 'last_price')

        open_g  = open_g.loc[open_g.monitor != 0]
        if open_g.empty: return

        if self.verbosity >= 3:
            print("Monitoring selected cases...                 ", end=' ', flush=True)
        open_g['symbol_id']  = symbol_id
        open_g['msg']        = ""
        open_g['crossed']    = False
        open_g['new_time']   = open_g.monitor

        up_speed   = (high_price - k_data.high.iat[0]) / self.DISTANCE
        down_speed = (k_data.low.iat[0] - low_price) / self.DISTANCE
        change_m_time = False
        for g in open_g.itertuples():
            if g.Index[1]:
                the_sign  = -1.0
                the_price = low_price
                the_speed = down_speed
            else:
                the_sign  = 1.0
                the_price = high_price
                the_speed = up_speed
            if the_sign * the_price >= the_sign * g.price:
                if g.monitor > 0:
                    open_g.at[g.Index, 'msg']      = "The price was crossed."
                    open_g.at[g.Index, 'crossed']  = True
                    open_g.at[g.Index, 'new_time'] = -1
                    change_m_time = True
            elif the_speed > 0.0:
                time_left = round(the_sign * (g.price - the_price) / the_speed * self.TIME_STEP / 60000)
                m_idx = _MONITOR_TIMES.index(g.monitor)
                if time_left <= g.monitor:
                    open_g.at[g.Index, 'msg']      = "approaching in {} minutes!".format(time_left)
                    open_g.at[g.Index, 'new_time'] = _MONITOR_TIMES[m_idx + 1]
                    change_m_time = True
                elif m_idx:
                    while time_left > _MONITOR_TIMES[m_idx] and m_idx:
                        m_idx -= 1
                    open_g.at[g.Index, 'new_time'] = _MONITOR_TIMES[m_idx]
                    change_m_time = True
        if change_m_time:
            await self.db.mark_open_groups(
                open_g.loc[open_g.monitor != open_g.new_time, ['symbol_id', 'new_time']],
                'next_monitor_time'
            )

        open_g.drop(open_g.loc[open_g.msg == ""].index, inplace=True)
        if open_g.empty:
            if self.verbosity >= 3:
                print("nothing to report")
            return
        s_name = self.db.symbols.at[symbol_id, 'symbol_name']
        self.db.verbosity = 1
        k_data, e_data, g_data, ge_data = await self.db.load_data(
            symbol_id,
            open_g.index.get_level_values('open_time').min() - self.graph.GT_DELTA,
            end_time
        )
        self.db.verbosity = self.verbosity
        self.graph.set_data(s_name, k_data, e_data, g_data, ge_data)
        sent_cnt = open_g.shape[0]
        for g in open_g.itertuples():
            if g.crossed:
                msg = await self.telegram_message('text', g.msg, rep_id=g.posted_id)
            else:
                msg = await self.telegram_message(
                    'price',
                    await self.graph.display_group(g.Index, stop='data'),
                    caption = "{sym} ≈{price}\n{addr}{sym}.P\n{msg}".format(
                        sym   = s_name,
                        price = g.price,
                        addr  = _TRADINGVIEW_URL,
                        msg   = g.msg
                    ),
                    retry = 20
                )
            if int == type(msg): sent_cnt -= 1
        if self.verbosity >= 3:
            print("{}/{} notifications sent / failed".format(sent_cnt, open_g.shape[0] - sent_cnt))

    async def report_archived_groups(self):

        if not _USE_BOTS: return

        check_time = await self.poke_interval('archived_check_time', _ARCH_CHECK_INTERVAL)
        if not check_time: return
        last_archived = await self.db.get_open_groups(archived = check_time[0])
        if last_archived.empty: return

        if self.verbosity >= 3:
            print("Reporting arcived groups...                  ", end=' ', flush=True)
        sent_cnt = last_archived.shape[0]
        for g in last_archived.itertuples():
            msg = await self.telegram_message('text', "The case is archived.", rep_id=g.posted_id)
            if int == type(msg): sent_cnt -= 1
        if self.verbosity >= 3:
            print("{}/{} notifications sent / failed".format(sent_cnt, last_archived.shape[0] - sent_cnt))

    async def send_open_groups(self, symbol_id):

        if not _USE_BOTS: return

        if self.verbosity >= 3:
            print("Checking new active groups...                ", end=' ', flush=True)
        open_g = await self.db.get_open_groups(symbol_id)
        open_g = open_g.loc[open_g.posted_id == 0]
        if open_g.empty:
            if self.verbosity >= 3:
                print("not found.")
        else:
            self.graph.set_data(
                self.symbol.symbol_name,
                self.ka.k_data,
                self.ka.e_data,
                self.ka.g_data,
                self.ka.ge_data
            )
            sent_cnt = open_g.shape[0]
            for g in open_g.itertuples():
                msg = await self.telegram_message(
                    'photo',
                    await self.graph.display_group(g.Index),
                    caption = "{sym} ≈{price}\n{addr}{sym}.P\n{bin}{sym}".format(
                        sym   = self.symbol.symbol_name,
                        price = g.price,
                        addr  = _TRADINGVIEW_URL,
                        bin   = _BINANCE_FUTURES_URL
                    ),
                    retry = 5
                )
                if int == type(msg):
                    sent_cnt -= 1
                else:
                    open_g.at[g.Index, 'posted_id'] = msg.message_id
            open_g['symbol_id'] = self.symbol.Index
            await self.db.mark_open_groups(open_g[['symbol_id', 'posted_id']], 'mark_posted')
            if self.verbosity >= 3:
                print("{}/{} new groups sent / failed.".format(sent_cnt, open_g.shape[0] - sent_cnt))

    async def check_zero_trades(self):
        zero_trades = self.new_data.loc[self.new_data.volume == 0.0].index
        if zero_trades.empty:
            if self.verbosity >= 3:
                print('   Zero trades check finished - no issues.')
            return self.new_data.index[-1]
        if self.verbosity >= 3:
            print('--- Some zero trades detected...')
        nei_zero_cnt   = 1
        prev_zero_time = zero_trades[0]
        for zero_time in zero_trades[1:]:
            if zero_time - prev_zero_time == self.TIME_STEP:
                nei_zero_cnt += 1
            else:
                nei_zero_cnt = 1
            if nei_zero_cnt == _NEIGHBOUR_ZERO_TRADES:
                last_non_zero = zero_time - _NEIGHBOUR_ZERO_TRADES * self.TIME_STEP
                date_time = pd.to_datetime(
                    last_non_zero + self.TIME_STEP,
                    unit='ms'
                ).strftime(self.db.TIME_FORMAT)
                if self.verbosity >= 2:
                    print("--- Critical count of zero trades detected at {}!".format(date_time))
                await log_write(_LOG_ZERO_TRADES, "{id}\t{sym}\t{dt}\t{zt} zero trades detected".format(
                    id  = self.symbol.Index,
                    sym = self.symbol.symbol_name,
                    dt  = date_time,
                    zt  = _NEIGHBOUR_ZERO_TRADES
                ))
                if _USE_BOTS:
                    await self.telegram_message(
                        'alert',
                        "{}: zero trades detected at {}!".format(self.symbol.symbol_name, date_time),
                        retry=10
                    )
                return last_non_zero
            prev_zero_time = zero_time
        if zero_trades[-1] == self.new_data.index[-1]:
            last_non_zero = self.new_data.shape[0] - 2
            while last_non_zero >= 0:
                if self.new_data.volume.iat[last_non_zero] == 0.0:
                    last_non_zero -= 1
                else:
                    break
            return self.new_data.index[0] + last_non_zero * self.TIME_STEP
        return self.new_data.index[-1]

    async def check_tick_zise(self, start_time, end_time, last_end_time, max_k_time):
        oc_data = self.new_data[['open', 'close']]
        self.new_data['h_tale'] = self.new_data.high - oc_data.max(axis=1)
        self.new_data['l_tale'] = oc_data.min(axis=1) - self.new_data.low
        min_tale = min(
            self.new_data.loc[self.new_data.h_tale > 0.0, 'h_tale'].min(),
            self.new_data.loc[self.new_data.l_tale > 0.0, 'l_tale'].min()
        )
        self.new_data.drop(columns=['h_tale', 'l_tale'], inplace=True)

        real_tick = floor(log10(min_tale))
        f_search  = True
        while f_search:
            first_digit = floor(min_tale / 10 ** real_tick)
            if first_digit == 0 or first_digit == 9:
                f_search = False
            else:
                min_tale  -= first_digit * 10 ** real_tick
                real_tick -= 1
                if real_tick <= -20:
                    raise Exception("Tick size evaluation failure.")
        real_tick = 10 ** (real_tick + 1)

        if real_tick != self.ka.tick_size:
            if self.f_check_tick:
                if self.verbosity >= 2:
                    print("--- Detected tick size {} differes from DB tick size {}!".format(real_tick, self.ka.tick_size))
                await log_write(_LOG_TICK_CHANGES, "{id}\t{sym}\t{db_tk}\t{dt_tk} DB / detected tick.".format(
                    id    = self.symbol.Index,
                    sym   = self.symbol.symbol_name,
                    dt_tk = real_tick,
                    db_tk = self.ka.tick_size
                ))
                if _USE_BOTS:
                    await self.telegram_message(
                        'alert',
                        "{}: tick change detected!".format(self.symbol.symbol_name),
                        retry=10
                    )
            if _GET_NEW_DATA: return -1
            if self.f_check_tick:
                self.data_chunk = 86400000 # one day
                if last_end_time == -1:
                    new_end_time = min(self.symbol.max_e_time + self.data_chunk, max_k_time)
                    ISL = pd.IndexSlice
                    self.ka.e_data  = self.ka.e_data.loc[ISL[:new_end_time], :]
                    self.ka.g_data  = self.ka.g_data.loc[ISL[:new_end_time], :]
                    self.ka.ge_data = self.ka.ge_data.loc[self.ka.ge_data.open_time <= new_end_time]
                else:
                    new_end_time = min(last_end_time + self.data_chunk, max_k_time)
                self.new_data = self.new_data.truncate(after = new_end_time)
                self.f_check_tick = False
                if self.verbosity >= 3:
                    print("--- Tick change procedure initiated, loaded data truncated.")
            else:
                if self.verbosity >= 3:
                    print("--- Tick change procedure complete, reloading data...")
                self.data_chunk   = _DATA_CHUNK
                new_end_time      = min(last_end_time + self.data_chunk, max_k_time)
                self.new_data     = await self.db.load_data(
                    self.symbol.Index,
                    start_time,
                    new_end_time,
                    klines_only=True
                )
                self.ka.tick_size = real_tick
                await self.db.save_tick_size(self.symbol.Index, real_tick)
                self.f_check_tick = True
            return new_end_time
        else:
            if self.verbosity >= 3:
                print('   Tick size check finished - no issues.')
            return end_time

    async def go_to_sleep(self, seconds):
        t = ceil(seconds)
        if self.verbosity < 3:
            await asyncio.sleep(t)
            return
        while t > 0:
            print("All done, time to sleep a little...           {} seconds left    ".format(t), end='\r', flush=True)
            await asyncio.sleep(1)
            t -= 1
        print("Slept well, back to work!                     {} seconds passed  ".format(ceil(seconds)))

    async def poke_interval(self, name, interval):
        async with self.db:
            last_time = await self.db.get_variable(name)
            the_time  = datetime.now().timestamp()
            if the_time - last_time >= interval:
                await self.db.set_variable(name, the_time)
                return (last_time, the_time)
            return ()

    async def iterate(self):

        if _USE_BOTS or _GET_NEW_DATA:
            the_time = datetime.now().timestamp()
            may_sleep_get = _SYM_REQ_INTERVAL   - the_time + self.db.symbols.last_req.min()
            may_sleep_bot = _BOT_CHECK_INTERVAL - the_time + await self.db.get_variable('bot_check_time')
            if _USE_BOTS and _GET_NEW_DATA:
                may_sleep = min(may_sleep_bot, may_sleep_get)
            elif _USE_BOTS:
                may_sleep = may_sleep_bot
            else:
                may_sleep = may_sleep_get
            if may_sleep > 0.0:
                await self.go_to_sleep(may_sleep)

        sym_num   = 0
        sym_total = self.db.symbols.shape[0]
        for sym in self.db.symbols.itertuples():

            if _USE_BOTS and await self.poke_interval('bot_check_time', _BOT_CHECK_INTERVAL):
                await self.check_bot()
            if self.run_mode and os.path.exists(_STOP_FILE):
                self.run_mode = False
            if not self.run_mode and _USE_BOTS:
                await self.telegram_message(
                    'text',
                    "Thread {} of {} stopped.".format(self.thread_num + 1, self.exe_parts)
                )
            while not self.run_mode:
                if not os.path.exists(_STOP_FILE):
                    self.run_mode = True
                else:
                    await self.go_to_sleep(_BOT_CHECK_INTERVAL * 2)
                    if _USE_BOTS and await self.poke_interval('bot_check_time', _BOT_CHECK_INTERVAL):
                        await self.check_bot()
                if self.run_mode and _USE_BOTS:
                    await self.telegram_message(
                        'text',
                        "Thread {} of {} started.".format(self.thread_num + 1, self.exe_parts)
                    )

            sym_num += 1
            if sym_num == 1 and self.verbosity >= 3:
                start_datetime = datetime.now()
                print("\n\n====================================================\nData processing started at {}\n\n".format(
                    start_datetime.strftime("%d.%m.%y %H:%M:%S")))
            if self.verbosity >= 3:
                print("\n--------------------------\n")
            if self.verbosity >= 1:
                print("Processing {} ({}/{})".format(sym.symbol_name, sym_num, sym_total))
            if self.verbosity >= 3:
                print("\n--------------------------\n")

            self.symbol       = sym
            self.ka.tick_size = sym.tick_size
            self.ka.clear_k_data()

            if _GET_NEW_DATA:
                may_sleep = _SYM_REQ_INTERVAL - datetime.now().timestamp() + sym.last_req
                if may_sleep > 0.0:
                    await self.go_to_sleep(may_sleep)
                if await self.db.get_binance_data(sym.Index):
                    self.db.symbols.at[sym.Index, 'last_req'] = datetime.now().timestamp()
                    await self.monitor_prices(sym.Index)
            if not _PROCESS_DATA: continue

            min_k_time  = self.db.symbols.at[sym.Index, 'min_data_time']
            max_k_time  = self.db.symbols.at[sym.Index, 'max_data_time']
            max_e_time  = self.db.symbols.at[sym.Index, 'max_e_time']
            open_g_time = self.db.symbols.at[sym.Index, 'open_g_time']
            if _CHECK_NEW_DATA and (max_k_time - max_e_time) // self.TIME_STEP < _MIN_NEW_KLINES and not _RECLASSIFY:
                if self.verbosity >= 3:
                    print("\n--- Insufficient new klines data - calculations unnecessary\n")
                continue

            if _RECLASSIFY:
                start_time = min_k_time
                end_time   = max_k_time
            else:
                start_time = max(min(max_e_time - self.MAX_D_FRAME, open_g_time), min_k_time)
                end_time   = min(max_e_time + self.data_chunk, max_k_time)
            self.new_data, self.ka.known_e, self.ka.known_g, self.ka.known_ge = \
                await self.db.load_data(sym.Index, start_time, end_time)
            if self.new_data.empty:
                if self.verbosity >= 2:
                    print("\n--- No klines data loaded - abnormal case?\n")
                msg = "{}: no klines data loaded - abnormal case?".format(sym.symbol_name)
                await log_write(_LOG_UNKNOWN_ERRORS, msg)
                if _USE_BOTS:
                    await self.telegram_message('alert', msg, retry=10)
                continue

            last_end_time = -1
            while end_time != last_end_time:

                if (_CHECK_ZERO_TRADES or _CHECK_TICK_SIZE) and self.verbosity >= 3:
                    print("Checking klines data integrity...")
                if _CHECK_ZERO_TRADES:
                    last_non_zero = await self.check_zero_trades()
                    if last_non_zero != self.new_data.index[-1]:
                        await self.db.truncate_klines(sym.Index, last_non_zero)
                        max_k_time = self.db.symbols.at[sym.Index, 'max_data_time']
                        if last_non_zero >= self.new_data.index[0]:
                            self.new_data = self.new_data.truncate(after = last_non_zero)
                            end_time = last_non_zero
                        else:
                            if self.verbosity >= 3:
                                print("--- All new data is zero, skipping the coin.\n")
                            break

                if _CHECK_TICK_SIZE:
                    end_time = await self.check_tick_zise(start_time, end_time, last_end_time, max_k_time)
                    if end_time == -1:
                        if self.verbosity >= 3:
                            print("--- Skipping the coin!\n")
                        break

                if self.verbosity >= 3:
                    print("Processing data...")
                self.ka.add_k_data(self.new_data)
                if self.verbosity >= 3:
                    print("   Klines / extremums / groups in memory...   {}/{}/{}".format(
                    self.ka.k_data.shape[0], self.ka.known_e.shape[0], self.ka.known_g.shape[0]))
                self.ka.process_data(self.db.statuses, self.db.classes)
                await self.db.save_data(sym.Index, self.ka)
                self.ka.set_changed(False)
                self.ka.trim_data(self.ka.k_data.index[-1] - self.MAX_D_FRAME)
                self.ka.reset_known()

                start_time = self.ka.k_data.index[-1] + self.TIME_STEP
                last_end_time = end_time
                end_time = min(end_time + self.data_chunk, max_k_time)
                if end_time != last_end_time:
                    self.new_data = await self.db.load_data(sym.Index, start_time, end_time, klines_only=True)

                if _USE_BOTS:
                    await self.report_archived_groups()

            if _SEND_OPEN_GROUPS:
                await self.send_open_groups(sym.Index)

        end_datetime = datetime.now()
        if self.verbosity >= 3:
            print('\nData processing finished at {}, elapsed time is {}.\n'.format(
                end_datetime.strftime("%d.%m.%y %H:%M:%S"),
                end_datetime - start_datetime
            ))

    async def run(self):
        try:
            await self.db.setup()
            self.init_after_db()
            if _CLEAR_PATTERNS:
                await self.db.clear_patterns()
            while True:
                await self.iterate()
                if _RECLASSIFY:
                    await self.db.reset_open_groups_rank()
                    break
                if not _GET_NEW_DATA:
                    break
        except Exception as ex:
            msg = "Thread {} raised {} : {}".format(self.thread_num, type(ex), ex)
            print("\n\n!!! Unhandled exception:\n\n{}".format(msg))
            await log_write(_LOG_UNKNOWN_ERRORS, msg)
            if _USE_BOTS:
                if int != type(await self.telegram_message('alert', msg, retry=100)):
                    print("\n!!! Telegram notification send.\n\n")
                else:
                    print("\n!!! Telegram notification failed.\n\n")
            raise ex

if __name__ == "__main__":
    script = KTscript(sys_argv[1:])
    asyncio.run(script.run())
