#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project: Klines Toolkit
File:    BinanceDB class

Created on Mon Oct 16 09:40:57 2023
@author: IGOR POLEV
"""

from sys      import maxsize as _MAXINT
from numpy    import nan     as _NaN
from datetime import datetime

import sqlite3        as sql
import pandas         as pd
import urllib.request as url_req
import urllib.error   as url_err
import json
import asyncio

from kt_utils  import file_lock, file_unlock, log_write

from kt_params import _DB_FILE
from kt_params import _PARAM_SET
from kt_params import _INTERVAL
from kt_params import _SERVER_TIME_URL
from kt_params import _BINANCE_URL
from kt_params import _KLINES_URL
from kt_params import _LOG_URL_ERRORS
from kt_params import _MIN_MONITOR_RANK
from kt_params import _MONITOR_TIMES
from kt_params import _RECLASSIFY

class BinanceDB:

    def __init__(self, symbol_list=[], subset=None, param_set=_PARAM_SET, db_name=_DB_FILE, interval=_INTERVAL):

        self.__db_name   = db_name
        self.__db        = None
        self.__cursor    = None
        self.__f_open    = False
        self.__DB_RETRY  = 40
        self.__URL_RETRY = 3
        self.TIME_FORMAT = "%d.%m.%y %H:%M"

        self.symbol_list  = symbol_list
        self.interval     = interval
        self.param_set    = param_set
        self.subset       = subset
        self.verbosity    = 1

    def __init_reqs_dict(self, sym_filter_and, sym_filter_where):
        self.__reqs = {
            'klines_read' : """
                SELECT
                    open_time,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM klines
                WHERE
                    interval   = {interval}   AND
                    symbol_id  = {sym_id}     AND
                    open_time >= {start_time} AND
                    open_time <= {end_time}
                ORDER BY open_time
            """.format(
                interval   = self.interval,
                sym_id     = '{sym_id}',
                start_time = '{start_time}',
                end_time   = '{end_time}'
            ),
            'extremums_read' : """
                SELECT
                    open_time,
                    minimum,
                    price,
                    range_time AS range,
                    r_weight,
                    v_factor,
                    r_factor,
                    tolerance,
                    high_tol,
                    low_tol,
                    next_high_time AS next_high,
                    next_high_closed,
                    next_low_time AS next_low,
                    next_low_closed,
                    last_ex_time AS last_ex,
                    last_non_ex_time AS last_non_ex,
                    height,
                    derivative
                FROM extremums
                WHERE
                    interval     = {interval}   AND
                    param_set_id = {p_id}       AND
                    symbol_id    = {sym_id}     AND
                    open_time   >= {start_time} AND
                    open_time   <= {end_time}
                ORDER BY
                    open_time,
                    minimum
            """.format(
                interval   = self.interval,
                p_id       = self.param_set_id,
                sym_id     = '{sym_id}',
                start_time = '{start_time}',
                end_time   = '{end_time}'
            ),
            'groups_read' : """
                SELECT
                    open_time,
                    minimum,
                    e_count,
                    "primary" AS 'primary',
                    range_time AS 'range',
                    r_factor,
                    g_tol,
                    low_tol,
                    high_tol,
                    rebound_time AS rebound,
                    pierce_factor,
                    class_id,
                    status_id,
                    rank,
                    result,
                    result_time AS result_id
                FROM groups
                WHERE
                    interval     = {interval}   AND
                    param_set_id = {p_id}       AND
                    symbol_id    = {sym_id}     AND
                    open_time   >= {start_time} AND
                    open_time   <= {end_time}
                ORDER BY
                    open_time,
                    minimum,
                    e_count
            """.format(
                interval   = self.interval,
                p_id       = self.param_set_id,
                sym_id     = '{sym_id}',
                start_time = '{start_time}',
                end_time   = '{end_time}'
            ),
            'relations_read' : """
                SELECT
                    open_time,
                    minimum,
                    e_count,
                    e_time
                FROM gr_ex_rels
                WHERE
                    interval     = {interval}   AND
                    param_set_id = {p_id}       AND
                    symbol_id    = {sym_id}     AND
                    open_time   >= {start_time} AND
                    open_time   <= {end_time}
                ORDER BY
                    open_time,
                    minimum,
                    e_count,
                    e_time
            """.format(
                interval   = self.interval,
                p_id       = self.param_set_id,
                sym_id     = '{sym_id}',
                start_time = '{start_time}',
                end_time   = '{end_time}'
            ),
            'open_groups_read' : """
                SELECT
                    symbol_id,
                    open_time,
                    minimum,
                    e_count,
                    monitor
                FROM open_groups
                WHERE
                    archived     = 0.0        AND
                    interval     = {interval} AND
                    param_set_id = {p_id}     AND
                    symbol_id    = {sym_id}
            """.format(
                interval   = self.interval,
                p_id       = self.param_set_id,
                sym_id     = '{sym_id}'
            ),
            'open_groups_delete' : """
                DELETE FROM open_groups
                WHERE
                    archived     = 0.0        AND
                    interval     = {interval} AND
                    param_set_id = {p_id}     AND
                    symbol_id    = :symbol_id AND
                    open_time    = :open_time AND
                    minimum      = :minimum   AND
                    e_count      = :e_count
            """.format(
                interval   = self.interval,
                p_id       = self.param_set_id
            ),
            'open_groups_update' : """
                UPDATE open_groups
                SET
                    class_id    = :class_id,
                    g_tol       = :g_tol,
                    delta_tol   = :delta_tol,
                    rank        = :rank,
                    price       = :price,
                    v_factor    = :v_factor,
                    r_factor    = :r_factor,
                    last_ex     = :last_ex,
                    last_non_ex = :last_non_ex,
                    height      = :height,
                    derivative  = :derivative
                WHERE
                    interval     = {interval} AND
                    param_set_id = {p_id}     AND
                    symbol_id    = :symbol_id AND
                    open_time    = :open_time AND
                    minimum      = :minimum   AND
                    e_count      = :e_count
            """.format(
                interval   = self.interval,
                p_id       = self.param_set_id
            ),
            'extremums_update' : """
                UPDATE extremums
                SET
                    range_time       = :range,
                    r_weight         = :r_weight,
                    next_high_time   = :next_high,
                    next_high_closed = :next_high_closed,
                    next_low_time    = :next_low,
                    next_low_closed  = :next_low_closed
                WHERE
                    interval     = {interval} AND
                    param_set_id = {p_id}     AND
                    symbol_id    = :symbol_id AND
                    open_time    = :open_time AND
                    minimum      = :minimum
            """.format(
                interval = self.interval,
                p_id     = self.param_set_id
            ),
            'groups_update' : """
                UPDATE groups
                SET
                    "primary"     = :primary,
                    range_time    = :range,
                    g_tol         = :g_tol,
                    low_tol       = :low_tol,
                    high_tol      = :high_tol,
                    r_factor      = :r_factor,
                    rebound_time  = :rebound,
                    pierce_factor = :pierce_factor,
                    class_id      = :class_id,
                    status_id     = :status_id,
                    rank          = :rank,
                    result_time   = :result_id,
                    result        = :result
                WHERE
                    interval     = {interval} AND
                    param_set_id = {p_id}     AND
                    symbol_id    = :symbol_id AND
                    open_time    = :open_time AND
                    minimum      = :minimum   AND
                    e_count      = :e_count
            """.format(
                interval = self.interval,
                p_id     = self.param_set_id
            ),
            'clear_patterns' : """
                DELETE FROM {tbl}
                WHERE
                    interval     = {interval} AND
                    param_set_id = {p_id}
                    {sym_cond}
            """.format(
                tbl      = '{tbl}',
                interval = self.interval,
                p_id     = self.param_set_id,
                sym_cond = sym_filter_and
            ),
            'clear_limits' : """
                UPDATE symbol_limits
                SET
                    open_g_time = NULL,
                    max_e_time  = NULL
                WHERE
                    interval     = {interval} AND
                    param_set_id = {p_id}
                    {sym_cond}
            """.format(
                interval = self.interval,
                p_id     = self.param_set_id,
                sym_cond = sym_filter_and
            ),
            'init_limit' : """
                INSERT INTO symbol_limits
                    (symbol_id, interval, param_set_id, min_data_time, max_data_time)
                VALUES
                    (?, {interval}, {p_id}, 0, 0)
            """.format(
                interval = self.interval,
                p_id     = self.param_set_id
            ),
            'reset_limits_k' : """
                UPDATE symbol_limits
                SET
                    min_data_time = totals.min_time,
                    max_data_time = totals.max_time
                FROM (
                    SELECT
                        ? AS symbol_id,
                        min(open_time) AS min_time,
                        max(open_time) AS max_time
                    FROM klines
                    WHERE
                        symbol_id = ? AND
                        interval = {interval}
                    HAVING
                        min_time IS NOT NULL AND
                        max_time IS NOT NULL
                ) AS totals
                WHERE
                    interval     = {interval} AND
                    param_set_id = {p_id}     AND
                    symbol_limits.symbol_id = ?
            """.format(
                interval = self.interval,
                p_id     = self.param_set_id
            ),
            'reset_limits_g' : """
                UPDATE symbol_limits
                SET open_g_time = totals.open_g_time
                FROM (
                    SELECT
                        ? AS symbol_id,
                        min(open_time) AS open_g_time
                    FROM groups
                    WHERE
                        param_set_id = {p_id}     AND
                        interval     = {interval} AND
                        status_id   != {status_1} AND
                        status_id   != {status_2} AND
                        symbol_id    = ?
                ) AS totals
                WHERE
                    param_set_id = {p_id}     AND
                    interval     = {interval} AND
                    symbol_limits.symbol_id = ?
            """.format(
                interval = self.interval,
                p_id     = self.param_set_id,
                status_1 = self.statuses['CLOSED'],
                status_2 = self.statuses['OUTDATED']
            ),
            'reset_limits_e' : """
                UPDATE symbol_limits
                SET max_e_time = totals.max_e_time
                FROM (
                    SELECT
                        ? AS symbol_id,
                        max(open_time) AS max_e_time
                    FROM extremums
                    WHERE
                        param_set_id = {p_id}     AND
                        interval     = {interval} AND
                        symbol_id    = ?
                ) AS totals
                WHERE
                    param_set_id = {p_id}     AND
                    interval     = {interval} AND
                    symbol_limits.symbol_id = ?
            """.format(
                interval = self.interval,
                p_id     = self.param_set_id
            ),
            'update_limit' : """
                UPDATE symbol_limits
                SET {field} = {new_value}
                WHERE
                    param_set_id = {p_id}     AND
                    interval     = {interval} AND
                    symbol_id    = {sym_id}
            """.format(
                interval  = self.interval,
                p_id      = self.param_set_id,
                sym_id    = '{sym_id}',
                field     = '{field}',
                new_value = '{new_value}'
            ),
            'update_limit_all_ps' : """
                UPDATE symbol_limits
                SET {field} = {new_value}
                WHERE
                    interval     = {interval} AND
                    symbol_id    = {sym_id}
            """.format(
                interval  = self.interval,
                sym_id    = '{sym_id}',
                field     = '{field}',
                new_value = '{new_value}'
            ),
            'mark_posted' : """
                UPDATE open_groups
                SET posted_id    = :posted_id
                WHERE
                    param_set_id = {p_id}     AND
                    interval     = {interval} AND
                    symbol_id    = :symbol_id AND
                    open_time    = :open_time AND
                    minimum      = :minimum   AND
                    e_count      = :e_count
            """.format(
                interval  = self.interval,
                p_id      = self.param_set_id
            ),
            'mark_monitor' : """
                UPDATE open_groups
                SET monitor = NOT monitor
                WHERE
                    param_set_id = {p_id}     AND
                    interval     = {interval} AND
                    symbol_id    = :symbol_id AND
                    open_time    = :open_time AND
                    minimum      = :minimum   AND
                    e_count      = :e_count
            """.format(
                interval  = self.interval,
                p_id      = self.param_set_id
            ),
            'mark_archived' : """
                UPDATE open_groups
                SET archived     = :the_time
                WHERE
                    param_set_id = {p_id}     AND
                    interval     = {interval} AND
                    symbol_id    = :symbol_id AND
                    open_time    = :open_time AND
                    minimum      = :minimum   AND
                    e_count      = :e_count
            """.format(
                interval  = self.interval,
                p_id      = self.param_set_id
            ),
            'reset_monitor_time' : """
                UPDATE open_groups
                SET monitor = {time}
                WHERE
                    param_set_id = {p_id}     AND
                    interval     = {interval} AND
                    monitor      = 1
            """.format(
                interval  = self.interval,
                p_id      = self.param_set_id,
                time      = _MONITOR_TIMES[0]
            ),
            'next_monitor_time' : """
                UPDATE open_groups
                SET monitor = :new_time
                WHERE
                    param_set_id = {p_id}     AND
                    interval     = {interval} AND
                    symbol_id    = :symbol_id AND
                    open_time    = :open_time AND
                    minimum      = :minimum   AND
                    e_count      = :e_count
            """.format(
                interval  = self.interval,
                p_id      = self.param_set_id
            ),
            'last_price' : """
                UPDATE open_groups
                SET last_price = :last_price
                WHERE
                    param_set_id = {p_id}     AND
                    interval     = {interval} AND
                    symbol_id    = :symbol_id AND
                    minimum      = :minimum   AND
                    archived     = 0.0        AND
                    posted_id   != 0
            """.format(
                interval  = self.interval,
                p_id      = self.param_set_id
            ),
            'open_groups' : """
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
                    interval     = {interval} AND
                    param_set_id = {p_id}     AND
                    symbol_id    = {sym_id}   AND
                    archived     = 0.0        AND
                    (
                        class_id IN {class_ids} AND
                        rank     >= {min_rank}
                        OR monitor != 0
                    )
            """.format(
                min_rank  = _MIN_MONITOR_RANK,
                class_ids = "('{}', '{}')".format(
                    self.classes['PIERCE_READY'],
                    self.classes['FIRST_TOUCH']
                ),
                interval  = self.interval,
                p_id      = self.param_set_id,
                sym_id    = '{sym_id}'
            ),
            'all_open_groups' : """
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
                    interval     = {interval} AND
                    param_set_id = {p_id}     AND
                    archived     = 0.0        AND
                    (
                        class_id IN {class_ids} AND
                        rank     >= {min_rank}
                        OR monitor != 0
                    )
            """.format(
                min_rank = _MIN_MONITOR_RANK,
                class_ids = "('{}', '{}')".format(
                    self.classes['PIERCE_READY'],
                    self.classes['FIRST_TOUCH']
                ),
                interval = self.interval,
                p_id     = self.param_set_id
                
            ),
            'archived_groups' : """
                SELECT
                    symbol_id,
                    open_time,
                    minimum,
                    e_count,
                    posted_id
                FROM
                    open_groups
                WHERE
                    interval     = {interval} AND
                    param_set_id = {p_id}     AND
                    archived    >= {time}
            """.format(
                interval  = self.interval,
                p_id      = self.param_set_id,
                time      = '{time}'
            ),
            'reset_og_rank' : """
                UPDATE open_groups
                SET rank = (
                    SELECT rank FROM "groups" WHERE
                        groups.param_set_id = open_groups.param_set_id AND
                        groups.interval     = open_groups.interval     AND
                        groups.symbol_id    = open_groups.symbol_id    AND
                        groups.open_time    = open_groups.open_time    AND
                        groups.minimum      = open_groups.minimum      AND
                        groups.e_count      = open_groups.e_count
                )
                {sym_cond}
            """.format(sym_cond = sym_filter_where),
            'read_symbol_limits' : """
                SELECT
                    symbol_id,
                    min_data_time,
                    max_data_time,
                    open_g_time,
                    max_e_time
                FROM symbol_limits
                WHERE
                    interval     = {interval} AND
                    param_set_id = {p_id}
                    {sym_cond}
            """.format(
                interval = self.interval,
                p_id     = self.param_set_id,
                sym_cond = sym_filter_and
            ),
            'symbol_limits_need_init' : """
                SELECT symbols.symbol_id
                FROM symbols LEFT JOIN symbol_limits ON
                    symbols.symbol_id = symbol_limits.symbol_id
                    AND interval     = {interval}
                    AND param_set_id = {p_id}
                WHERE
                    symbol_limits.symbol_id IS NULL
            """.format(
                interval = self.interval,
                p_id     = self.param_set_id
            ),
            'truncate_klines' : """
                DELETE FROM klines
                WHERE
                    interval  = {interval} AND
                    symbol_id = {sym_id}   AND
                    open_time > {last_time}
            """.format(
                interval  = self.interval,
                sym_id    = '{sym_id}',
                last_time = '{last_time}'
            )
        }

    async def setup(self):

        if self.verbosity >= 3:
            print("Requesting database setup ...", end=' ', flush=True)
        async with self:

            self.symbol_names = dict(self.__cursor.execute(
                "SELECT symbol_name, symbol_id FROM symbols WHERE status = 'ACTIVE'"
            ).fetchall())
            self.statuses = dict(self.__cursor.execute(
                "SELECT status_code, status_id FROM statuses"
            ).fetchall())
            self.classes = dict(self.__cursor.execute(
                "SELECT class_code, class_id FROM classes"
            ).fetchall())

            self.param_set_id = int(self.__cursor.execute(
                "SELECT id FROM param_sets WHERE set_name = ?", (self.param_set,)
            ).fetchone()[0])
            self.params = dict(self.__cursor.execute(
                "SELECT param_name, value FROM param_set_values WHERE set_name = ?", (self.param_set,)
            ).fetchall())

            self.params['EXTR_FRAME_SIZE']      = int(self.params['EXTR_FRAME_SIZE'])
            self.params['VOLUME_FRAME_SIZE']    = int(self.params['VOLUME_FRAME_SIZE'])
            self.params['TOLERANCE_FRAME_SIZE'] = int(self.params['TOLERANCE_FRAME_SIZE'])
            self.params['RANGE_OC_TOL_WEIGHT']  = int(self.params['RANGE_OC_TOL_WEIGHT'])
            self.params['RANGE_OC_WEIGHT']      = int(self.params['RANGE_OC_WEIGHT'])
            self.params['RANGE_E_TOL_WEIGHT']   = int(self.params['RANGE_E_TOL_WEIGHT'])
            self.params['RANGE_E_WEIGHT']       = int(self.params['RANGE_E_WEIGHT'])
            self.params['TOO_OLD_LIMIT']        = int(self.params['TOO_OLD_LIMIT'])
            self.params['TIME_STEP']            = self.interval * 60000

            if self.subset:
                if not self.symbol_list:
                    self.symbol_list = list(self.symbol_names.keys())
                idx = 0
                for sym in self.symbol_list.copy():
                    if idx % self.subset['div'] != self.subset['rem']:
                        self.symbol_list.remove(sym)
                    idx += 1
            if self.symbol_list:
                if len(self.symbol_list) == 1:
                    sym_filter_and = "AND symbol_id = " + str(self.symbol_names[self.symbol_list[0]])
                else:
                    sym_filter_and = "AND symbol_id IN " + str(tuple(
                        map(lambda name: self.symbol_names[name], self.symbol_list)
                    )).replace(',)', ')')
                sym_filter_where = sym_filter_and.replace('AND', 'WHERE')
            else:
                sym_filter_and   = ""
                sym_filter_where = ""

            self.__init_reqs_dict(sym_filter_and, sym_filter_where)

            f_get_data     = True
            f_reset_limits = True
            while f_get_data:
                sym_limits = pd.read_sql(
                    self.__reqs['read_symbol_limits'],
                    self.__db, index_col='symbol_id'
                )
                if sym_limits.empty:
                    if f_reset_limits:
                        if self.verbosity >= 3: print('\n')
                        await self.reset_symbol_limits()
                        f_reset_limits = False
                    else:
                        raise Exception("Failure reading table 'symbol_limits'!")
                else:
                    f_get_data = False
            self.symbols = pd.read_sql(
                "SELECT * FROM symbols WHERE status = 'ACTIVE' " + sym_filter_and,
                self.__db, index_col='symbol_id'
            ).join(sym_limits)

        self.symbols.open_g_time.mask(
            self.symbols.open_g_time.isna() & self.symbols.max_e_time.isna(),
            self.symbols.min_data_time,
            inplace=True
        )
        self.symbols.open_g_time.mask(
            self.symbols.open_g_time.isna(),
            self.symbols.max_e_time + self.params['TIME_STEP'],
            inplace=True
        )
        self.symbols.max_e_time.mask(
            self.symbols.max_e_time.isna(),
            self.symbols.min_data_time - self.params['TIME_STEP'],
            inplace=True
        )
        self.symbols = self.symbols.astype({'open_g_time':'int64', 'max_e_time':'int64'})
        self.symbols['last_req'] = 0.0

        if self.verbosity >= 3:
            if f_reset_limits: print("done.")
            else: print("DB setup recieved.")

    async def __aenter__(self):
        if not self.__f_open:
            await file_lock(self.__db_name, self.__DB_RETRY)
            self.__db = sql.connect(self.__db_name)
            self.__cursor = self.__db.cursor()
            self.__f_open = True
        return self

    async def __aexit__(self, exc_type=None, exc=None, tb=None):
        if self.__f_open:
            self.__cursor.close()
            self.__db.close()
            self.__f_open = False
            file_unlock(self.__db_name)

    async def truncate_klines(self, symbol_id, last_time):

        if self.verbosity >= 2:
            print("--- Truncating zero trades... ", end=' ', flush=True)
        async with self:
            self.__cursor.execute(self.__reqs['truncate_klines'].format(
                sym_id    = symbol_id,
                last_time = last_time
            ))
            self.__update_limit(symbol_id, 'max_data_time', last_time)
            self.__db.commit()
        if self.verbosity >= 3:
            print("done.")

    async def clear_patterns(self):

        print("\n!!! ATTENTION !!!\nDeleting ALL patterns for {}...".format(self.symbol_list))
        conf = input("Are you sure? Type 'YES' to confirm: ")
        if conf != 'YES': return
        req = self.__reqs['clear_patterns']
        async with self:
            for t in ['open_groups', 'gr_ex_rels', 'groups', 'extremums']:
                cnt = self.__cursor.execute(req.format(tbl=t)).rowcount
                print("   {} records deleted form table '{}'.".format(cnt, t))
            self.__cursor.execute(self.__reqs['clear_limits'])
            self.__db.commit()
        self.symbols['open_g_time'] = self.symbols.min_data_time
        self.symbols['max_e_time']  = self.symbols.min_data_time - self.params['TIME_STEP']
        print("Patterns deleted.\n")

    async def reset_symbol_limits(self):

        if self.verbosity >= 3:
            print("Calculating all symbol limits...")
        if self.__f_open:
            need_close = False
        else:
            need_close = True
            await self.__aenter__()
        need_init = self.__cursor.execute(self.__reqs['symbol_limits_need_init']).fetchall()
        if need_init:
            cnt = self.__cursor.executemany(self.__reqs['init_limit'], need_init).rowcount
            self.__db.commit()
            if self.verbosity >= 3:
                print("   {} absent symbols registered.".format(cnt))
        need_reset = self.__cursor.execute(
                "SELECT symbol_id, symbol_id, symbol_id FROM symbols"
        ).fetchall()
        for req in ['reset_limits_k', 'reset_limits_e', 'reset_limits_g']:
            cnt = self.__cursor.executemany(self.__reqs[req], need_reset).rowcount
            if self.verbosity >= 3:
                print("   {} limits calculated ({}).".format(cnt, req))
        self.__db.commit()
        if need_close: await self.__aexit__()
        if self.verbosity >= 3:
            print("Limits calculations finished.")

    async def get_open_groups(self, symbol_id=None, archived=None):
        idx_columns = ['open_time', 'minimum', 'e_count']
        if archived:
            req = self.__reqs['archived_groups'].format(time=archived)
            idx_columns.append('symbol_id')
        elif symbol_id:
            req = self.__reqs['open_groups'].format(sym_id=symbol_id)
        else:
            req = self.__reqs['all_open_groups']
            idx_columns.append('symbol_id')
        async with self:
            result = pd.read_sql(req, self.__db).astype({'minimum':'bool'})
        result.set_index(idx_columns, inplace=True)
        return result

    async def mark_open_groups(self, open_g, mark):
        async with self:
            self.__cursor.executemany(
                self.__reqs[mark],
                open_g.reset_index().to_dict('records')
            )
            if mark == 'mark_monitor':
                self.__cursor.execute(self.__reqs['reset_monitor_time'])
            self.__db.commit()

    async def reset_open_groups_rank(self):
        async with self:
            self.__cursor.execute(self.__reqs['reset_og_rank'])
            self.__db.commit()

    async def sql_load(self, sql_req):
        async with self:
            return pd.read_sql(sql_req, self.__db)

    async def sql_execute(self, sql_req):
        async with self:
            cnt = self.__cursor.execute(sql_req).rowcount
            self.__db.commit()
        return cnt

    async def get_variable(self, var_name):
        if self.__f_open:
            need_close = False
        else:
            need_close = True
            await self.__aenter__()
        result = self.__cursor.execute(
            "SELECT value FROM vars WHERE variable = ?", (var_name,)
        ).fetchone()[0]
        if need_close: await self.__aexit__()
        return result

    async def set_variable(self, var_name, value, get_last=False):
        if self.__f_open:
            need_close = False
        else:
            need_close = True
            await self.__aenter__()
        if get_last:
            last_value = self.get_variable(var_name)
        self.__cursor.execute(
            "UPDATE vars SET value = ? WHERE variable = ?",
            (value, var_name)
        )
        self.__db.commit()
        if need_close: await self.__aexit__()
        if get_last:   return last_value

    async def load_data(self, symbol_id, start_time, end_time, klines_only=False):

        if self.verbosity >= 3:
            print("Loading DB data...\n   Period to load (UTC)...                    from {} to {}".format(
                pd.to_datetime(start_time, unit='ms').strftime(self.TIME_FORMAT),
                pd.to_datetime(end_time,   unit='ms').strftime(self.TIME_FORMAT)
            ))

        async with self:
            k_data = pd.read_sql(self.__reqs['klines_read'].format(
                sym_id     = symbol_id,
                start_time = start_time,
                end_time   = end_time
            ), self.__db).astype({
                'open_time' : 'int64',
                'open'      : 'float64',
                'high'      : 'float64',
                'low'       : 'float64',
                'close'     : 'float64',
                'volume'    : 'float64'
            })
            k_data.set_index('open_time', inplace=True)
            k_data.sort_index(inplace=True)

            if klines_only:
                if self.verbosity >= 3:
                    print("   Klines loaded...                           {}".format(k_data.shape[0]))
                return k_data

            e_data = pd.read_sql(self.__reqs['extremums_read'].format(
                sym_id     = symbol_id,
                start_time = start_time,
                end_time   = end_time
            ), self.__db).astype({
                'open_time'        : 'int64',
                'minimum'          : 'bool',
                'price'            : 'float64',
                'range'            : 'int64',
                'r_weight'         : 'int64',
                'v_factor'         : 'float64',
                'r_factor'         : 'float64',
                'tolerance'        : 'float64',
                'low_tol'          : 'float64',
                'high_tol'         : 'float64',
                'next_high'        : 'int64',
                'next_high_closed' : 'bool',
                'next_low'         : 'int64',
                'next_low_closed'  : 'bool',
                'last_ex'          : 'int64',
                'last_non_ex'      : 'int64',
                'height'           : 'float64',
                'derivative'       : 'float64'
            })
            e_data.set_index(['open_time','minimum'], inplace=True)
            e_data.sort_index(inplace=True)
            e_data['changed'] = False

            g_data = pd.read_sql(self.__reqs['groups_read'].format(
                sym_id     = symbol_id,
                start_time = start_time,
                end_time   = end_time
            ), self.__db).astype({
                'open_time'     : 'int64',
                'minimum'       : 'bool',
                'e_count'       : 'int64',
                'primary'       : 'bool',
                'range'         : 'int64',
                'g_tol'         : 'float64',
                'low_tol'       : 'float64',
                'high_tol'      : 'float64',
                'r_factor'      : 'float64',
                'rebound'       : 'int64',
                'pierce_factor' : 'float64',
                'class_id'      : 'int64',
                'status_id'     : 'int64',
                'rank'          : 'float64',
                'result'        : 'float64',
                'result_id'     : 'int64'
            })
            g_data.set_index(['open_time','minimum','e_count'], inplace=True)
            g_data.sort_index(inplace=True)
            g_data['changed'] = False

            ge_data = pd.read_sql(self.__reqs['relations_read'].format(
                sym_id     = symbol_id,
                start_time = start_time,
                end_time   = end_time
            ), self.__db).astype({
                'open_time' : 'int64',
                'minimum'   : 'bool',
                'e_count'   : 'int64',
                'e_time'    : 'int64'
            })
            if self.verbosity >= 3:
                print("   Klines / extremums / groups loaded...      {}/{}/{}".format(
                    k_data.shape[0], e_data.shape[0], g_data.shape[0]
                ))

        return k_data, e_data, g_data, ge_data

    async def save_data(self, symbol_id, data_set):

        if self.verbosity >= 3:
            print("Saving data into DB ...")

        new_e     = data_set.e_data.drop(data_set.known_e.index)
        changed_e = data_set.e_data.drop(new_e.index)
        changed_e.reset_index(inplace=True)
        changed_e['symbol_id'] = symbol_id
        changed_e = changed_e.loc[changed_e.changed, [
            'symbol_id',
            'open_time',
            'minimum',
            'range',
            'r_weight',
            'next_high',
            'next_high_closed',
            'next_low',
            'next_low_closed'
        ]].astype({
            'next_high_closed' : 'int64',
            'next_low_closed'  : 'int64',
            'minimum'          : 'int64'
        }).to_dict('records')
        new_e.rename(columns={
            'range'       : 'range_time',
            'next_high'   : 'next_high_time',
            'next_low'    : 'next_low_time',
            'last_ex'     : 'last_ex_time',
            'last_non_ex' : 'last_non_ex_time'
        }, inplace=True)
        new_e.drop(columns='changed', inplace=True)
        new_e['param_set_id'] = self.param_set_id
        new_e['interval']     = self.interval
        new_e['symbol_id']    = symbol_id

        new_g     = data_set.g_data.drop(data_set.known_g.index)
        changed_g = data_set.g_data.drop(new_g.index)
        changed_g.reset_index(inplace=True)
        changed_g['symbol_id'] = symbol_id
        changed_g = changed_g.loc[changed_g.changed, [
            'symbol_id',
            'open_time',
            'minimum',
            'e_count',
            'primary',
            'range',
            'g_tol',
            'low_tol',
            'high_tol',
            'r_factor',
            'rebound',
            'pierce_factor',
            'class_id',
            'status_id',
            'rank',
            'result_id',
            'result'
        ]].astype({
            'primary' : 'int64',
            'minimum' : 'int64'
        }).to_dict('records')
        new_g.rename(columns={
            'range'      : 'range_time',
            'rebound'    : 'rebound_time',
            'result_id'  : 'result_time'
        }, inplace=True)
        new_g.drop(columns='changed', inplace=True)
        new_g['param_set_id'] = self.param_set_id
        new_g['interval']     = self.interval
        new_g['symbol_id']    = symbol_id

        new_ge = data_set.ge_data.set_index(['open_time', 'minimum', 'e_count', 'e_time'])
        new_ge.drop(data_set.known_ge.set_index(['open_time', 'minimum', 'e_count', 'e_time']).index, inplace=True)
        new_ge['param_set_id'] = self.param_set_id
        new_ge['interval']     = self.interval
        new_ge['symbol_id']    = symbol_id

        if not _RECLASSIFY:
            new_og = data_set.g_data.loc[
                (data_set.g_data.status_id == self.statuses['OPEN']) &
                (data_set.g_data.range == _MAXINT), [
                    'class_id',
                    'g_tol',
                    'r_factor',
                    'low_tol',
                    'high_tol',
                    'rank'
                ]
            ].join(data_set.e_data[[
                'price',
                'v_factor',
                'last_ex',
                'last_non_ex',
                'height',
                'derivative'
            ]], on=['open_time', 'minimum'])
            g_time = new_og.index.get_level_values('open_time')
            new_og['last_ex']      = (g_time - new_og.last_ex)     // self.params['TIME_STEP']
            new_og['last_non_ex']  = (g_time - new_og.last_non_ex) // self.params['TIME_STEP']
            new_og['delta_tol']    = new_og.high_tol - new_og.low_tol
            new_og.drop(columns=['low_tol', 'high_tol'], inplace=True)

        async with self:

            if not new_e.empty:
                new_e.to_sql  ('extremums',  self.__db, if_exists='append')
                self.__update_limit(symbol_id, 'max_e_time',  new_e.index.get_level_values('open_time')[-1])
            if not new_g.empty:
                new_g.to_sql  ('groups',     self.__db, if_exists='append')
                new_ge.to_sql ('gr_ex_rels', self.__db, if_exists='append')
                self.__update_limit(symbol_id, 'open_g_time', data_set.g_data.loc[
                    (data_set.g_data.status_id != self.statuses['CLOSED']) &
                    (data_set.g_data.status_id != self.statuses['OUTDATED'])
                ].index.get_level_values('open_time')[0])
            changed_e = self.__cursor.executemany(self.__reqs['extremums_update'], changed_e).rowcount
            changed_g = self.__cursor.executemany(self.__reqs['groups_update'],    changed_g).rowcount
            self.__db.commit()

            if not _RECLASSIFY:
                og_data = pd.read_sql(self.__reqs['open_groups_read'].format(
                    sym_id = symbol_id
                ), self.__db).astype({'minimum':'bool'})
                delete_og = og_data.join(
                    data_set.g_data.loc[
                        (data_set.g_data.status_id != self.statuses['OPEN']) |
                        (data_set.g_data.range != _MAXINT),
                        []
                    ],
                    on=['open_time', 'minimum', 'e_count'],
                    how='inner'
                ).astype({'minimum':'int64'})
                delete_og['the_time'] = datetime.now().timestamp()
                self.__cursor.executemany(
                    self.__reqs['mark_archived'],
                    delete_og.loc[delete_og.monitor != 0].to_dict('records')
                )
                self.__db.commit()
                self.__cursor.executemany(self.__reqs['open_groups_delete'], delete_og.to_dict('records'))
                update_og = og_data.join(
                    new_og,
                    on=['open_time', 'minimum', 'e_count'],
                    how='inner'
                )
                self.__cursor.executemany(
                    self.__reqs['open_groups_update'],
                    update_og.astype({'minimum':'int64'}).to_dict('records')
                )
                self.__db.commit()
                new_og.drop(update_og.set_index(['open_time', 'minimum', 'e_count']).index, inplace=True)
                new_og['param_set_id'] = self.param_set_id
                new_og['interval']     = self.interval
                new_og['symbol_id']    = symbol_id
                new_og['last_price']   = 0.0
                last_idx = data_set.k_data.index[-1]
                min_mask = new_og.index.get_level_values('minimum').to_numpy()
                new_og.loc[ min_mask, 'last_price'] = data_set.k_data.at[last_idx, 'low']
                new_og.loc[~min_mask, 'last_price'] = data_set.k_data.at[last_idx, 'high']
                new_og.to_sql('open_groups', self.__db, if_exists='append')

        if self.verbosity >= 3:
            print("   Extremums saved / updated...               {}/{}".format(new_e.shape[0], changed_e))
            print("   Groups saved / updated...                  {}/{}".format(new_g.shape[0], changed_g))

    async def delete_symbols(self, sym_names):

        if self.verbosity >= 3:
            print("Deleting symbols {}:".format(sym_names))
        delete_request = "DELETE FROM {} WHERE symbol_id IN " + str(
            tuple(map(lambda name: self.symbol_names[name], sym_names))
        ).replace(',)', ')')
        async with self:
            for tbl in [
                'open_groups',
                'gr_ex_rels',
                'groups',
                'extremums',
                'klines',
                'symbol_limits',
                'symbols'
            ]:
                if self.verbosity >= 3:
                    print("   processing table {}...".format(tbl), end=" ", flush=True)
                cnt = self.__cursor.execute(delete_request.format(tbl)).rowcount
                if self.verbosity >= 3:
                    print('{} records deleted.'.format(cnt))
            self.__db.commit()
        if self.verbosity >= 3:
            print("Symbols deleted.")

    async def get_binance_data(self, symbol_id):

        HTTP_ERROR_MSG = "\n--- HTTP error {} - see {}.\n--- Processing terminated."
        HTTP_ERROR_LOG = "[{}] {}: HTTP code {} - {}"
        URL_ERROR_MSG  = "\n--- URL error:\n--- {}\n--- See {}."
        URL_ERROR_LOG  = "[{}] {}: {}"

        if self.verbosity >= 3:
            print("Recieving Binance data...                    ", end=' ', flush=True)
        msg = ''
        retry_left = self.__URL_RETRY
        while retry_left:
            try:
                with url_req.urlopen(_BINANCE_URL + _SERVER_TIME_URL, timeout=20) as response:
                    s_time = int(json.loads(response.read())['serverTime'])
                    if response.status != 200:
                        await log_write(_LOG_URL_ERRORS, HTTP_ERROR_LOG.format(
                            "TIME_REQUEST", "", response.status, response.headers
                        ))
                        msg = HTTP_ERROR_MSG.format(response.status, _LOG_URL_ERRORS)
                        if self.verbosity >= 1: print(msg)
                        raise Exception(msg)
                break
            except (url_err.URLError, TimeoutError) as error:
                msg = "{} : {}".format(type(error), error)
                await log_write(_LOG_URL_ERRORS, URL_ERROR_LOG.format("TIME_REQUEST", "", msg))
                retry_left -= 1
                if retry_left: await asyncio.sleep(10)
        if not retry_left:
            await log_write(_LOG_URL_ERRORS, URL_ERROR_LOG.format("TIME_REQUEST", "", msg))
            if self.verbosity >= 2:
                print(URL_ERROR_MSG.format(msg, _LOG_URL_ERRORS))
            return False

        TIME_STEP     = self.params['TIME_STEP']
        max_data_time = self.symbols.at[symbol_id, 'max_data_time']
        if s_time - max_data_time < TIME_STEP:
            if self.verbosity >= 3:
                print("new data not found.")
            return False
        start_time = max_data_time + TIME_STEP

        url_params = "?symbol={symbol}&interval={interval}m&startTime={start}".format(
            symbol   = self.symbols.at[symbol_id, 'symbol_name'],
            start    = start_time,
            interval = self.interval
        )
        msg = ''
        retry_left = self.__URL_RETRY
        while retry_left:
            try:
                with url_req.urlopen(_BINANCE_URL + _KLINES_URL + url_params, timeout=20) as response:
                    if response.status == 200:
                        new_data = pd.read_json(response, orient='values')
                    else:
                        await log_write(_LOG_URL_ERRORS, HTTP_ERROR_LOG.format(
                            "KLINES_REQUEST",
                            self.symbols.at[symbol_id, 'symbol_name'],
                            response.status,
                            response.headers
                        ))
                        msg = HTTP_ERROR_MSG.format(response.status, _LOG_URL_ERRORS)
                        if self.verbosity >= 1: print(msg)
                        raise Exception(msg)
                break
            except (url_err.URLError, TimeoutError) as error:
                msg = "{} : {}".format(type(error), error)
                await log_write(_LOG_URL_ERRORS, URL_ERROR_LOG.format(
                    "KLINES_REQUEST", self.symbols.at[symbol_id, 'symbol_name'], msg
                ))
                if self.verbosity >= 2:
                    print(URL_ERROR_MSG.format(msg, _LOG_URL_ERRORS))
                retry_left -= 1
                if retry_left: await asyncio.sleep(10)
        if not retry_left:
            await log_write(_LOG_URL_ERRORS, URL_ERROR_LOG.format(
                "KLINES_REQUEST", self.symbols.at[symbol_id, 'symbol_name'], msg
            ))
            if self.verbosity >= 2:
                print(URL_ERROR_MSG.format(msg, _LOG_URL_ERRORS))
            return False
        if new_data.shape[1] != 12:
            msg = "[KLINES_REQUEST] {}: abnormal result shape: {}".format(
                self.symbols.at[symbol_id, 'symbol_name'], new_data.shape
            )
            await log_write(_LOG_URL_ERRORS, msg)
            if self.verbosity >= 2:
                print("\n--- URL error:\n--- {}\n--- See {}.".format(msg, _LOG_URL_ERRORS))
            return False

        new_data.drop(columns=[6,7,9,10,11], inplace=True)
        new_data.rename(columns={
            0 : 'open_time',
            1 : 'open',
            2 : 'high',
            3 : 'low',
            4 : 'close',
            5 : 'volume',
            8 : 'count'
        }, inplace=True)
        new_data = new_data.astype({
            'open_time' : 'int64',
            'open'      : 'float64',
            'high'      : 'float64',
            'low'       : 'float64',
            'close'     : 'float64',
            'volume'    : 'float64',
            'count'     : 'int64'
        })
        new_data['symbol_id'] = symbol_id
        new_data['interval']  = self.interval
        new_data.set_index('open_time', inplace=True)
        new_data.sort_index(inplace=True)
        if new_data.index[0] != start_time and self.symbols.at[symbol_id, 'min_data_time'] != 0:
            raise Exception("Binance returned non-continuous data!")
        async with self:
            new_data.to_sql('klines', self.__db, if_exists='append')
            if self.symbols.at[symbol_id, 'min_data_time'] == 0:
                start_time = new_data.index[0]
                self.__update_limit(symbol_id, 'min_data_time', start_time)
                self.__update_limit(symbol_id, 'open_g_time',   start_time)
                self.__update_limit(symbol_id, 'max_e_time',    start_time  - self.params['TIME_STEP'])
            self.__update_limit(symbol_id, 'max_data_time', new_data.index[-1])
            self.__db.commit()

        if self.verbosity >= 3:
            print("{} klines recieved.".format(new_data.shape[0]))
        return True

    def __update_limit(self, symbol_id, field, new_value):
        if field in ['max_data_time', 'min_data_time']:
            req = self.__reqs['update_limit_all_ps']
        else:
            req = self.__reqs['update_limit']
        self.__cursor.execute(req.format(
            field     = field,
            new_value = new_value,
            sym_id    = symbol_id
        ))
        if new_value == 'NULL':
            new_value = _NaN
        self.symbols.at[symbol_id, field] = new_value

    async def save_tick_size(self, symbol_id, tick_size):
        async with self:
            self.__cursor.execute(
                "UPDATE symbols SET tick_size = {} WHERE symbol_id = {}".format(tick_size, symbol_id)
            )
            self.__db.commit()
