#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project: Klines Toolkit
File:    Script for downloading archive Binance data

Created on Mon Oct 16 09:40:57 2023
@author: IGOR POLEV
"""

# Script to download Binance public data and parse it to SQL database
# based on code of Binance scripts from GitHub:
# https://github.com/binance/binance-public-data/tree/master/python

# REQUIRED FILES:
# enums.py
# utility.py
# from enums import *
from utility import get_path, download_file #, get_all_symbols

# params section

_DEBUG_OUTPUT = False

_DATA_PORTIONS = 'monthly' # 'daily' or 'monthly'
_CHECK_EXIST_IN_DB = False

_SYMBOLS = [
    # '1000BTTCUSDT',
    # 'AKROUSDT',
    # 'ANCUSDT',
    'BTCSTUSDT',
    'BTSUSDT',
    # 'BTTUSDT',
    # 'BZRXUSDT',
    'COCOSUSDT',
    'CVCUSDT',
    # 'DODOUSDT',
    # 'DOTECOUSDT',
    'FTTUSDT',
    'HNTUSDT',
    # 'KEEPUSDT',
    'LENDUSDT',
    # 'LUNAUSDT',
    # 'NUUSDT',
    'RAYUSDT',
    'SCUSDT',
    'SRMUSDT',
    'TOMOUSDT',
    # 'YFIIUSDT'
]
_FILL_TABLE = 'klines'
_INTERVALS = ['5m'] # ['1m', '5m']
# _SYMBOLS = get_all_symbols("um")
# _SYMBOLS = []
_YEARS = [2024]
_MONTHS = range(1,4)
# _DAYS = [29]
# _DAYS = range(15, 31)

_SAVE_PATH = "/home/igor/Archive/Binance"
_DB_FILE = "/home/igor/Crypto/Binance/binance_data.db"

# Import section

# import sys
# from argparse import ArgumentParser, RawTextHelpFormatter, ArgumentTypeError
# from datetime import *
import pandas as pd
import sqlite3 as sql

# Main section

print("\n-- Binance data retriver script --\n")

print("DB request...", end=' ', flush=True)
db = sql.connect(_DB_FILE)
db_symbols = pd.read_sql("SELECT symbol_id, symbol_name FROM symbols", db, index_col='symbol_name')
if _FILL_TABLE == 'aggTrades':
    get_columns = [
        'agg_trade_id',
        'price',
        'quantity',
        'transact_time'
    ]
    if _CHECK_EXIST_IN_DB and _DATA_PORTIONS != 'daily':
        db_periods = pd.read_sql("SELECT DISTINCT symbol_id, year, month FROM agg_trades", db)
    else:
        db_periods = pd.DataFrame()
elif _FILL_TABLE == 'klines':
    get_columns = [
        'open_time',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'count'
    ]
    if _CHECK_EXIST_IN_DB and _DATA_PORTIONS != 'daily':
        db_periods = pd.read_sql("SELECT DISTINCT symbol_id, year, month, interval FROM klines", db)
    else:
        db_periods = pd.DataFrame()
else:
    db.close()
    raise Exception("\nUnknown table '{}'".format(_FILL_TABLE))
print('done.')

_INTERVAL_DB_CODES = {
    '1m' : 1,
    '5m' : 5
}
_BINANCE_DATA_TYPES = {
    'agg_trade_id'    : 'int64',
    'price'           : 'float64',
    'quantity'        : 'float64',
    'first_trade_id'  : 'int64',
    'last_trade_id'   : 'int64',
    'transact_time'   : 'int64',
    'is_buyer_maker'  : 'bool',
    'open_time'       : 'int64',
    'open'            : 'float64',
    'high'            : 'float64',
    'low'             : 'float64',
    'close'           : 'float64',
    'volume'          : 'float64',
    'close_time'      : 'int64',
    'count'           : 'int64'
}
_DEFAULT_CSV_COLUMNS = [
    'open_time',
    'open',
    'high',
    'low',
    'close',
    'volume',
    'close_time',
    'quote_volume',
    'count',
    'taker_buy_volume',
    'taker_buy_quote_volume',
    'ignore'
]

if _FILL_TABLE == 'aggTrades': _INTERVALS = [None]
if _DATA_PORTIONS != 'daily': _DAYS = [None]

if not _SYMBOLS:
    _SYMBOLS = db_symbols.index.array
symbols_count = len(_SYMBOLS)
for i, symbol in enumerate(_SYMBOLS):
    print("\n-------------------------------\n[{}/{}] - downloading monthly {} data\n-------------------------------".format(i+1, symbols_count, symbol))
    try:
        symbol_id = db_symbols.at[symbol, 'symbol_id']
    except KeyError:
        print("No such symbol in database. Adding.")
        db.execute("INSERT INTO symbols (symbol_name) VALUES ('{}')".format(symbol))
        db.commit()
        db_symbols = pd.read_sql("SELECT symbol_id, symbol_name FROM symbols", db, index_col='symbol_name')
    try:
        symbol_id = db_symbols.at[symbol, 'symbol_id']
    except KeyError:
        raise Exception("\nFailed to add {} symbol to database!".format(symbol))
    for year in _YEARS:
        for month in _MONTHS:
            if _FILL_TABLE == 'aggTrades':
                if _CHECK_EXIST_IN_DB and not db_periods.empty:
                    if not db_periods.loc[
                            (db_periods['symbol_id'] == symbol_id) &
                            (db_periods['year'] == year) &
                            (db_periods['month'] == month)
                        ].empty:
                        print("Trade data for {} in {:02d}.{} allready exists in database. Skipping.".format(symbol, month, year))
                        continue
            for interval in _INTERVALS:
                if _CHECK_EXIST_IN_DB and _FILL_TABLE == 'klines' and not db_periods.empty:
                    if not db_periods.loc[
                            (db_periods['symbol_id'] == symbol_id) &
                            (db_periods['year'] == year) &
                            (db_periods['month'] == month) &
                            (db_periods['interval'] == _INTERVAL_DB_CODES[interval])
                        ].empty:
                        print("{} kline data for {} in {:02d}.{} allready exists in database. Skipping.".format(interval, symbol, month, year))
                        continue
                path = get_path("um", _FILL_TABLE, _DATA_PORTIONS, symbol, interval)
                if _FILL_TABLE == 'klines':
                    file_name = "{}-{}-{}-{:02d}.zip".format(symbol.upper(), interval, year, month)
                elif _FILL_TABLE == 'aggTrades':
                    file_name = "{}-aggTrades-{}-{:02d}.zip".format(symbol.upper(), year, month)
                for day in _DAYS:
                    if _DATA_PORTIONS == 'daily':
                        if _FILL_TABLE == 'klines':
                            file_name = "{}-{}-{}-{:02d}-{:02d}.zip".format(symbol.upper(), interval, year, month, day)
                        elif _FILL_TABLE == 'aggTrades':
                            file_name = "{}-aggTrades-{}-{:02d}-{:02d}.zip".format(symbol.upper(), year, month, day)
                    try:
                        download_file(path, file_name, folder=_SAVE_PATH)
                    except:
                        print("Failed to download file. Skipping.")
                        continue
                    try:
                        sym_data = pd.read_csv(
                            _SAVE_PATH + '/' + file_name,
                            usecols=get_columns,
                            dtype=_BINANCE_DATA_TYPES,
                            engine='c'
                        )
                    except ValueError:
                        print("\nFailed to parse CSV with column names - parsing with default column names!")
                        sym_data = pd.read_csv(
                            _SAVE_PATH + '/' + file_name,
                            names=_DEFAULT_CSV_COLUMNS,
                            usecols=get_columns,
                            dtype=_BINANCE_DATA_TYPES,
                            engine='c'
                        )
                    sym_data['symbol_id'] = symbol_id
                    sym_data['year'] = year
                    sym_data['month'] = month
                    if _FILL_TABLE == 'klines':
                        sym_data['interval'] = _INTERVAL_DB_CODES[interval]
                    try:
                        sym_data.to_sql(_FILL_TABLE, db, if_exists='append', index=False)
                        print("\n[{}] records for period {:02d}.{} added to database.".format(sym_data.shape[0], month, year))
                    except:
                        print("\nFailed to add [{}] records to database.".format(sym_data.shape[0]))
db.close()
print("Done!")