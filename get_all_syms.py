#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project: Klines Toolkit
File:    Get all symbols script

Created on Fri Apr 12 15:08:40 2024
@author: IGOR POLEV
"""

import asyncio
from nest_asyncio import apply as async_apply

import json
import urllib.request
import pandas as pd
import sqlite3 as sql

from kt_params import _BINANCE_URL
from kt_params import _INFO_URL
from kt_params import _DB_FILE

async def main():

    def get_tick(row):
        for d in row.filters:
            if 'tickSize' in d.keys():
                return d['tickSize']
        raise Exception("Tick size not found for {}".format(row.symbol))
    
    print("Requesting Binance...", end=' ', flush=True)
    with urllib.request.urlopen(_BINANCE_URL + _INFO_URL) as response:
        print("response recieved with code ", response.status)
        data = json.loads(response.read())['symbols']
    
    df = pd.DataFrame(data=data)
    df = df.loc[:, [
        'symbol',
        'status',
        'onboardDate',
        'filters'
    ]]
    df['tickSize'] = df.apply(get_tick, axis=1)
    df = df.drop(columns='filters').astype({
        'symbol' : 'str',
        'status' : 'str',
        'onboardDate' : 'int64',
        'tickSize' : 'float64'
    })
    df.set_index('symbol', inplace=True)

    print("Adding table 'einfo' to database...", end=' ', flush=True)
    db = sql.connect(_DB_FILE)
    df.to_sql('einfo', db)
    db.close()
    print("done.")

    return

if __name__ == "__main__":
    async_apply()
    asyncio.run(main())
