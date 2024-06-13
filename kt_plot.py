#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project: Klines Toolkit
File:    Data visualization script

Created on Mon Nov 15 15:09:00 2023
@author: IGOR POLEV
"""

_SYM   = 'RADUSDT'
_START = '2024-05-04'
_DAYS  = 2


from sys import maxsize as _MAXINT
# import random
import pandas as pd
import asyncio

from kt_binancedb import BinanceDB
from kt_graph     import KGraph

from kt_params import _PIC_WIDTH
from kt_params import _PIC_HEIGHT
from kt_params import _GROUP_SHOW_DELTA_HOURS

async def show_plot(sym_name, day_start, day_cnt):

    print("\n--- Klines Toolkit data visualization script ---\n")
    print("Drawing {days} days of {sym} starting from {start}...\n".format(
        sym   = sym_name,
        days  = day_cnt,
        start  = day_start
    ))
    
    db = BinanceDB([sym_name])
    await db.setup()
    graph = KGraph(
        classes=db.classes,
        statuses=db.statuses,
        pic_size=(2268, 1285),
        scale=5,
        renderer='browser'
    )

    start_time = int(pd.to_datetime(day_start).timestamp()) * 1000
    end_time   = start_time + day_cnt * 86400000
    k_data, e_data, g_data, ge_data = await db.load_data(db.symbol_names[sym_name], start_time, end_time)

    # g_data = g_data.loc[g_data.primary & (g_data.index.get_level_values('e_count').to_numpy() > 1)]
    # g_data = g_data.loc[(g_data.class_id == db.classes['UNKNOWN']) | (g_data.status_id == db.statuses['UNDEFINED'])]
    # g_data = g_data.loc[(g_data.class_id == db.classes['PIERCE_READY']) & (g_data.status_id == db.statuses['CLOSED'])]
    # g_data = g_data.loc[(g_data.class_id == db.classes['SECOND_TOUCH']) & (g_data.result_id != -1)]
    # e_data = e_data.xs(False, level='minimum', drop_level=False)

    graph.set_data(sym_name, k_data, e_data, g_data, ge_data)
    # await graph.display_group((1705467900000,False,3), how='screen')
        
    await graph.display_data([
        'VOLUMES',
        'GROUP_RANGES',
        # 'EXTREMUM_RANGES',
        # 'GROUP_RANKS',
        # 'LAST_EXTREMUM',
        # 'LAST_NON_EXTREMUM',
        # 'EX_HEIGHT',
        'GROUP_EXTREMUMS',
        # 'EXTREMUMS',
        # 'DERIVATIVE',
        # 'GROUP_TOLERANCES',
        # 'GROUP_RESULTS',
        # 'NEXT_PRICES',
        # 'REBOUNDS',
        'KLINES'
    ])

    print("\nDrawing done.")

async def show_groups():
    
    db = BinanceDB()
    await db.setup()
    graph = KGraph(db.classes, db.statuses, pic_size=(_PIC_WIDTH, _PIC_HEIGHT), scale=10, renderer='png')
    
    open_g = await db.get_open_groups()
    o_times = open_g[['symbol_id', 'open_time']].groupby('symbol_id').min()
    o_times.open_time -= _GROUP_SHOW_DELTA_HOURS * 3600000
    for gr in open_g.itertuples(index=False):
        k, e, g, ge = await db.load_data(gr.symbol_id, o_times.at[gr.symbol_id, 'open_time'], _MAXINT)
        graph.set_data(db.symbols.at[gr.symbol_id, 'symbol_name'], k, e, g, ge)
        await graph.display_group((gr.open_time, gr.minimum, gr.e_count), how='screen')
        input("\npress Enter...")

if __name__ == "__main__":
    asyncio.run(show_plot(_SYM, _START, _DAYS))
    # asyncio.run(show_groups())
