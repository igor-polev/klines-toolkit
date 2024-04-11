#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project: Klines Toolkit
File:    Data exploration script

Created on Mon Dec 25 09:01:57 2023
@author: IGOR POLEV
"""

_TEST_FACTOR = 'rank'
_BIN_CNT      = 10
_MAX_LIMIT    = 2.2
_TEST_LIMIT   = None
_CLASS_ID     = None

import pandas as pd
import asyncio
from nest_asyncio import apply as async_apply

from kt_binancedb import BinanceDB

async def main():

    db = BinanceDB()
    print("Requesting DB...", end=' ', flush=True)
    e_data = await db.sql_load("""
        SELECT
            open_time,
            minimum,
            price,
            v_factor,
            (open_time - last_ex_time ) / 300000 AS last_ex,
            (open_time - last_non_ex_time) / 300000 AS last_non_ex,
            height,
            derivative
        FROM extremums
        WHERE
            param_set_id = 4
    """)
    e_data.set_index(['open_time', 'minimum'], inplace=True)
    df = await db.sql_load("""
        SELECT
            open_time,
            minimum,
            e_count,
            rank,
            range_time - open_time AS distance,
            high_tol - low_tol AS delta_tol,
            g_tol,
            abs(result) as result,
            class_id,
            status_id
        FROM groups
        WHERE
            param_set_id = 4 AND
            class_id     = 2
    """)
    df = df.set_index(['open_time', 'minimum', 'e_count']).join(e_data, on=['open_time', 'minimum'])
    print("done.")
    print('\n')
    
    if _CLASS_ID:
        df = df.loc[df.class_id == _CLASS_ID]
    if _TEST_FACTOR in ['last_ex']:
        df = df.loc[df[_TEST_FACTOR] != 0]
    if _MAX_LIMIT:
        df = df.loc[df[_TEST_FACTOR] <= _MAX_LIMIT]
    
    df['result_class'] = pd.cut(df.result, (-1.0, 0.005, 0.05, df.result.max()), labels=['poor', 'good', 'rare'])
    
    if _TEST_LIMIT:
        df['bin'], bins = pd.cut(df[_TEST_FACTOR], (-1.0, _TEST_LIMIT, df[_TEST_FACTOR].max()), labels=[0,1], retbins=True)
    else:
        df['bin'], bins = pd.cut(df[_TEST_FACTOR], _BIN_CNT, labels=range(_BIN_CNT), retbins=True)
    df_bins = df.loc[:, ['bin', 'result_class', 'result']].groupby(['bin', 'result_class']).count()
    df_totals = df_bins.reset_index().loc[:,['bin', 'result']].groupby('bin').sum()
    
    df_result = df_bins.xs('good', level='result_class').join(df_totals, on='bin', rsuffix='_total')
    df_result.rename(columns={'result':'cnt','result_total':'total'}, inplace=True)
    df_result['prob'] = df_result.cnt / df_result.total
    print('\n')
    print(df_result)
    print('\nBins:')
    for i in range(len(bins)-1):
        print(i, '\t (', bins[i], ':', bins[i+1], ')')

# =============================================================================
# print("Plotting...", end=' ', flush=True)
# df.loc[
#     (df.result > 0.0)
#     & (df.result <= 0.05)
#     & (df.vf_bin == 9)
# ].hist(
#     column='result',
#     bins=50,
#     # xlim=(0.0, 0.1),
#     figsize=(20,10)
# )
# =============================================================================

# =============================================================================
# print(bins)
# df.info()
# print(df.iat[0,12])
# =============================================================================

# =============================================================================
# g_data.loc[g_data.class_id == 2].plot(
#     x='g_tol',
#     y='result',
#     kind='scatter',
#     # ylim=(0.0, 0.05),
#     figsize=(25,10))
# print("done.")
# =============================================================================

if __name__ == "__main__":
    async_apply()
    asyncio.run(main())
