#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project: Klines Toolkit
File:    KAnalizer class

Created on Mon Oct 16 09:54:04 2023

@author: IGOR POLEV
"""

from sys      import maxsize as _MAXINT
from numpy    import nan     as _NaN
from datetime import datetime
from math     import floor, log10, log, exp

import pandas as pd

from kt_params import _RECLASSIFY

class KAnalizer:

    def __init__(self, params):

        self.params     = params
        self.tick_size  = None
        
        # params for distance penalty function
        self.dp_a = 24.0
        self.dp_k = log(100.0) / (self.params['EXTR_FRAME_SIZE'] - 24.0)

        self.clear_k_data()
        self.__e_empty = pd.DataFrame(
            data = {
                'price'            : [0.0],
                'range'            : [0],
                'r_weight'         : [0],
                'v_factor'         : [0.0],
                'tolerance'        : [0.0],
                'low_tol'          : [0.0],
                'high_tol'         : [0.0],
                'next_high'        : [0],
                'next_high_closed' : [False],
                'next_low'         : [0],
                'next_low_closed'  : [False],
                'last_ex'          : [0],
                'last_non_ex'      : [0],
                'height'           : [0.0],
                'derivative'       : [0.0],
                'changed'          : [False]
            },
            index = pd.MultiIndex(
                levels=([0],[False]),
                codes=([0],[0]),
                names=('open_time','minimum'),
                sortorder=0
            )
        ).drop((0,False))
        self.e_data = self.__e_empty.copy()
        self.g_data = pd.DataFrame(
            data = {
                'primary'       : [False],
                'range'         : [0],
                'g_tol'         : [0.0],
                'low_tol'       : [0.0],
                'high_tol'      : [0.0],
                'rebound'       : [0],
                'pierce_factor' : [0.0],
                'class_id'      : [0],
                'status_id'     : [0],
                'rank'          : [0.0],
                'result'        : [0.0],
                'result_id'     : [0],
                'changed'       : [False]
            },
            index = pd.MultiIndex(
                levels=([0],[False],[0]),
                codes=([0],[0],[0]),
                names=('open_time','minimum','e_count'),
                sortorder=0
            )
        ).drop((0,False,0))
        self.ge_data = pd.DataFrame(
            data = {
                'open_time' : [0],
                'minimum'   : [False],
                'e_count'   : [0],
                'e_time'    : [0]
            },
        ).drop(0)

        self.reset_known()

    def add_k_data(self, k_data):
        self.k_data = pd.concat([self.k_data, k_data])
        self.k_data.sort_index(inplace=True)

    def clear_k_data(self):
        self.k_data = pd.DataFrame(
            data = {
                'open'   : [0.0],
                'high'   : [0.0],
                'low'    : [0.0],
                'close'  : [0.0],
                'volume' : [0.0]
            },
            index = pd.Index(data=[0], name='open_time')
        ).drop(0)

    def set_changed(self, changed):
        self.e_data.changed = changed
        self.g_data.changed = changed

    def find_extremums(self):

        TIME_STEP           = self.params['TIME_STEP']
        E_FRAME_SIZE        = self.params['EXTR_FRAME_SIZE']
        E_FRAME             = E_FRAME_SIZE * TIME_STEP
        V_FRAME             = self.params['VOLUME_FRAME_SIZE'] * TIME_STEP
        T_FRAME             = self.params['TOLERANCE_FRAME_SIZE'] * TIME_STEP
        T_ORDER             = self.params['TOLERANCE_ORDER']
        T_FACTOR            = self.params['MAX_TOLERANCE_FACTOR']
        RANGE_OC_TOL_WEIGHT = self.params['RANGE_OC_TOL_WEIGHT']
        RANGE_OC_WEIGHT     = self.params['RANGE_OC_WEIGHT']
        RANGE_E_TOL_WEIGHT  = self.params['RANGE_E_TOL_WEIGHT']
        RANGE_E_WEIGHT      = self.params['RANGE_E_WEIGHT']
        S_FACTOR            = self.params['DERIVATIVE_SMOOTH_FACTOR']
        TOO_OLD             = self.params['TOO_OLD_LIMIT'] * TIME_STEP
        ISL = pd.IndexSlice

        def eval_volume(kline, e_field, e_sign):
            if (kline.close - kline.open) * e_sign > 0.0:
                return (kline[e_field] - kline.close) / (kline[e_field] - kline.open) * kline.volume
            return kline.volume

        def limit_by_derivative(price_data, dup_keep):
            p_data = price_data.droplevel('minimum')
            if p_data.index.has_duplicates:
                p_data = p_data.loc[~p_data.index.duplicated(keep=dup_keep)]
            if p_data.shape[0] < 3:
                return _MAXINT
            p_data['derivative'] = _NaN
            p_data.iloc[1:, 1] = (p_data.iloc[1:, 0].array - p_data.iloc[:-1, 0].array) / (p_data.index[1:] - p_data.index[:-1]) * TIME_STEP
            p_data['ewma'] = p_data.derivative.ewm(alpha=S_FACTOR, adjust=False, ignore_na=True).mean()
            p_data['sign_change'] = _NaN
            p_data.iloc[2:, 3] = p_data.iloc[2:, 2].array * p_data.iloc[1:-1, 2].array
            limit_set = p_data.loc[p_data.sign_change < 0.0]
            if limit_set.empty:
                return _MAXINT
            return limit_set.index[0]

        print("   Searching for local extremums ...         ", end=' ', flush=True)
        t_start = datetime.now().timestamp()
        min_found = []
        max_found = []
        if self.known_e.empty:
            first_new = 0
        else:
            first_new = self.k_data.index.get_loc(self.known_e.index[-1][0]) + 1
        for i in self.k_data.index[first_new + E_FRAME_SIZE : -E_FRAME_SIZE]:
            sk_data = self.k_data.truncate(before = i - E_FRAME, after = i + E_FRAME)
            if i == sk_data.low.idxmin(): min_found.append(i)
            if i == sk_data.high.idxmax(): max_found.append(i)
        print("done in {:.2f} seconds.".format(datetime.now().timestamp() - t_start))

        print("   Evaluating volume, range and roundness ...", end=' ', flush=True)
        t_start = datetime.now().timestamp()

        e_data  = self.__e_empty.copy()
        se_data = self.__e_empty.copy()
        for ex_min in [False, True]:
            if ex_min:
                new_found = min_found
                e_sign    = -1.0
                e_field   = 'low'
                e_tol     = 'low_tol'
                ne_tol    = 'high_tol'
                e_fun     = min
                ne_fun    = max
            else:
                new_found = max_found
                e_sign    = 1.0
                e_field   = 'high'
                e_tol     = 'high_tol'
                ne_tol    = 'low_tol'
                e_fun     = max
                ne_fun    = min
            if self.known_e.empty:
                known_m = self.known_e
            else:
                known_m = self.known_e.xs(ex_min, level='minimum')

            if new_found:
                e_data = self.k_data.loc[new_found, [e_field]].copy()
                e_data.rename(columns={e_field:'price'}, inplace=True)
                e_data['range']            = _MAXINT
                e_data['r_weight']         = 100
                e_data['v_factor']         = 0.0
                e_data['r_factor']         = 0.0
                e_data['tolerance']        = 0.0
                e_data['low_tol']          = 0.0
                e_data['high_tol']         = 0.0
                e_data['last_ex']          = 0
                e_data['last_non_ex']      = 0
                e_data['height']           = 0.0
                e_data['derivative']       = 0.0
                e_data['next_high']        = -1
                e_data['next_high_closed'] = False
                e_data['next_low']         = -1
                e_data['next_low_closed']  = False
                e_data['changed']          = True
                for i in e_data.index:

                    # evaluation of volume
                    e_oc = e_fun(self.k_data.at[i,'open'], self.k_data.at[i,'close'])
                    vol = eval_volume(self.k_data.loc[i], e_field, e_sign) + \
                          eval_volume(self.k_data.loc[i + TIME_STEP], e_field, e_sign)
                    if e_sign * self.k_data.at[i - TIME_STEP, e_field] > e_sign * e_oc:
                        vol += eval_volume(self.k_data.loc[i - TIME_STEP], e_field, e_sign)
                    e_data.at[i,'v_factor'] = vol / self.k_data.loc[i - V_FRAME : i, 'volume'].median()

                    # evaluation of tolerance
                    sk_data = self.k_data.truncate(before = i - T_FRAME, after = i, copy=True)
                    sk_data['variance'] = sk_data.high - sk_data.low
                    tolerance = self.tick_size * max(
                        1.0,
                        min(
                            sk_data.variance.median(),
                            sk_data.variance.mean()
                        ) * T_ORDER // self.tick_size
                    )
                    e_data.at[i,'tolerance'] = tolerance
                    near = list(range(i - E_FRAME, i + E_FRAME + TIME_STEP, TIME_STEP))
                    near.remove(i)
                    if ex_min:
                        near_e = self.k_data.loc[near,'low'].min()
                    else:
                        near_e = self.k_data.loc[near,'high'].max()
                    price = e_data.at[i, 'price']
                    e_data.at[i, ne_tol] = e_fun(
                        price - e_sign * tolerance * T_FACTOR,
                        ne_fun(
                            price - e_sign * tolerance,
                            e_fun(e_oc, near_e)
                        )
                    )
                    e_data.at[i, e_tol] = price + e_sign * tolerance

                    # evaluation of roundness
                    e_data.at[i, 'r_factor'] = self.__eval_roundness(
                        price,
                        e_data.at[i, 'low_tol'],
                        e_data.at[i, 'high_tol']
                    )
                    
            elif known_m.empty:
                continue

            # evaluation of actuality range
            if known_m.empty:
                last_known = 0
            else:
                last_known = known_m.index[-1]
                if new_found:
                    e_data = pd.concat([known_m, e_data])
                    e_data.sort_index(inplace=True)
                else:
                    e_data = known_m
            for i in e_data.loc[e_data.range == _MAXINT].index:
                idx = max(i, last_known) + TIME_STEP
                r_weight = e_data.at[i, 'r_weight']
                this_e = e_sign * e_data.at[i, 'price']
                this_e_tol = e_sign * e_data.at[i, e_tol]
                last_time = i + TOO_OLD
                for j in e_data.loc[(e_data.index >= idx) & (e_data.index <= last_time) & (this_e < e_sign * e_data.price)].index:
                    next_e = e_sign * e_data.at[j, 'price']
                    next_oc = e_sign * e_fun(self.k_data.at[j,'open'], self.k_data.at[j,'close'])
                    if this_e_tol < next_oc:
                        r_weight -= RANGE_OC_TOL_WEIGHT # expected to be exactly 100
                    elif this_e < next_oc:
                        r_weight -= RANGE_OC_WEIGHT
                    elif this_e_tol < next_e:
                        r_weight -= RANGE_E_TOL_WEIGHT
                    else:
                        r_weight -= RANGE_E_WEIGHT
                    if r_weight <= 0:
                        e_data.at[i,'range'] = j
                        e_data.at[i,'r_weight'] = r_weight
                        e_data.at[i,'changed'] = True
                        break
                if e_data.at[i,'r_weight'] != r_weight:
                    e_data.at[i,'r_weight'] = r_weight
                    e_data.at[i,'changed'] = True
                sk_data = self.k_data.loc[
                    (idx <= self.k_data.index) &
                    (self.k_data.index < min(e_data.at[i,'range'], last_time + TIME_STEP)) &
                    (e_sign * self.k_data[e_field] > this_e_tol),
                    ['open', 'close']
                ].copy()
                if not sk_data.empty:
                    if ex_min:
                        sk_data['oc'] = sk_data.min(axis=1)
                    else:
                        sk_data['oc'] = sk_data.max(axis=1)
                    sk_data = sk_data.loc[e_sign * sk_data.oc > this_e_tol]
                    if not sk_data.empty:
                        e_data.at[i,'range'] = sk_data.index[0]
                        e_data.at[i,'r_weight'] = -100
                        e_data.at[i,'changed'] = True

            e_data['minimum'] = ex_min
            if se_data.empty:
                se_data = e_data
            elif e_data.empty:
                e_data = se_data
            else:
                e_data = pd.concat([se_data, e_data])
            # after 2nd iteration e_data will definitely contain result
        if not e_data.empty:
            e_data.set_index('minimum', append=True, inplace=True)
            e_data.sort_index(inplace=True)
        print("done in {:.2f} seconds.".format(datetime.now().timestamp() - t_start))

        if e_data.index.has_duplicates: raise Exception("Duplicate extremums detected.")

        print("   Evaluating area and derivative ...        ", end=' ', flush=True)
        t_start = datetime.now().timestamp()

        min_data = e_data.xs(True,  level='minimum')
        max_data = e_data.xs(False, level='minimum')
        if self.known_e.empty:
            first_new = 0
        else:
            first_new = e_data.index.get_loc(self.known_e.index[-1]) + 1
        for i in e_data.index[first_new:]:
            ex_time  = i[0]
            ex_min   = i[1]
            if ex_min:
                e_sign  = -1.0
                m_data  = min_data
                nm_data = max_data
            else:
                e_sign  = 1.0
                m_data  = max_data
                nm_data = min_data
            e_price  = m_data.at[ex_time, 'price']
            se_price = e_sign * e_price
            sk_data  = m_data.loc[(e_sign * m_data.price > se_price) & (m_data.index < ex_time)]
            if sk_data.empty:
                last_ex = 0
            else:
                last_ex = sk_data.index[-1]
                e_data.at[i, 'last_ex'] = last_ex
            sk_data = nm_data.truncate(before = last_ex, after = ex_time - TIME_STEP)
            if not sk_data.empty:
                if ex_min:
                    last_ex = sk_data.price.idxmax()
                    # dup_keep = 'first'
                    e_field = 'low'
                else:
                    last_ex  = sk_data.price.idxmin()
                    # dup_keep = 'last'
                    e_field = 'high'
                e_data.at[i, 'last_non_ex'] = last_ex
                e_data.at[i, 'height']      = abs(e_price - sk_data.at[last_ex, 'price'])
                e_data.at[i, 'derivative']  = abs((e_price - self.k_data.at[ex_time - E_FRAME, e_field]) / e_price)

        print("done in {:.2f} seconds.".format(datetime.now().timestamp() - t_start))

        print("   Searching for subsequent prices ...       ", end=' ', flush=True)
        t_start = datetime.now().timestamp()

        last_k = self.k_data.index[-1]
        mask_min = e_data.index.get_level_values('minimum').to_numpy()
        mask_max = ~mask_min
        mask_high_open = ~e_data.next_high_closed
        mask_low_open  = ~e_data.next_low_closed
        se_data = e_data.loc[mask_min & mask_high_open | mask_max & mask_low_open]
        if not se_data.empty:
            for i, ex in se_data.iterrows():
                ex_time = i[0]
                limit_time = min(ex.range, ex_time + TOO_OLD)
                if i[1]:
                    ne_field    = 'next_high'
                    ne_f_closed = 'next_high_closed'
                    dup_keep    = 'last'
                    nm_data     = max_data
                else:
                    ne_field    = 'next_low'
                    ne_f_closed = 'next_low_closed'
                    dup_keep    = 'first'
                    nm_data     = min_data
                limit_time = min(
                    limit_time,
                    limit_by_derivative(e_data.loc[ISL[ex_time : limit_time], ['price']], dup_keep)
                )
                if not ex[ne_f_closed] and limit_time <= last_k:
                    e_data.at[i, ne_f_closed] = True
                    e_data.at[i, 'changed'] = True
                next_set_vals = nm_data.loc[ex_time + TIME_STEP : limit_time, 'price']
                if not next_set_vals.empty:
                    if i[1]:
                        new_value = next_set_vals.idxmax()
                    else:
                        new_value = next_set_vals.idxmin()
                    if ex[ne_field] != new_value:
                        e_data.at[i, ne_field] = new_value
                        e_data.at[i, 'changed'] = True
        se_data = e_data.loc[(e_data.range != _MAXINT) & (mask_min & mask_low_open | mask_max & mask_high_open)]
        if not se_data.empty:
            for i, ex in se_data.iterrows():
                limit_time = i[0] + TOO_OLD
                if i[1]:
                    e_sign     = -1.0
                    e_field    = 'next_low'
                    e_f_closed = 'next_low_closed'
                    dup_keep   = 'first'
                    m_data     = min_data
                    nm_data    = max_data
                else:
                    e_sign     = 1.0
                    e_field    = 'next_high'
                    e_f_closed = 'next_high_closed'
                    dup_keep   = 'last'
                    m_data     = max_data
                    nm_data    = min_data
                next_set_vals = nm_data.loc[
                    (nm_data.index >  ex.range)  &
                    (nm_data.index <= limit_time) &
                    (e_sign * nm_data.price < e_sign * ex.price)
                ]
                if not next_set_vals.empty:
                    limit_time = next_set_vals.index[0]
                limit_time = min(
                    limit_time,
                    limit_by_derivative(e_data.loc[ISL[ex.range : limit_time], ['price']], dup_keep)
                )
                if not ex[e_f_closed] and limit_time <= last_k:
                    e_data.at[i, e_f_closed] = True
                    e_data.at[i, 'changed'] = True
                next_set_vals = m_data.loc[ex.range : limit_time, 'price']
                if not next_set_vals.empty:
                    if i[1]:
                        new_value = next_set_vals.idxmin()
                    else:
                        new_value = next_set_vals.idxmax()
                    if ex[e_field] != new_value:
                        e_data.at[i, e_field] = new_value
                        e_data.at[i, 'changed'] = True

        print("done in {:.2f} seconds.".format(datetime.now().timestamp() - t_start))
        if e_data.empty:
            self.e_data = self.known_e.copy()
        else:
            self.e_data = e_data

    def collect_groups(self, statuses):

        def get_rebound(g_idx, g_price, g_data, em_data, from_idx, to_idx):
            ISL = pd.IndexSlice
            if g_idx[1]:
                e_sign = -1.0
                e_oc  = 'min_oc'
                e_s_tol  = - g_data.at[g_idx, 'low_tol']
                ne_s_tol = - g_data.at[g_idx, 'high_tol']
            else:
                e_sign = 1.0
                e_oc  = 'max_oc'
                e_s_tol  = g_data.at[g_idx, 'high_tol']
                ne_s_tol = g_data.at[g_idx, 'low_tol']
            try:
                e_check = em_data.loc[ISL[
                    from_idx : to_idx,
                    (e_sign * em_data.price >= ne_s_tol) &
                    (e_sign * em_data[e_oc] <= e_s_tol)
                ], :]
            except KeyError:
                e_check = pd.DataFrame()
            if e_check.empty: return -1, 0.0
            r_idx = e_check.index[0]
            r_price = em_data.at[r_idx, 'price']
            if e_sign * r_price > e_s_tol:
                return r_idx[0], abs(r_price / g_price - 1.0)
            else:
                return r_idx[0], 0.0

        def drop_by_minmax(e_check, min_field, max_field, fixed_min, fixed_max, max_delta):
            while max(e_check[max_field].max(), fixed_max) - min(e_check[min_field].min(), fixed_min) > max_delta:
                i_min = e_check[min_field].idxmin()
                i_max = e_check[max_field].idxmax()
                min_d = fixed_min - e_check.at[i_min, min_field]
                max_d = e_check.at[i_max, max_field] - fixed_max
                if max_d > 0.0:
                    if min_d > 0.0:
                        if max_d > min_d:
                            i_drop = i_max
                        else:
                            i_drop = i_min
                    else:
                        i_drop = i_max
                else:
                    if min_d > 0.0:
                        i_drop = i_min
                    else:
                        raise Exception("can't select element")
                e_check.drop(i_drop, inplace=True)
                if e_check.empty: break
            return e_check

        TIME_STEP = self.params['TIME_STEP']
        T_FACTOR  = self.params['MAX_TOLERANCE_FACTOR']
        TOO_OLD   = self.params['TOO_OLD_LIMIT'] * TIME_STEP
        ISL = pd.IndexSlice

        print("   Collecting extremums in primary groups ...", end=' ', flush=True)
        t_start = datetime.now().timestamp()

        if self.known_e.empty:
            first_new = 0
        else:
            first_new = self.known_e.index[-1][0] + TIME_STEP
        g_data = self.e_data.loc[ISL[first_new:], ['low_tol', 'high_tol', 'range', 'r_factor']].copy()
        g_data['e_count']       = 1
        g_data['primary']       = True
        g_data['rebound']       = -1
        g_data['pierce_factor'] = 0.0
        g_data['class_id']      = 0
        g_data['status_id']     = 0
        g_data['rank']          = 0.0
        g_data['result']        = 0.0
        g_data['result_id']     = -1
        g_data['g_tol']         = 0.0
        g_data['changed']       = True
        ge_data = g_data.e_count.reset_index()
        ge_data['e_time'] = ge_data.open_time

        kprim_g = self.known_g.loc[self.known_g.primary]
        if kprim_g.empty:
            g_check = g_data
        else:
            sg_data = kprim_g.join(self.e_data.range, on=['open_time','minimum'], rsuffix='_e')
            sg_data.loc[sg_data.range != sg_data.range_e, 'changed'] = True
            sg_data['range'] = sg_data.range_e
            sg_data.drop(columns='range_e', inplace=True)
            sg_data.reset_index(level='e_count', inplace=True)
            g_data = pd.concat([sg_data, g_data])
            g_data.sort_index(inplace=True)
            ge_data = pd.concat([
                self.known_ge.merge(kprim_g.loc[:,[]], how='inner', on=['open_time', 'minimum', 'e_count']),
                ge_data
            ], ignore_index=True)
            g_check = g_data.loc[(g_data.status_id != statuses['OUTDATED']) & (g_data.range >= first_new)]

        em_data = self.e_data.join(self.k_data[['open', 'close']], on='open_time')
        em_data['min_oc'] = em_data[['open', 'close']].min(axis=1)
        em_data['max_oc'] = em_data[['open', 'close']].max(axis=1)
        ex_min_data = em_data.xs(True,  level='minimum', drop_level=False)
        ex_max_data = em_data.xs(False, level='minimum', drop_level=False)
        gr_data = ge_data.join(self.e_data[['price', 'low_tol', 'high_tol']], on=['e_time', 'minimum'])
        gr_min_data = gr_data.loc[gr_data.minimum]
        gr_max_data = gr_data.loc[~gr_data.minimum]
        non_groups = []
        for i in g_check.index:
            gr_time = i[0]
            gr_min  = i[1]
            if not ge_data.loc[(ge_data.e_time == gr_time) & (ge_data.open_time != gr_time)].empty:
                ge_data.drop(ge_data.loc[(ge_data.open_time == gr_time) & (ge_data.minimum == gr_min)].index, inplace=True)
                non_groups.append(i)
                continue

            if gr_min:
                em_data = ex_min_data
                gr_data = gr_min_data
            else:
                em_data = ex_max_data
                gr_data = gr_max_data
            tolerance = em_data.at[i, 'tolerance']
            gr_price  = em_data.at[i, 'price']
            gr_list   = gr_data.loc[gr_data.open_time == gr_time]

            e_check_first = max(gr_list.e_time.max() + TIME_STEP, first_new)
            last_time     = min(g_check.at[i, 'range'], gr_time + TOO_OLD)
            try:
                e_check = em_data.loc[ISL[
                    e_check_first : last_time,
                    (abs(em_data.price - gr_price) <= tolerance * T_FACTOR)
                ], ['price', 'low_tol', 'high_tol']].copy()
            except KeyError:
                e_check = pd.DataFrame()
            if not e_check.empty:
                low_max   = gr_list.low_tol.max()
                high_min  = gr_list.high_tol.min()
                try:
                    e_check = drop_by_minmax(
                        e_check,
                        min_field = 'high_tol',
                        max_field = 'low_tol',
                        fixed_min = high_min,
                        fixed_max = low_max,
                        max_delta = 0.0
                    )
                except:
                    raise Exception("Abnormal group detected: low_tol > high_tol for some elements.")
                if not e_check.empty:
                    price_min = gr_list.price.min()
                    price_max = gr_list.price.max()
                    try:
                        e_check = drop_by_minmax(
                            e_check,
                            min_field = 'price',
                            max_field = 'price',
                            fixed_min = price_min,
                            fixed_max = price_max,
                            max_delta = tolerance * (T_FACTOR + 1.0)
                        )
                    except:
                        raise Exception("Abnormal group detected: prices are too distant.")
                if not e_check.empty:
                    min_price = min(gr_list.price.min(), e_check.price.min())
                    max_price = max(gr_list.price.max(), e_check.price.max())
                    e_count   = g_check.at[i, 'e_count'] + e_check.shape[0]
                    low_tol   = min(max(low_max,  e_check.low_tol.max()),  min(price_min, e_check.price.min()))
                    high_tol  = max(min(high_min, e_check.high_tol.min()), max(price_max, e_check.price.max()))
                    g_data.at[i, 'e_count']       = e_count
                    g_data.at[i, 'rebound']       = -1
                    g_data.at[i, 'pierce_factor'] = 0.0
                    g_data.at[i, 'low_tol']       = low_tol
                    g_data.at[i, 'high_tol']      = high_tol
                    g_data.at[i, 'r_factor']      = self.__eval_roundness(gr_price, low_tol, high_tol)
                    g_data.at[i, 'g_tol']         = (max_price - min_price) / (max_price + min_price) * 2.0
                    g_data.at[i, 'changed']       = True
                    e_check['e_count'] = e_count
                    e_check.drop(columns=['price', 'low_tol', 'high_tol'], inplace=True)
                    e_check.reset_index(inplace=True)
                    e_check.rename(columns={'open_time':'e_time'}, inplace=True)
                    e_check['open_time'] = gr_time
                    ge_data.loc[(ge_data.open_time == gr_time) & (ge_data.minimum == gr_min), 'e_count'] = e_count
                    ge_data = pd.concat([ge_data, e_check], ignore_index=True)
                    #!!! DEBUG: In some rare cases extremum is added to group more then once - investigation needed!
                    test_data = ge_data.loc[
                        (ge_data.open_time == gr_time) &
                        (ge_data.minimum   == gr_min ) &
                        (ge_data.e_count   == e_count),
                        'e_time'
                    ]
                    test_dups = test_data.duplicated()
                    if test_dups.any():
                        raise Exception("Extremum(s) {} added to group {} more then once!".format(
                            test_data.loc[test_dups].to_list(), (gr_time, gr_min, e_count)
                        ))
                    #-----------------------------------------------------------------------------------------------
                    e_check_first = e_check.e_time.max() + TIME_STEP

            if g_data.at[i, 'rebound'] == -1:
                rebound, pierce_factor = get_rebound(i, gr_price, g_data, em_data, e_check_first, last_time)
                if rebound != -1:
                    g_data.at[i, 'rebound']       = rebound
                    g_data.at[i, 'pierce_factor'] = pierce_factor
                    g_data.at[i, 'changed']       = True

        g_data.drop(non_groups, inplace=True)
        g_data.set_index('e_count', append=True, inplace=True)
        g_data.sort_index(inplace=True)
        ge_data.sort_values(['open_time', 'minimum', 'e_count', 'e_time'], ignore_index=True, inplace=True)
        print("done in {:.2f} seconds.".format(datetime.now().timestamp() - t_start))

        if g_data.index.has_duplicates: raise Exception("Duplicate groups detected after primary groups collection.")

        print("   Additional groups processing ...          ", end=' ', flush=True)
        t_start = datetime.now().timestamp()

        # Secondary groups

        sg_data = g_data.loc[g_data.index.get_level_values('e_count').to_numpy() > 1].copy()
        if not sg_data.empty:
            if not self.known_g.empty:
                drop_sec_idx = self.known_g.loc[~self.known_g.primary].copy()
                if not drop_sec_idx.empty:
                    drop_sec_idx.reset_index(level='e_count', inplace=True)
                    drop_sec_idx = drop_sec_idx[['e_count']].groupby(level=['open_time','minimum']).max()
                    drop_sec_idx.e_count += 1
                    drop_sec_idx = drop_sec_idx.set_index('e_count', append=True).index
                    sg_data.drop(drop_sec_idx, errors='ignore', inplace=True)

            e_data = self.e_data[['price', 'low_tol', 'high_tol']]
            sg_data['primary'] = False
            sg_data['changed'] = True
            while not sg_data.empty:
                sg_data.reset_index(level='e_count', inplace=True)
                sg_data['e_count'] -= 1
                sg_data.set_index('e_count', append=True, inplace=True)
                for i in sg_data.index:
                    gr_time = i[0]
                    gr_min  = i[1]
                    gr_cnt  = i[2]
                    if gr_min:
                        em_data = ex_min_data
                    else:
                        em_data = ex_max_data
                    gr_list = ge_data.loc[
                        (ge_data.open_time == gr_time) &
                        (ge_data.minimum   == gr_min) &
                        (ge_data.e_count   == gr_cnt + 1)
                    ].join(e_data, on=['e_time', 'minimum'])
                    sg_data.at[i, 'rebound'], sg_data.at[i, 'pierce_factor'] = get_rebound(
                        i, em_data.at[(gr_time, gr_min), 'price'], sg_data, em_data,
                        gr_list.iat[gr_cnt-1, 3] + TIME_STEP, gr_list.iat[gr_cnt, 3]
                    )
                    gr_list['e_count'] -= 1
                    gr_list   = gr_list.iloc[:gr_cnt]
                    low_tol   = min(gr_list.low_tol.max(), gr_list.price.min())
                    high_tol  = max(gr_list.high_tol.min(), gr_list.price.max())
                    min_price = gr_list.price.min()
                    max_price = gr_list.price.max()
                    sg_data.at[i, 'low_tol']   = low_tol
                    sg_data.at[i, 'high_tol']  = high_tol
                    sg_data.at[i, 'r_factor']  = self.__eval_roundness(gr_price, low_tol, high_tol)
                    sg_data.at[i, 'g_tol']     = (max_price - min_price) / (max_price + min_price) * 2.0
                    ge_data = pd.concat([ge_data, gr_list.iloc[:, :4]], ignore_index=True)
                g_data = pd.concat([g_data, sg_data])
                sg_data = sg_data.loc[sg_data.index.get_level_values('e_count').to_numpy() > 1]
                if not self.known_g.empty:
                    if not drop_sec_idx.empty:
                        sg_data.drop(drop_sec_idx, errors='ignore', inplace=True)

        kprim_g = self.known_g.loc[~self.known_g.primary]
        if not kprim_g.empty:
            g_data = pd.concat([g_data, kprim_g])
            ge_data = pd.concat([
                ge_data,
                self.known_ge.merge(kprim_g.loc[:,[]], how='inner', on=['open_time', 'minimum', 'e_count'])
            ], ignore_index=True)
        g_data.sort_index(inplace=True)
        ge_data.sort_values(['open_time', 'minimum', 'e_count', 'e_time'], ignore_index=True, inplace=True)

        if g_data.index.has_duplicates: raise Exception("Duplicate groups detected after secondary groups addition.")

        # Redefining rebound for groups of two extremums

        gr_data = ge_data.loc[ge_data.e_count == 2]
        for i in g_data.loc[(g_data.index.get_level_values('e_count').to_numpy() == 2) & g_data.changed].index:
            gr_time = i[0]
            gr_min  = i[1]
            if gr_min:
                em_data  = ex_min_data
                e_sign   = -1.0
                e_s_tol  = - g_data.at[i, 'low_tol']
                ne_s_tol = - g_data.at[i, 'high_tol']
            else:
                em_data  = ex_max_data
                e_sign   = 1.0
                e_s_tol  = g_data.at[i, 'high_tol']
                ne_s_tol = g_data.at[i, 'low_tol']
            gr_list = gr_data.loc[(gr_data.open_time == gr_time) & (gr_data.minimum == gr_min)]
            try:
                e_check = em_data.loc[ISL[
                    gr_list.iat[0,3] + TIME_STEP : gr_list.iat[1,3] - TIME_STEP,
                    (e_sign * em_data.price >= ne_s_tol)
                ], :]
            except KeyError:
                e_check = pd.DataFrame()
            if not e_check.empty:
                if e_check.shape[0] == 1:
                    r_time = gr_list.iat[1,3]
                else:
                    r_time = e_check.index[1][0]
                g_data.at[i, 'rebound'] = r_time
                r_price = em_data.at[(r_time, gr_min), 'price']
                if e_sign * r_price > e_s_tol:
                    g_data.at[i, 'pierce_factor'] = abs(r_price / em_data.at[(gr_time, gr_min), 'price'] - 1.0)
                else:
                    g_data.at[i, 'pierce_factor'] = 0.0

        # Redefining range for secondary groups

        sg_data = g_data.loc[~g_data.primary & (g_data.range != g_data.rebound)].copy()
        if not sg_data.loc[sg_data.rebound == -1].empty:
            raise Exception("Abnormal case found: secondary group without rebound.")
        sg_data['range']    = sg_data.rebound
        sg_data['r_weight'] = 0
        sg_data['changed']  = True
        g_data.loc[sg_data.index] = sg_data

        self.g_data  = g_data
        self.ge_data = ge_data
        print("done in {:.2f} seconds.".format(datetime.now().timestamp() - t_start))

    def classify_groups(self, statuses, classes):

        # Scaling function is 2^(-1000 x / x_avg) where:
        # x     - distance between points
        # x_avg - "average" value of x in series, actually 0.5*(min(x)+max(x))
        # So, 0.1% of relative distance results in coefficient 1/2, zero distance results in coefficient 1
        # Product of all coefficients is returned.
        def eval_price_variance(data):
            p = data.sort_values(ignore_index=True).to_frame()
            p_scale = 2000.0 / (p.iat[0, 0] + p.iat[p.shape[0]-1, 0])
            p['price_diff'] = 0.0
            p.iloc[1:, 1] = p.iloc[:-1, 0].array - p.iloc[1:, 0].array
            return p.price_diff.map(lambda p: 2.0 ** (p * p_scale)).product()

        # to avoid negative values of log it is glued to power function by value and derivative at x = 5    
        def log_scale(x):
            if x >= 5.0:
                return log10(x)
            else:
                return 0.2571170303326 * x ** 0.621379355908633
            
        def distance_penalty(x):
            return 1.0 / (1.0 + exp(self.dp_k * (x - self.dp_a)))

        print("   Groups classification ...                 ", end=' ', flush=True)
        t_start = datetime.now().timestamp()

        TIME_STEP   = self.params['TIME_STEP']
        TOO_OLD     = self.params['TOO_OLD_LIMIT'] * TIME_STEP
        ISL = pd.IndexSlice

        if _RECLASSIFY:
            g_data = self.g_data
        else:
            g_data = self.g_data.loc[
                (self.g_data.status_id != statuses['OUTDATED']) &
                ((self.g_data.status_id != statuses['CLOSED']) | self.g_data.changed)
            ]
        g_data = g_data.join(self.e_data[['v_factor', 'tolerance']], on=['open_time', 'minimum'])
        g_data['rank_old']      = g_data['rank']
        g_data['result_old']    = g_data['result']
        g_data['result_id_old'] = g_data['result_id']
        g_data['status_id_old'] = g_data['status_id']

        e_min_data = self.e_data.xs(True, level='minimum').copy()
        e_max_data = self.e_data.xs(False, level='minimum').copy()
        e_counts = g_data.index.get_level_values('e_count').to_numpy()

        # FIRST_TOUCH & SECOND_TOUCH classes
        #---------------------------------------------------------------------

        g_data.loc[e_counts == 1, 'class_id'] = classes['FIRST_TOUCH']
        g_data.loc[e_counts == 2, 'class_id'] = classes['SECOND_TOUCH']
        cl_data = g_data.loc[(e_counts == 1) | (e_counts == 2)].copy()
        if not cl_data.empty:
            # rank is proportional to volume factor (scaled) and price roundness factor
            # rank is inversaly proportional to relative tolerance
            cl_data['rank'] = 3.0 * cl_data.v_factor.map(log_scale) * cl_data.r_factor \
                              / (cl_data.high_tol - cl_data.low_tol) * cl_data.tolerance
            cl_data = cl_data.join(self.e_data.range, on=['open_time','minimum'], rsuffix='_e')

            # Minimums with positive result
            cl_min = cl_data.loc[(cl_data.rebound != -1) & cl_data.index.get_level_values('minimum').to_numpy()].copy()
            if not cl_min.empty:
                cl_min = cl_min.join(e_min_data[['next_high','next_high_closed']], on='rebound')
                cl_min = cl_min.loc[
                    (cl_min.next_high <= cl_min.index.get_level_values('open_time').to_numpy() + TOO_OLD) &
                    (cl_min.next_high <= cl_min.range_e)
                ]
                cl_min = cl_min.join(e_max_data.price, on='next_high')
                cl_min['price']     = cl_min.price.fillna(cl_min.high_tol)
                cl_min['result']    = cl_min.price / cl_min.high_tol - 1.0
                cl_min['result_id'] = cl_min.next_high
                cl_min.drop(columns=['next_high','price'], inplace=True)
                cl_min.rename(columns={'next_high_closed':'next_closed'}, inplace=True)

            # Maximums with positive result
            cl_max = cl_data.loc[(cl_data.rebound != -1) & ~cl_data.index.get_level_values('minimum').to_numpy()].copy()
            if not cl_max.empty:
                cl_max = cl_max.join(e_max_data[['next_low','next_low_closed']], on='rebound')
                cl_max = cl_max.loc[
                    (cl_max.next_low <= (cl_max.index.get_level_values('open_time').to_numpy() + TOO_OLD)) &
                    (cl_max.next_low <= cl_max.range_e)
                ]
                cl_max = cl_max.join(e_min_data.price, on='next_low')
                cl_max['price']     = cl_max.price.fillna(cl_max.low_tol)
                cl_max['result']    = cl_max.price / cl_max.low_tol - 1.0
                cl_max['result_id'] = cl_max.next_low
                cl_max.drop(columns=['next_low','price'], inplace=True)
                cl_max.rename(columns={'next_low_closed':'next_closed'}, inplace=True)

            if not cl_min.empty or not cl_max.empty:
                # if cl_min or cl_max is empty, type of next_closed will be corrupted after concat: astype is needed
                sg_data = pd.concat([cl_min, cl_max]).astype({'next_closed':'bool'})
                sg_data.loc[sg_data.next_closed,  'status_id'] = statuses['CLOSED']
                sg_data.loc[~sg_data.next_closed, 'status_id'] = statuses['OPEN']
                sg_data.drop(columns='next_closed', inplace=True)
                cl_data.loc[sg_data.index] = sg_data

            # Negative result
            sg_data = cl_data.loc[cl_data.result_id == -1].copy()
            if not sg_data.empty:
                sg_data.loc[sg_data.range == _MAXINT, 'status_id'] = statuses['OPEN']
                sg_data.loc[sg_data.range != _MAXINT, 'status_id'] = statuses['CLOSED']
                cl_data.loc[sg_data.index] = sg_data

            cl_data.drop(columns='range_e', inplace=True)
            g_data.loc[cl_data.index] = cl_data

        # PIERCE_READY class
        #---------------------------------------------------------------------

        # preparing necessary data
        e_min_data['minimum'] = True
        e_max_data['minimum'] = False
        e_min_data.set_index('minimum', append=True, inplace=True)
        e_max_data.set_index('minimum', append=True, inplace=True)
        # selecting pierce-ready cases
        cl_data = g_data.loc[e_counts >= 3].copy()
        if not cl_data.empty:
            cl_data['class_id'] = classes['PIERCE_READY']
            # cycling each group to classify separately
            for gr in cl_data.itertuples():
                # initiating local variables
                gr_time = gr.Index[0]
                gr_min  = gr.Index[1]
                if gr_min:
                    e_sign    = -1.0
                    e_tol     = gr.low_tol
                    nxt_field = 'next_low'
                    em_data   = e_min_data
                    nem_data  = e_max_data
                else:
                    e_sign    = 1.0
                    e_tol     = gr.high_tol
                    nxt_field = 'next_high'
                    em_data   = e_max_data
                    nem_data  = e_min_data
                # setting limit to search by too-old criteria
                last_time = gr_time + TOO_OLD

                # selecting group extremums
                gr_list = self.ge_data.loc[
                    (self.ge_data.open_time == gr_time) &
                    (self.ge_data.minimum   == gr_min) &
                    (self.ge_data.e_count   == gr.Index[2]),
                    ['e_time', 'minimum']
                ].join(em_data[['v_factor','price']], on=['e_time','minimum'])

                # rank is proportional to price roundness factor
                # rank is proportional weighted sum of extremums v_factors (scaled)
                # with weights proportional to distance between extremums (scaled)
                # rank is inversaly proportional to exponential function of relative price variation (see above)

                # distance between extremums scaled so that 48 klines (2 hours in 5 min interval) results in wieght 1
                # first extremum goes with weight 1
                gr_list['d_factor']  = 0.0
                gr_list.iloc[1:, gr_list.shape[1]-1] = (gr_list.iloc[1:, 0].array - gr_list.iloc[:-1, 0].array) / TIME_STEP
                gr_list['d_factor']  = gr_list.d_factor.map(distance_penalty)
                gr_list['v_factor']  = gr_list.v_factor.map(log_scale)
                # first extremum goes with 1.0 weight
                gr_list['vd_factor'] = gr_list.v_factor.iat[0]
                # other extremums are weighted according to their v_factor value: smallest v_factor goes with smallest weight etc.
                gr_list.iloc[1:, gr_list.shape[1]-1] = gr_list.v_factor.iloc[1:].sort_values().array * gr_list.d_factor.iloc[1:].sort_values().array

# =============================================================================
#                 # DEBUG
#                 if gr.Index == (1705587300000,False,3):
#                     print("\n\nr_factor = ", gr.r_factor)
#                     for e in gr_list.itertuples():
#                         print("v_factor(e) = {}".format(e.v_factor))
#                         print("d_factor(e) = {}".format(e.d_factor))
#                         print("vd_factor(e) =", e.vd_factor)
#                     print("vd_factor_total =", gr_list.vd_factor.sum())
#                     for e in gr_list.sort_values('price').itertuples():
#                         print("e_price =", e.price)
#                     print("p_factor =", eval_price_variance(gr_list.price))
#                     input()
# =============================================================================

                cl_data.at[gr.Index, 'rank'] = gr.r_factor * gr_list.vd_factor.sum() * eval_price_variance(gr_list.price)
                # secondary groups are non-resultative and closed by definition
                cl_data.at[gr.Index, 'status_id'] = statuses['CLOSED']
                if not gr.primary: continue
                # searching for result
                # only finite groups can be resultative by definition
                if gr.range != _MAXINT:
                    # searching for opposite extremum inside range with good next price outside range (but not too old)
                    try:
                        ie_data = nem_data.loc[ISL[
                            gr_time + TIME_STEP : gr.range - TIME_STEP,
                            (nem_data[nxt_field] >= gr.range) &
                            (nem_data[nxt_field] <= last_time)
                        ], :]
                    except KeyError:
                        ie_data = pd.DataFrame()
                    if not ie_data.empty:
                        # latest of such extremum represents result via next price movement
                        idx = ie_data.index[-1]
                        result_id = ie_data.at[idx, nxt_field]
                        cl_data.at[gr.Index, 'result_id'] = result_id
                        cl_data.at[gr.Index, 'result']    = em_data.at[(result_id, gr_min), 'price'] / e_tol - 1.0
                        # if next price movement is open, then group is still open too
                        if not ie_data.at[idx, nxt_field + '_closed']:
                            cl_data.at[gr.Index, 'status_id'] = statuses['OPEN']
                # infinite groups are non-resultative and open* by definition
                else: # gr.range == _MAXINT
                    cl_data.at[gr.Index, 'status_id'] = statuses['OPEN']
                    # counting internal pierces of the group
                    try:
                        p_data = em_data.loc[ISL[
                            gr_time + TIME_STEP : min(gr.range, last_time) - TIME_STEP,
                            e_sign * em_data.price > e_sign * e_tol
                        ], :]
                    except KeyError:
                        p_data = pd.DataFrame()
                    # *if there are pierces after last extremum in group, status is unconfirmed
                    if not p_data.empty:
                        if p_data.index[-1][0] >= gr_list.e_time.max():
                            cl_data.at[gr.Index, 'status_id'] = statuses['UNCONFIRMED']

            g_data.loc[cl_data.index] = cl_data

        #---------------------------------------------------------------------

        g_times = g_data.index.get_level_values('open_time').to_numpy()
        g_data.loc[
            g_data.status_id.isin([statuses['OPEN'], statuses['UNCONFIRMED']]) &
            (self.k_data.index[-1] - g_times > TOO_OLD),
        'status_id'] = statuses['OUTDATED']

        g_data.loc[
            (g_data.rank_old      != g_data['rank'])   |
            (g_data.result_old    != g_data.result)    |
            (g_data.result_id_old != g_data.result_id) |
            (g_data.status_id_old != g_data.status_id),
            'changed'
        ] = True
        g_data.drop(columns=['rank_old', 'result_old', 'result_id_old', 'status_id_old'], inplace=True)

        self.g_data.loc[g_data.index] = g_data
        print("done in {:.2f} seconds.".format(datetime.now().timestamp() - t_start))

    def process_data(self, statuses, classes):
        t_start = datetime.now().timestamp()
        if _RECLASSIFY:
            self.e_data  = self.known_e.copy()
            self.g_data  = self.known_g.copy()
            self.ge_data = self.known_ge.copy()
        else:
            self.find_extremums()
            self.collect_groups(statuses)
        self.classify_groups(statuses, classes)
        print("   -------\n   Total time spent...                        {:.1f} seconds".format(
            datetime.now().timestamp() - t_start))
        print("   New extremums / groups found...            {}/{}".format(
            self.e_data.shape[0] - self.known_e.shape[0],
            self.g_data.shape[0] - self.known_g.shape[0]
        ))

    def trim_data(self, start_time):
        if start_time > self.k_data.index[0]:
            ISL = pd.IndexSlice
            self.k_data  = self.k_data.truncate(before = start_time)
            self.e_data  = self.e_data.loc[ISL[start_time:], :]
            self.g_data  = self.g_data.loc[ISL[start_time:], :]
            self.ge_data = self.ge_data.loc[self.ge_data.open_time >= start_time]

    def reset_known(self):
        self.known_e  = self.e_data.copy()
        self.known_g  = self.g_data.copy()
        self.known_ge = self.ge_data.copy()
        
    def __eval_roundness(self, num, low, high, mult=4):
    
        m_num  = num  * mult
        m_low  = low  * mult
        m_high = high * mult
        
        order = floor(log10(m_num))
        level = 2
        while level > -2:
            appr = round(m_num, -order)
            if appr >= m_low and appr <= m_high:
                break
            order -= 1
            level -= 1
            
        return 2 ** level
