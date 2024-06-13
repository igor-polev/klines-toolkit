#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""""
Project: Klines Toolkit
File:    KGraph class

Created on Thu Oct 26 18:28:08 2023

@author: IGOR POLEV
"""

#---------------------------------------------------------------------
# Graphic output elements:
#   VOLUMES
#   GROUP_RANGES
#   EXTREMUM_RANGES
#   LAST_EXTREMUM
#   LAST_NON_EXTREMUM
#   EX_HEIGHT
#   DERIVATIVE
#   GROUP_TOLERANCES
#   GROUP_RANKS
#   EXTREMUMS
#   GROUP_RESULTS
#   NEXT_PRICES
#   GROUP_EXTREMUMS
#   REBOUNDS
#   KLINES
#---------------------------------------------------------------------

from sys  import maxsize as _MAXINT
from math import floor, log10

import pandas as pd
import plotly.graph_objects as go

from kt_binancedb import BinanceDB

from kt_params import _GROUP_SHOW_DELTA_HOURS

class KGraph:

    def __init__(self, classes, statuses, pic_size=(2850,1180), scale=5, renderer='browser'):

        self.__empty_df = pd.DataFrame()     
        self.verbosity  = 1

        self.classes  = classes
        self.statuses = statuses

        self.width    = pic_size[0]
        self.height   = pic_size[1]
        self.scale    = scale
        self.renderer = renderer
        
        self.symbol   = None
        self.k_data   = self.__empty_df
        self.e_data   = self.__empty_df
        self.g_data   = self.__empty_df
        self.ge_data  = self.__empty_df
        self.GT_DELTA = _GROUP_SHOW_DELTA_HOURS * 3600000
        
    def set_data(self, symbol=None, k_data=pd.DataFrame(), e_data=pd.DataFrame(), g_data=pd.DataFrame(), ge_data=pd.DataFrame()):
        if symbol: self.symbol = symbol
        if not k_data.empty:
            self.k_data = k_data.copy()
            self.k_data.reset_index(inplace=True)
            self.k_data.set_index(pd.to_datetime(self.k_data.open_time, unit='ms'), inplace=True)
            self.k_data.rename(columns={'open_time':'server_open_time'}, inplace=True)
        if not e_data.empty:
            self.e_data = e_data.copy()
            self.e_data.reset_index(inplace=True)
            self.e_data.set_index([
                pd.to_datetime(self.e_data.open_time, unit='ms'),
                self.e_data.minimum
            ], inplace=True)
            self.e_data.rename(columns={'open_time':'server_open_time'}, inplace=True)
            self.e_data.drop(columns='minimum', inplace=True)
        if not g_data.empty:
            self.g_data = g_data.copy()
            self.g_data.reset_index(inplace=True)
            self.g_data.set_index([
                pd.to_datetime(self.g_data.open_time, unit='ms'),
                self.g_data.minimum,
                self.g_data.e_count
            ], inplace=True)
            self.g_data.rename(columns={'open_time':'server_open_time'}, inplace=True)
            self.g_data.drop(columns=['minimum', 'e_count'], inplace=True)
        if not ge_data.empty:
            self.ge_data = ge_data.copy()
            self.ge_data.rename(columns={
                'open_time' : 'server_open_time',
                'e_time'    : 'server_e_time'
            }, inplace=True)
            self.ge_data['open_time'] = pd.to_datetime(self.ge_data.server_open_time, unit='ms')
            self.ge_data['e_time']    = pd.to_datetime(self.ge_data.server_e_time,    unit='ms')

    # parameter stop = 'group' | 'range' | 'data'
    async def display_group(self, idx, stop='group', how='image', name=None):
        
        if self.k_data.empty or self.e_data.empty or self.g_data.empty or self.ge_data.empty:
            if self.verbosity >= 2:
                print("\n--- Not enough data to display.\n")
            return
        
        ISL = pd.IndexSlice
        
        k_copy = self.k_data
        g_copy = self.g_data
        dt = pd.to_datetime(idx[0], unit='ms')
        if stop == 'data':
            end_time = k_copy.at[k_copy.index[-1], 'server_open_time']
        elif stop == 'range':
            end_time = g_copy.at[(dt, idx[1], idx[2]), 'range']
            if end_time == _MAXINT:
                end_time = k_copy.at[k_copy.index[-1], 'server_open_time']
            else:
                end_time += self.GT_DELTA
        else: # including default stop == 'group':
            end_time = self.ge_data.loc[
                (self.ge_data.server_open_time == idx[0]) &
                (self.ge_data.minimum          == idx[1]) &
                (self.ge_data.e_count          == idx[2]),
                'server_e_time'
            ].iat[idx[2] - 1] + self.GT_DELTA
        self.k_data = k_copy.loc[pd.to_datetime(idx[0] - self.GT_DELTA, unit='ms') : pd.to_datetime(end_time, unit='ms')]
        self.g_data = g_copy.loc[ISL[dt:dt, idx[1]:idx[1], idx[2]:idx[2]], :]

        result = await self.display_data([
            'VOLUMES',
            'KLINES',
            'GROUP_RANGES',
            'GROUP_EXTREMUMS',
            'GROUP_RANKS'
        ], how, name, "{} R={:.2f}".format(self.symbol, g_copy.at[(dt, idx[1], idx[2]), 'rank']), False)
        
        self.k_data = k_copy
        self.g_data = g_copy
        return result
   
    async def display_data(self, elements, how='screen', name=None, title=None, expand_k=True):
    
        if self.k_data.empty:
            if self.verbosity >= 2:
                print("\n--- No klines data to display.\n")
            return

        last_time = self.k_data.at[self.k_data.index[-1], 'server_open_time']
        if expand_k:
            first_time = self.k_data.at[self.k_data.index[0], 'server_open_time']
            if not self.g_data.empty:
# =============================================================================
#                 last_time = max(
#                     last_time,
#                     self.g_data.loc[
#                         (self.g_data.result_id != -1) &
#                         (self.g_data.class_id == self.classes['PIERCE_READY']),
#                         'range'
#                     ].max()
#                 )
# =============================================================================
                if 'REBOUNDS' in elements:
                    last_time = max(last_time, self.g_data.rebound.max())
                if 'GROUP_RESULTS' in elements:
                    last_time = max(last_time, self.g_data.result_id.max())
            if not self.e_data.empty:
                if 'NEXT_PRICES' in elements:
                    last_time = max(
                        last_time,
                        self.e_data[['next_low', 'next_high']].max().max()
                    )
                if 'LAST_EXTREMUM' in elements:
                    first_time = min(
                        first_time,
                        self.e_data.loc[self.e_data.last_ex != 0, 'last_ex'].min()
                    )
                if 'LAST_NON_EXTREMUM' in elements:
                    first_time = min(
                        first_time,
                        self.e_data.loc[self.e_data.last_ex != 0, 'last_non_ex'].min()
                    )
            if last_time  > self.k_data.at[self.k_data.index[-1], 'server_open_time'] or \
               first_time < self.k_data.at[self.k_data.index[0], 'server_open_time']:
                db = BinanceDB([self.symbol])
                self.set_data(k_data = await db.load_data(
                    db.symbol_names[self.symbol],
                    first_time,
                    last_time,
                    klines_only=True
                ))

        # Y-axis range calculation
        a = (self.k_data.high.max() + self.k_data.low.min()) / 2.0
        m_order = floor(log10(a))
        y_mid = round(a / 10.0 ** m_order) * 10.0 ** m_order

        if title:
            gr_title = title
        else:
            gr_title = self.symbol
        fig = go.Figure()
        fig.update_layout(
            title_text      = gr_title,
            title_font_size = 40,
            title_y         = 0.95,
            width           = self.width,
            height          = self.height,
            margin          = dict(l=20, r=20, t=20, b=20),
            paper_bgcolor   = 'DarkGrey',
            showlegend      = False,
            dragmode        = 'pan',
            yaxis1          = dict(
                title_text      = 'Volume',
                title_font_size = 20,
                tickfont_size   = 20,
                side            = 'left',
                autorange       = True,
                fixedrange      = True,
                showgrid        = False
            ),
            yaxis2          = dict(
                title_text      = 'Price',
                title_font_size = 20,
                tickfont_size   = 20,
                side            = 'right',
                overlaying      = 'y',
                autorange       = True,
                fixedrange      = False,
                tick0           = y_mid,
                dtick           = y_mid * 0.01
            ),
            xaxis_tickfont_size       = 20,
            xaxis_rangeslider_visible = False
        )
        
        # Volumes
        if 'VOLUMES' in elements:
            fig.add_bar(
                x            = self.k_data.index,
                y            = self.k_data.volume,
                name         = 'Volume',
                marker_color = "Purple",
                opacity      = 0.5
            )
    
        # Ranges of groups
        if 'GROUP_RANGES' in elements:
            if self.g_data.empty:
                if self.verbosity >= 2:
                    print("\n--- No groups data to display.\n")
                return
            g_data = self.g_data.copy()
            g_data.loc[g_data.range > last_time, 'range'] = last_time # cutting down range for output
            for gr in g_data.itertuples():
                dt = pd.to_datetime(gr.range, unit='ms')
                fig.add_scatter(
                    x         = [gr.Index[0], gr.Index[0], dt,          dt,         gr.Index[0]],
                    y         = [gr.low_tol,  gr.high_tol, gr.high_tol, gr.low_tol, gr.low_tol],
                    mode      = 'lines',
                    fill      = 'toself',
                    fillcolor = 'DarkGrey',
                    opacity   = 0.3,
                    yaxis     = 'y2'
                )
    
        # Ranges of extremums
        if 'EXTREMUM_RANGES' in elements:
            if self.e_data.empty:
                if self.verbosity >= 2:
                    print("\n--- No extremums data to display.\n")
                return
            e_data = self.e_data.copy()
            e_data.loc[e_data.range > last_time, 'range'] = last_time # cutting down range for output
            for ex in e_data.itertuples():
                dt = pd.to_datetime(ex.range, unit='ms')
                fig.add_scatter(
                    x         = [ex.Index[0], ex.Index[0], dt,          dt,         ex.Index[0]],
                    y         = [ex.low_tol,  ex.high_tol, ex.high_tol, ex.low_tol, ex.low_tol],
                    mode      = 'lines',
                    fill      = 'toself',
                    fillcolor = 'DarkGrey',
                    opacity   = 0.3,
                    yaxis     = 'y2'
                )

        #---------------------------------------------------------------------------------------------------
            
        #
        if 'LAST_EXTREMUM' in elements:
            if self.e_data.empty:
                if self.verbosity >= 2:
                    print("\n--- No extremums data to display.\n")
                return
            e_data = self.e_data.loc[self.e_data.last_ex != 0]
            for ex in e_data.itertuples():
                if ex.Index[1]:
                    e_field = 'low'
                else:
                    e_field = 'high'
                dt = pd.to_datetime(ex.last_ex, unit='ms')
                fig.add_scatter(
                    x         = [ex.Index[0], dt],
                    y         = [ex.price,    self.k_data.at[dt, e_field]],
                    mode      = 'lines',
                    yaxis     = 'y2'
                )
    
        #
        if 'LAST_NON_EXTREMUM' in elements:
            if self.e_data.empty:
                if self.verbosity >= 2:
                    print("\n--- No extremums data to display.\n")
                return
            e_data = self.e_data.loc[self.e_data.last_non_ex != 0]
            for ex in e_data.itertuples():
                if ex.Index[1]:
                    e_field = 'high'
                else:
                    e_field = 'low'
                dt = pd.to_datetime(ex.last_non_ex, unit='ms')
                fig.add_scatter(
                    x         = [ex.Index[0], dt],
                    y         = [ex.price,    self.k_data.at[dt, e_field]],
                    mode      = 'lines',
                    yaxis     = 'y2'
                )
    
        #
        if 'EX_HEIGHT' in elements:
            if self.e_data.empty:
                if self.verbosity >= 2:
                    print("\n--- No extremums data to display.\n")
                return
            e_data = self.e_data.loc[self.e_data.height != 0.0]
            for ex in e_data.itertuples():
                if ex.Index[1]:
                    sign = 1.0
                else:
                    sign = -1.0
                fig.add_scatter(
                    x         = [ex.Index[0], ex.Index[0]],
                    y         = [ex.price,    ex.price + sign * ex.height],
                    mode      = 'lines',
                    yaxis     = 'y2'
                )

        #
        if 'DERIVATIVE' in elements:
            if self.e_data.empty:
                if self.verbosity >= 2:
                    print("\n--- No extremums data to display.\n")
                return
            e_data = self.e_data.loc[self.e_data.derivative != 0.0]
            fig.add_scatter(
                x           = e_data.index.get_level_values('open_time'),
                y           = e_data.price,
                marker_size = e_data.derivative * self.scale * 100.0,
                opacity     = 0.7,
                mode        = 'markers',
                yaxis       = 'y2'
            )
    
        #
        if 'GROUP_TOLERANCES' in elements:
            if self.g_data.empty:
                if self.verbosity >= 2:
                    print("\n--- No groups data to display.\n")
                return
            g_data = self.g_data.loc[self.g_data.g_tol != 0.0].join(self.e_data.price, on=['open_time', 'minimum'])
            fig.add_scatter(
                x           = g_data.index.get_level_values('open_time'),
                y           = g_data.price,
                marker_size = g_data.g_tol * self.scale * 1000,
                opacity     = 0.7,
                mode        = 'markers',
                yaxis       = 'y2'
            )
    
        #---------------------------------------------------------------------------------------------------
    
        # Ranks of groups
        if 'GROUP_RANKS' in elements:
            if self.g_data.empty:
                if self.verbosity >= 2:
                    print("\n--- No groups data to display.\n")
                return
            g_data = self.g_data.join(self.e_data.price, on=['open_time', 'minimum'])
            g_data = g_data.loc[self.g_data['rank'] >= 0.0]
            fig.add_scatter(
                x           = g_data.index.get_level_values('open_time'),
                y           = g_data.price,
                marker_size = g_data['rank'] * self.scale,
                opacity     = 0.7,
                mode        = 'markers',
                yaxis       = 'y2'
            )
            g_data = g_data.loc[self.g_data['rank'] < 0.0]
            fig.add_scatter(
                x             = g_data.index.get_level_values('open_time'),
                y             = g_data.price,
                marker_size   = g_data['rank'] * -self.scale,
                opacity       = 0.7,
                mode          = 'markers',
                marker_symbol = 'diamond',
                marker_color  = 'red',
                yaxis         = 'y2'
            )
    
        # Local extremums
        if 'EXTREMUMS' in elements:
            if self.e_data.empty:
                print("\n--- No extremums data to display.\n")
                return
            fig.add_scatter(
                x           = self.e_data.index.get_level_values('open_time'),
                y           = self.e_data.price,
                mode        = 'markers',
                marker_size = self.e_data.v_factor * self.scale,
                yaxis       = 'y2'
            )
    
        # Results of groups
        if 'GROUP_RESULTS' in elements:
            if self.g_data.empty:
                if self.verbosity >= 2:
                    print("\n--- No groups data to display.\n")
                return
            g_data = self.g_data.loc[
                (self.g_data.result_id != -1) &
                self.g_data.index.get_level_values('minimum') &
                self.g_data.class_id.isin([self.classes['FIRST_TOUCH'], self.classes['SECOND_TOUCH']])
            ]
            for gr in g_data.itertuples():
                fig.add_scatter(
                    x     = [pd.to_datetime(gr.rebound, unit='ms'),  pd.to_datetime(gr.result_id, unit='ms')],
                    y     = [gr.high_tol,                            gr.high_tol * (1.0 + gr.result)],
                    mode  = 'lines',
                    yaxis = 'y2'
                )
            g_data = self.g_data.loc[
                (self.g_data.result_id != -1) &
                ~self.g_data.index.get_level_values('minimum') &
                self.g_data.class_id.isin([self.classes['FIRST_TOUCH'], self.classes['SECOND_TOUCH']])
            ]
            for gr in g_data.itertuples():
                fig.add_scatter(
                    x     = [pd.to_datetime(gr.rebound, unit='ms'),  pd.to_datetime(gr.result_id, unit='ms')],
                    y     = [gr.low_tol,                             gr.low_tol * (1.0 + gr.result)],
                    mode  = 'lines',
                    yaxis = 'y2'
                )
            g_data = self.g_data.loc[
                (self.g_data.result_id != -1) &
                self.g_data.index.get_level_values('minimum') &
                (self.g_data.class_id == self.classes['PIERCE_READY'])
            ]
            for gr in g_data.itertuples():
                fig.add_scatter(
                    x     = [pd.to_datetime(gr.range, unit='ms'),  pd.to_datetime(gr.result_id, unit='ms')],
                    y     = [gr.low_tol,                           gr.low_tol * (1.0 + gr.result)],
                    mode  = 'lines',
                    yaxis = 'y2'
                )
            g_data = self.g_data.loc[
                (self.g_data.result_id != -1) &
                ~self.g_data.index.get_level_values('minimum') &
                (self.g_data.class_id == self.classes['PIERCE_READY'])
            ]
            for gr in g_data.itertuples():
                fig.add_scatter(
                    x     = [pd.to_datetime(gr.range, unit='ms'),  pd.to_datetime(gr.result_id, unit='ms')],
                    y     = [gr.high_tol,                          gr.high_tol * (1.0 + gr.result)],
                    mode  = 'lines',
                    yaxis = 'y2'
                )
    
        # Next price movement
        if 'NEXT_PRICES' in elements:
            if self.e_data.empty:
                if self.verbosity >= 2:
                    print("\n--- No extremums data to display.\n")
                return
            for ex in self.e_data.itertuples():
                if ex.next_low != -1:
                    dt = pd.to_datetime(ex.next_low, unit='ms')
                    fig.add_scatter(
                        x     = [ex.Index[0], dt],
                        y     = [ex.price,    self.k_data.at[dt, 'low']],
                        mode  = 'lines',
                        yaxis = 'y2'
                    )
                if ex.next_high != -1:
                    dt = pd.to_datetime(ex.next_high, unit='ms')
                    fig.add_scatter(
                        x     = [ex.Index[0], dt],
                        y     = [ex.price,    self.k_data.at[dt, 'high']],
                        mode  = "lines",
                        yaxis = 'y2'
                    )

        # Extremums of groups
        if 'GROUP_EXTREMUMS' in elements:
            if self.g_data.empty:
                if self.verbosity >= 2:
                    print("\n--- No groups data to display.\n")
                return
            last_e_time = self.ge_data.server_e_time.max()
            if last_e_time > self.e_data.server_open_time.max() and expand_k:
                db = BinanceDB([self.symbol])
                await db.setup()
                k_data, e_data, g_data, ge_data = await db.load_data(
                    db.symbol_names[self.symbol],
                    self.k_data.at[self.k_data.index[0], 'server_open_time'],
                    last_e_time
                )
            for i in self.g_data.index:
                gr_list = self.ge_data.loc[
                    (self.ge_data.open_time == i[0]) &
                    (self.ge_data.minimum   == i[1]) &
                    (self.ge_data.e_count   == i[2])
                ].join(self.e_data.price, on=['e_time', 'minimum'], how='inner')
                fig.add_scatter(
                    x             = gr_list.e_time,
                    y             = gr_list.price,
                    mode          = 'markers',
                    marker_symbol = 'x',
                    marker_size   = self.scale,
                    marker_color  = 'black',
                    yaxis         = 'y2'
                )
    
        # First rebounds of groups
        if 'REBOUNDS' in elements:
            if self.g_data.empty:
                if self.verbosity >= 2:
                    print("\n--- No groups data to display.\n")
                return
            g_data = self.g_data.loc[self.g_data.rebound != -1].join(self.e_data.price, on=['open_time', 'minimum'])
            fig.add_scatter(
                x             = pd.to_datetime(g_data.rebound, unit='ms'),
                y             = g_data.price,
                mode          = 'markers',
                marker_symbol = 'x',
                marker_size   = self.scale,
                marker_color  = 'black',
                yaxis         = 'y2'
            )

        # Klines
        if 'KLINES' in elements:
            fig.add_candlestick(
                x                     = self.k_data.index,
                open                  = self.k_data.open,
                high                  = self.k_data.high,
                low                   = self.k_data.low,
                close                 = self.k_data.close,
                name                  = 'OLHC',
                yaxis                 = 'y2',
                line_width            = 1,
                increasing_fillcolor  = "Teal",
                increasing_line_color = "Teal",
                decreasing_fillcolor  = "Red",
                decreasing_line_color = "Red"
            )

        if   how == 'screen':
            fig.show(renderer=self.renderer)
        elif how == 'file':
            fig.write_image(name)
        elif how == 'image':
            return fig.to_image(format='png')