#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project: Klines Toolkit
File:    service functions

Created on Mon Oct 16 11:14:48 2023

@author: IGOR POLEV
"""

from kt_params import _DEBUG_OUTPUT

def df_print(dataframe, name='<noname>', lines=10, debugOnly=True):
    if debugOnly and not _DEBUG_OUTPUT: return
    print("\n---", name, "---\n")
    dataframe.info()
    print('')
    print(dataframe.head(lines))
    print("\n-----------------\n")