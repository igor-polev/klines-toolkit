#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project: Klines Toolkit
File:    utility functions

Created on Mon Jan 29 00:15:55 2024

@author: IGOR POLEV
"""

import os
import asyncio
from datetime   import datetime
from contextlib import closing

async def file_lock(file_name, retry=20, wait_sec=1, raise_on_failure=True):
    lock       = file_name + '.lock'
    retry_left = retry
    while os.path.exists(lock) and retry_left:
        retry_left -= 1
        if retry_left: await asyncio.sleep(wait_sec)
    retry_left = retry
    while retry_left:
        try:
            open(lock, 'x').close()
            break
        except FileExistsError:
            retry_left -= 1
            if retry_left: await asyncio.sleep(wait_sec)
    if not retry_left and raise_on_failure:
        raise Exception("Locking {} failured.".format(file_name))

def file_unlock(file_name):
    lock = file_name + '.lock'
    if os.path.exists(lock):
        os.remove(lock)
        
async def log_write(file, msg):
    await file_lock(file)
    with closing(open(file, 'a')) as log:
        log.write("{}\t{}\n".format(datetime.now().strftime("%y-%m-%d %H:%M:%S"), msg))
    file_unlock(file)
