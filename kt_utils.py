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
import uuid
import glob
from datetime   import datetime
from contextlib import closing

async def file_lock(file_name, retry=20, wait_sec=1, raise_on_failure=True):
    
    req_file_pat = file_name + "_*.req"
    req_file     = "{}_{}.req".format(file_name, str(uuid.uuid4()))
    open(req_file, 'x').close()
    c_time     = os.stat(req_file).st_ctime
    retry_left = retry
    while retry_left:
        min_time = c_time
        for r in glob.glob(req_file_pat):
            try:
                other_time = os.stat(r).st_ctime
            except FileNotFoundError:
                continue
            if other_time < c_time:
                min_time = other_time
                break
        if min_time < c_time:
            retry_left -= 1
            if retry_left:
                await asyncio.sleep(wait_sec)
        else:
            break
    if not retry_left:
        if os.path.exists(req_file):
            os.remove(req_file)
        if raise_on_failure:
            raise Exception("Locking {} failed.".format(file_name))
        else:
            return
    
    lock_file  = file_name + '.lock'
    retry_left = retry
    while os.path.exists(lock_file) and retry_left:
        retry_left -= 1
        if retry_left:
            await asyncio.sleep(wait_sec)
    if retry_left:
        open(lock_file, 'x').close()
        if os.path.exists(req_file):
            os.remove(req_file)
    else:
        if os.path.exists(req_file):
            os.remove(req_file)
        if raise_on_failure:
            raise Exception("Locking {} failed.".format(file_name))

def file_unlock(file_name):
    lock = file_name + '.lock'
    if os.path.exists(lock):
        os.remove(lock)
        
async def log_write(file, msg):
    await file_lock(file)
    with closing(open(file, 'a')) as log:
        log.write("{}\t{}\n".format(datetime.now().strftime("%y-%m-%d %H:%M:%S"), msg))
    file_unlock(file)
