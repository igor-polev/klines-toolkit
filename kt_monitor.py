#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project: Klines Toolkit
File:    Price monitoring script

Created on Sat Apr 27 15:04:33 2024
@author: IGOR POLEV
"""

import asyncio

from kt_utils import log_write

from kt_params import _LOG_UNKNOWN_ERRORS

class KTmonitor:
    
    async def iterate(self):
        pass
    
    async def run(self):
        try:
            while True: await self.iterate()
        except Exception as ex:
            msg = "Thread {} raised {} : {}".format('MONITOR', type(ex), ex)
            print("\n\n!!! Unhandled exception:\n\n{}".format(msg))
            await log_write(_LOG_UNKNOWN_ERRORS, msg)
            if int != type(await self.telegram_message('alert', msg, retry=100)):
                print("\n!!! Telegram notification send.\n\n")
            else:
                print("\n!!! Telegram notification failed.\n\n")
            raise ex

if __name__ == "__main__":
    script = KTmonitor()
    asyncio.run(script.run())
