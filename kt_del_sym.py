#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project: Klines Toolkit
File:    Symbols removal script

Created on Thu Dec 14 18:00:12 2023
@author: IGOR POLEV
"""

import sys
from contextlib   import closing
from kt_binancedb import BinanceDB

def main_proc(argv):

    print("\n--- Klines Toolkit symbols removal script ---\n")
    print("Removing symbols:", argv)
    if input("\nAre you sure? Type YES if you are: ") != 'YES':
        sys.exit()
    
    db = BinanceDB(argv)
    with closing(db.open()) as db:
        db.read_setup()
        db.delete_symbols(argv)

    print("Done.")

if __name__ == "__main__":
    main_proc(sys.argv[1:])