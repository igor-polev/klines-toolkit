#!/bin/bash

for (( i=0; i<$KT_EXE_COUNT ; i++ ))
do
    let y=i+1
    gnome-terminal --title "$y/$KT_EXE_COUNT - Klines Toolkit" --tab -- \
    env all_proxy='' nice -n 10 python3 ~/Git/klines-toolkit/kt_script.py -v 3 -t $i
done

