# Klines Toolkit

Tools for collecting and analysing Binance klines (candlestick) data.

## Overview

A set of Python scripts that collect OHLCV klines from the Binance REST API, store them in a local SQLite database, and provide command-line tools for exploring, plotting and analysing the accumulated history. Collection runs as a long-lived Linux service (a systemd unit template is included), so history builds up continuously and unattended.

## Stack

Python 3, NumPy, Pandas, SQLite, plotly, systemd. Helper scripts for Windows are in the win folder.

## Status

A personal research project, published as a working example rather than as a packaged tool. Parameters live in kt_params.py and need to be set for your environment before the first run.
