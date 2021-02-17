#!/bin/bash

PATH="/bin:/usr/bin:/sbin:/usr/sbin:/usr/local/bin:/usr/local/sbin"
BASENAME="${0##*/}"
echo "$STOCK_DIR"
cd "$STOCK_DIR"
ls *.py
python3.8 "$STOCK_EXEC_SCRIPT" "${@}"
