#!/bin/bash
# This script exports all blocks and transactions from 1. Jan 2010 to 1. Jan 2023 using rusty-blockparser (rust program)
# export PATH="$HOME/.cargo/bin:$PATH"  # sets path variable for the cargo bin folder where rusty-blockparser is located
rusty-blockparser --start 32490 --end 769788 -d ~/raw-data/.bitcoin/blocks -t 16 balances ~/processed-data/rusty-dump  # creates balances.csv -> address ; balance
rusty-blockparser --start 32490 --end 769788 -d ~/raw-data/.bitcoin/blocks -t 16 unspentcsvdump ~/processed-data/rusty-dump  # creates unspent.csv -> txid ; indexOut ; height ; value ; address
rusty-blockparser --start 32490 --end 769788 -d ~/raw-data/.bitcoin/blocks -t 16 csvdump ~/processed-data/rusty-dump # creates big csvs with all data -> blocks.csv, transactions.csv, tx_in.csv, tx_out.csv
rusty-blockparser --start 32490 --end 769788 -d ~/raw-data/.bitcoin/blocks -t 16 opreturn ~/processed-data/rusty-dump
