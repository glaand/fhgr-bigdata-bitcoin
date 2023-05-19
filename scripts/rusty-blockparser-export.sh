#!/bin/bash
# This script exports all blocks and transactions
# export PATH="$HOME/.cargo/bin:$PATH"  # sets path variable for the cargo bin folder where rusty-blockparser is located
rusty-blockparser -d mounted-data/raw-data/blocks -t 16 balances mounted-data/processed-data/rusty-dump  # creates balances.csv -> address ; balance
rusty-blockparser -d mounted-data/raw-data/blocks -t 16 unspentcsvdump mounted-data/processed-data/rusty-dump  # creates unspent.csv -> txid ; indexOu>
rusty-blockparser -d mounted-data/raw-data/blocks -t 16 csvdump mounted-data/processed-data/rusty-dump # creates big csvs with all data -> blocks.csv,>
rusty-blockparser -d mounted-data/raw-data/blocks -t 16 opreturn
