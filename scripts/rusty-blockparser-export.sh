#!/bin/bash
# This script exports all blocks and transactions
# export PATH="$HOME/.cargo/bin:$PATH"  # sets path variable for the cargo bin folder where rusty-blockparser is located
#rusty-blockparser --start 0 --end 790000 -d /raw-data/blocks balances /processed-data/rusty-dump  # creates balances.csv
#rusty-blockparser --start 0 --end 790000 -d /raw-data/blocks unspentcsvdump /processed-data/rusty-dump  # creates unspent.csv
rusty-blockparser --start 0 --end 790000 -d /raw-data/blocks csvdump /processed-data/rusty-dump # creates big csvs with all data
#rusty-blockparser -d /raw-data/blocks opreturn
