#!/bin/sh

cd ../..

# build
make release

cd build/release/examples/process-agg/

# run
./process-agg benchmark time all 3 time.out
#./process-agg benchmark stats all 1 stats.out
#./process-agg benchmark groups all 3 groups.out
#./process-agg benchmark groups opt 3 groups.out
#./process-agg benchmark select all 3 select.out
#./process-agg benchmark select opt 3 select.out
#./process-agg benchmark read all 3 read.out
