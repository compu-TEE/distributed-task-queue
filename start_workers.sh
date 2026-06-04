#!/bin/bash

COUNT=${1:-4}
POISON_COUNT=${2:-0}

for ((i=1;i<=COUNT;i++))
do
    (
        cd worker || exit
        go run . worker-$i
    ) &
done

for ((i=1;i<=POISON_COUNT;i++))
do
    (
        cd worker_poison || exit
        go run . poison-worker-$i
    ) &
done

wait