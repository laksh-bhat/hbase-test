#!/bin/sh

sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
java -cp $(echo lib/*.jar | tr ' ' ':'):bin Write 127.0.0.1:60010 myTable throughput
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
java -cp $(echo lib/*.jar | tr ' ' ':'):bin Read 127.0.0.1:60010 myTable throughput
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
java -cp $(echo lib/*.jar | tr ' ' ':'):bin Write 127.0.0.1:60010 myTable latency
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
java -cp $(echo lib/*.jar | tr ' ' ':'):bin Read 127.0.0.1:60010 myTable latency
