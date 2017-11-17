#!/bin/bash
MAIN="`pwd`/paxos-kvdb.lua"
echo $MAIN
export bootstrap=$MAIN
export serverid=$1
../silly/silly paxos.conf

