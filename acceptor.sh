#!/bin/bash
MAIN="`pwd`/acceptor.lua"
echo $MAIN
export bootstrap=$MAIN
export serverid=$1
../silly/silly paxos.conf

