#!/bin/bash
echo "start test 2A"

int=1
while (( $int<=50 ))
do
	echo $int
	let "int++"
	go test -run 2A >> /Users/shuxin/go/src/mit/6.824/src/raft/testOut/outTest2A
done
