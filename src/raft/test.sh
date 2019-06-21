#!/bin/bash


echo "start test $1"

touch testOut/$1
echo "touch file $1"

int=1
while (( $int<=20 ))
do
	let "int++"
	go test -run $1 >> /Users/shuxin/go/src/mit/6.824/src/raft/testOut/$1
	echo "$int done"
done

echo grep -o "PASS" /Users/shuxin/go/src/mit/6.824/src/raft/testOut/$1 | wc -l

