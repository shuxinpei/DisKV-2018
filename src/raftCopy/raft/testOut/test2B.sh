#!/bin/bash
echo "start test 2B"

int=1
touch /testOut/TestFailAgree2B
while (( $int<=50 ))
do
	echo $int
	let "int++"
	go test -run TestFailAgree2B >> /testOut/TestFailAgree2B
done
