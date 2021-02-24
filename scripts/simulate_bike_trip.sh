#!/bin/bash
FILE_TRIPS=$1
numberLines=$(wc -l $1)
#First line is header
current=2
next=3
total_lines=$(bzcat $FILE_TRIPS | wc -l)
while true; do
	start_time=$(bzcat $FILE_TRIPS | head -n $current | tail -n 1 | awk -F"\t" '{print $3}' | sed 's/\"//g' )
	next_time=$(bzcat $FILE_TRIPS | head -n $next | tail -n 1 | awk -F"\t" '{print $3}' | sed 's/\"//g' )
	start_seconds=$(date -d "$start_time" "+%s")
	end_seconds=$(date -d "$next_time" "+%s")
	seconds_to_wait=$(echo "$end_seconds-$start_seconds" | bc)
	bzcat $FILE_TRIPS | head -n $current | tail -n 1
	current=$(echo "($current+1)%$total_lines" | bc)
	next=$(echo "($next+1)%$total_lines" | bc)
	if [ $current = "0" ]
	then
		current=2
	fi
	if [ $next = "0" ]
	then
		next=3
	fi
	sleep $seconds_to_wait
done
