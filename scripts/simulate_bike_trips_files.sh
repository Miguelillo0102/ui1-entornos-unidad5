#!/bin/bash
#Where the dataset file is
FILE_TRIPS=$1
#Where the destination folder for the files to be written in
DESTINATION_FOLDER=$2
#How many lines from the dataset file should be read
LINES_PER_DF=$3
#How many seconds should be waited before the next batch is generated
RATE=$4
total_lines=$(bzcat $FILE_TRIPS | wc -l )
total_lines=$(echo "$total_lines-1" | bc)
header=$(bzcat $FILE_TRIPS | head -n 1)
lines_read=0
number_of_files=0
rm -rf $DESTINATION_FOLDER
mkdir -p $DESTINATION_FOLDER
while [ $lines_read -lt $total_lines ]; do
	produced_lines=$(echo "$lines_read+2" | bc)
	echo "Creating $DESTINATION_FOLDER/$number_of_files.tsv"
	echo $header > $FILE_TRIPS.aux
	bzcat $FILE_TRIPS | tail -n +$produced_lines  | head -n $LINES_PER_DF >> $FILE_TRIPS.aux
       	mv $FILE_TRIPS.aux $DESTINATION_FOLDER/$number_of_files.tsv	
	echo "Created $DESTINATION_FOLDER/$number_of_files.tsv"
	number_of_files=$(echo "$number_of_files+1" | bc)
	lines_read=$(echo "$lines_read+$LINES_PER_DF" | bc)
	sleep $RATE
done
