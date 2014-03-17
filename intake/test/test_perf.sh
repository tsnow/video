#!/bin/bash

 bundle exec ruby intake/intake.rb 1>/dev/null & 

sleep 2;

jq -n '{ collection : { items : [ "things" ] } }' > testfile;

for i in 10 20 30 40 50 60 70 80 90 100 110 120 130 140 150 160 170 180 190 200; do

    ab -n 1000 -c $i -p testfile -e outcome-$i.csv 'http://127.0.0.1:9000/?pim_id=1000'; 

done;


ls outcome-* | sed -e 's,outcome-,,' -e 's,.csv,,' | sort -n | while read i; do
    sed "s;.*,\(.*\);$i,\1;" outcome-$i.csv; 
done > outcome.csv;

kill %1 %2 %3;

open outcome.csv -a numbers
echo "Chart that in a scatterplot, with a trendline"
