#!/bin/bash

PORT=9000;
function setup(){
    cat /dev/null > test_output.log;
}
function log(){
    cat > response.json;
    cat response.json | jq '.' >> test_output.log  2>&1 ;
    if [[ "$?" -ne "0" ]]; then
        cat response.json >> test_output.log;
        echo "" >> test_output.log;
    fi
}
function GET(){
    echo '# GET' "$@" >> test_output.log;
    url="$1";
    shift;
    if [[ "$1" == "" ]]; then
        curl -X GET "$url" | log;
    else
        jq -n "$@" | curl -d'@-' -X GET "$url" | log;
    fi
}
function POST(){
    echo '# POST' "$@" >> test_output.log;
    url="$1";
    shift;
    if [[ "$1" == "" ]]; then
        curl -X POST "$url" | log;
    else
        jq -n "$@" | curl -d'@-' -X POST "$url" | log;
    fi
}
setup;
GET localhost:$PORT/s3/raw-impressions/;
GET localhost:$PORT/s3/raw-impressions/?pim_id=1000;
POST localhost:$PORT/s3/raw-impressions/?pim_id=1000;
POST localhost:$PORT/s3/raw-impressions/?pim_id=1000 1.2;
POST localhost:$PORT/s3/raw-impressions/?pim_id=1000 '{ totally: "JSON" }' ;
POST localhost:$PORT/s3/raw-impressions/?pim_id=1000 '{ collection: "JSON" }';
POST localhost:$PORT/s3/raw-impressions/?pim_id=1000 '{ collection: { items: "JSON" } }';
POST localhost:$PORT/s3/raw-impressions/?pim_id=1000 '{ collection: { items: ["JSON"] } }';
POST localhost:$PORT/s3/raw-impressions/?pim_id=1000 '{ collection: { items: {} } }';
