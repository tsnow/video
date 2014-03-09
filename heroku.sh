#!/bin/bash

git remote -v heroku 2>/dev/null || git add remote heroku git@heroku.com:tm-video-uploader.git  
if [[ "$1" == "" ]]; then
  git push heroku master:master;
else
  git push heroku $1:master;
fi

if [[ "$APP_URL" -ne "" ]]; then
  open "$APP_URL";
fi
