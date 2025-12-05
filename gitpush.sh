#!/bin/bash

MESSAGE="$1"

if [ -z "$MESSAGE" ]; then
  MESSAGE="Update"
fi

git add .
git commit -m "$MESSAGE"
git push
