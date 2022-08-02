#!/bin/bash

images_file=$1

if [ -f $images_file ]; then
  for image in $(cat $images_file); do
    docker push $image;
  done
fi