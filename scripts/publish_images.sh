#!/bin/bash

images_file=$1
image_version=${CONNECTOR_IMAGE_VERSION}
if [ -z ${image_version} ]
  then image_version="latest"
fi

if [ -f $images_file ]; then
  for image in $(cat $images_file); do
    echo "publishing image: ${image}:${image_version}"
    docker push ${image}:${image_version};
    echo "published image: ${image}:${image_version}"
  done
fi