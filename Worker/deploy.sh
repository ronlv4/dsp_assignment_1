#!/bin/bash -xe

rm -f Worker.tar
git pull
tar -cf Worker.tar pom.xml src
aws s3 cp Worker.tar s3://dspassignment1/Worker.tar
