#!/bin/bash

cd Downloads
mkdir reddit_raw
cd reddit_raw
wget -A xz -c -r -l 1 -nd https://files.pushshift.io/reddit/comments/

INS="`seq 2017 2018`" 

for f in ${INS}; do
	mkdir ${f}
	xz -d RC_${f}-*.xz
	mv RC_${f}-* ${f}
	aws s3 sync ${f} s3://reddit-tc
	rm -rf ${f}
done
