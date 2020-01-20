#!/bin/bash

cd Downloads
mkdir reddit_raw
cd reddit_raw
wget -A bz2 -c -r -l 1 -nd https://files.pushshift.io/reddit/comments/

INS="`seq 2005 2017`" 

for f in ${INS}; do
	mkdir ${f}
	bzip2 -d RC_${f}-*.bz2
	mv RC_${f}-* ${f}
	aws s3 sync ${f} s3://reddit-tc
	rm -rf ${f}
done
