#!/bin/bash

cd Downloads
mkdir reddit_raw
cd reddit_raw
wget -A zst -c -r -l 1 -nd https://files.pushshift.io/reddit/comments/

INS="`seq 2018 2019`" 

for f in ${INS}; do
	mkdir ${f}
	zstd -d --rm RC_${f}-*.zst
	mv RC_${f}-* ${f}
	aws s3 sync ${f} s3://reddit-tc
	rm -rf ${f}
done
