#!/bin/bash

YEAR=$1 
MONTH=$2
FORMAT=$3

mkdir reddit
cd reddit

case ${FORMAT} in
	"bz2") 
		wget -A bz2 -c -r -l 1 -nd https://files.pushshift.io/reddit/comments/RC_${YEAR}-${MONTH}.bz2
		bzip2 -d RC_${YEAR}-${MONTH}.bz2 ;;
	"xz") 
		wget -A xz -c -r -l 1 -nd https://files.pushshift.io/reddit/comments/RC_${YEAR}-${MONTH}.xz
		xz -d RC_${YEAR}-${MONTH}.xz ;;
	"zst") 
		wget -A zst -c -r -l 1 -nd https://files.pushshift.io/reddit/comments/RC_${YEAR}-${MONTH}.zst
		zstd -d --rm RC_${YEAR}-${MONTH}.zst ;;
	*) echo "Extension invalid. Please use one of the following extension: bz2, xz, zst." ;;
esac

aws s3 sync . s3://reddit-tc

cd ..
rm -rf reddit
