#!/bin/bash

DIR_BASE=../../../../../../src/data/resources/basiccompany
if [ $# -gt 0 ]; then
  DIR_BASE=$1
fi

function download() {
	for (( YEAR=$1; YEAR<=$2; YEAR++ )); do
		for (( MONTH=$3; MONTH<=$4; MONTH++ )); do
			for (( PART=1; PART<=$5; PART++ )); do
				FILE="BasicCompanyData-"$YEAR"-"$(printf '%02d' $MONTH)"-01-part"$PART"_"$5".zip"
				URI="http://download.companieshouse.gov.uk/"$FILE
				echo "Checking file ["$FILE"] available from ["$URI"]"
				if [ ! -f $FILE ]; then
					if [[ $(date +%Y) -gt $YEAR || ($(date +%Y) -eq $YEAR && $(date +'%-m') -gt $MONTH) ]]; then
						if [ $(curl -sI $URI | grep "HTTP/1.1 200 OK" | wc -l) -gt 0 ]; then
  						wget $URI
						else
						 	echo "ERROR: Could not download ["$URI"]"
						fi
						sleep 2
						echo -n "INFO: Sleeping to prevent getting flagged as a DoS " && sleep 15 && echo "[done]"
					fi
				fi
			done
		done
	done
}

cd $DIR_BASE/warehouse/zip
download 2012 2012 5 12 4
download 2013 2013 1 12 4
