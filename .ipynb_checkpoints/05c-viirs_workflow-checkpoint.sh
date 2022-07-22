#!/bin/bash

# This script is for VIIRS files and:
# 1) downloads a L1A and GEO file for an input download url,
# 2) process the L1A to subscened L2
# 3) removes the L1A, GEO
#
#-----------------------------------
# Defining Input Variables
#-----------------------------------

# -l: granlink
# -S: satellite directory filepath including trailing slash
# -p: par filepath, name, and extension
# -c: earth data login cookies file including path and filename

while getopts l:S:p:c: flag
do
    case "${flag}" in
        l) granlink=${OPTARG};;
        S) satDir=${OPTARG};;
        p) parFile=${OPTARG};;
        c) cookieFile=${OPTARG};;
    esac
done

#-----------------------------------
# Downloading file
#-----------------------------------

#string manipulation to set savedir
filename=${granlink##*/}

sat=${filename:0:1}
year=${filename:1:4}
doy=${filename:5:3}

#sorting file names
base=${filename%%.*}
L1Afile=${filename}
ext=${filename#*.}
ext2=${ext%.*}
sen=${ext2:4}
if [[ $sen = SNPP ]]
then
	geofile=$base.GEO-M_SNPP.nc
	L2file=$base.L2
elif [[ $sen = JPSS1 ]]
then 
	geofile=$base.GEO-M_JPSS1.nc
	L2file=$base.L2
else
	echo "ERROR: don't recognize viirs sensor extension"
	exit 1
fi
outputlog=$base.log
parfile=$base.par
tprfile=$base.tpr
defaultpar=$parFile
#defaultpar=/mnt/storage/labs/mitchell/nasacms2018/analysis/scripts/pardefaults.par

#GEO download url
geourl=https://oceandata.sci.gsfc.nasa.gov/cmr/getfile/$geofile

if [[ $sat = V ]]
then
	satellite=viirs
else
	echo "ERROR: unrecognized satellite sensor. Aborting..."
	exit 1
fi

savedir=$satDir$satellite/$year/$doy/
mkdir -p $savedir
cd $savedir

#download L1A file if it doesn't already exist
#NB: user credentials in ~/.urs_cookies
if [[ ! -f $savedir$filename ]]; then
	echo "***** Downloading " $filename " *****"
	wget --load-cookies=$cookieFile --auth-no-challenge=on \
	--directory-prefix=$savedir --content-disposition -o $outputlog $granlink #was ~/.urs_cookies
	wgetL1AStatus=$?
fi

if [[ wgetL1AStatus -eq 0 ]]; then
	#download GEO file if it doesn't already exist
	#NB: user credentials in ~/.urs_cookies
	if [[ ! -f $savedir$geofile ]]; then
		echo "***** Downloading " $geofile " *****"
		wget --load-cookies=$cookieFile --auth-no-challenge=on \
		--directory-prefix=$savedir --content-disposition -o $outputlog $geourl

		wgetGEOStatus=$?
	fi

	if [[ wgetGEOStatus -eq 0 ]]; then
	
		#-----------------------------------
		# Process file to level 2
		#-----------------------------------
		echo "***** Processing " $base " *****"

		#making par file by combining anc with the defaults and filenames
		cat <<-EOF >$tprfile
			ifile=$L1Afile
			geofile=$geofile
			ofile=$L2file
			north=45
			south=42
			east=-66
			west=-71
		EOF

		cat $tprfile $defaultpar > $parfile

		#L1B to L2
		l2gen par=$parfile >> $outputlog
		l2Status=$?

		#removing unneeded files
		rm $tprfile
		rm $L1Afile
		rm $geofile
		rm $outputlog
		rm $parfile

		if [[ l2Status -eq 0 ]]; then
			echo "L2 file " $L2file " produced"
		else
			echo "ERROR: Failed processing L1A to L2 for " $base
		fi
	else
		echo "ERROR: wget fail for " $geofile
	fi
else
	echo "ERROR: wget fail for " $filename
fi
