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

while getopts i:g:S:p:c:w:s:e:n: flag
do
    case "${flag}" in
        i) granid=${OPTARG};;
        g) granlink=${OPTARG};;
        S) satDir=${OPTARG};;
        p) parFile=${OPTARG};;
        c) cookieFile=${OPTARG};;
        w) wlon=${OPTARG};;
        s) slat=${OPTARG};;
        e) elon=${OPTARG};;
        n) nlat=${OPTARG};;
    esac
done

#-----------------------------------
# Downloading file
#-----------------------------------

#string manipulation to set savedir
filename=${granlink##*/}

#sorting file names
sen=${filename%%.*} #SNPP_VIIRS #JPSS1_VIIRS #JPSS2_VIIRS
base_ext=${filename#*.}
base=${base_ext%%.*}
L1Afile=${filename}

year=${granid: -13:-9}
doy=${granid: -9:-6}

if [[ $sen = SNPP_VIIRS ]]
then
	geofile=$sen.$base.GEO.nc
	L2file=$granid.L2
elif [[ $sen = JPSS1_VIIRS ]]
then 
	geofile=$sen.$base.GEO.nc
	L2file=$granid.L2
elif [[ $sen = JPSS2_VIIRS ]]
then 
	geofile=$sen.$base.GEO.nc
	L2file=$granid.L2
else
	echo "ERROR: don't recognize viirs sensor extension"
	exit 1
fi
outputlog=$sen.$base.log
parfile=$sen.$base.par
tprfile=$sen.$base.tpr
defaultpar=$parFile
#defaultpar=/mnt/storage/labs/mitchell/nasacms2018/analysis/scripts/pardefaults.par

#GEO download url
geourl=https://oceandata.sci.gsfc.nasa.gov/cmr/getfile/$geofile

if [[ $sen = SNPP_VIIRS ]]
then
	satellite=snpp
elif [[ $sen = JPSS1_VIIRS ]]
then
	satellite=jpss1
elif [[ $sen = JPSS2_VIIRS ]]
then
	satellite=jpss2
else
	echo "ERROR: unrecognized satellite sensor. Aborting..."
	exit 1
fi

savedir=$satDir/$satellite/$year/$doy/
if [ ! -d $savedir ]
then mkdir -p $savedir
fi
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
			north=$nlat
			south=$slat
			east=$elon
			west=$wlon
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
