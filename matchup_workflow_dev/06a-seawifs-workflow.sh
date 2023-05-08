#!/bin/bash

# This script is for SEAWIFS files and:
# 1) downloads a L1A file for an input download url,
# 2) process the L1A to subscened L2
# 3) removes the L1A file
#
#-----------------------------------
# Defining input variables
#-----------------------------------

# -l: granlink
# -S: satellite directory filepath including trailing slash
# -p: par filepath, name, and extension
# -c: earth data login cookies file including path and filename

while getopts g:S:p:c:w:s:e:n: flag
do
    case "${flag}" in
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

sat=${filename:0:1}
year=${filename:1:4}
doy=${filename:5:3}

#sorting file names
base=${filename%%.*}
L1Afile=${filename%.*}
L1Asubfile=$L1Afile.SUB
geofile=$base.GEO
geosubfile=$geofile.SUB
L1Bfile=$base.L1B_LAC
L2file=$base.L2
outputlog=$base.log
parfile=$base.par
tprfile=$base.tpr
defaultpar=$parFile

if [[ $sat = S ]]
then
	satellite=seawifs
else
	echo "ERROR: unrecognized satellite sensor. Aborting..."
	exit 1
fi

savedir=$satDir/$satellite/$year/$doy/
mkdir -p $savedir
cd $savedir

#download L1A file if it doesn't already exist
#NB: user credentials in ~/.urs_cookies
if [[ ! -f $savedir$filename ]]; then
	echo "***** Downloading " $filename " *****"
	wget --load-cookies=$cookieFile --auth-no-challenge=on \
	--directory-prefix=$savedir --content-disposition -o $outputlog $granlink
	wgetL1AStatus=$?
fi

if [[ wgetL1AStatus -eq 0 ]]; then
	#-----------------------------------
	# Process file to level 2
	#-----------------------------------
	echo "***** Processing " $base " *****"
	
	#unzip
	bunzip2 $filename

		
	#making par file by combining anc with the defaults and filenames
	cat <<-EOF >$tprfile
		ifile=$L1Afile
		ofile1=$L2file
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
	rm $outputlog
	rm $parfile

	if [[ l2Status -eq 0 ]]; then
		echo "L2 file " $L2file " produced"
	else
		echo "ERROR: Failed processing L1A to L2 for " $base
	fi
else
	echo "ERROR: wget fail for " $filename
fi
