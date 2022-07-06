#!/bin/bash

# This script is for downloading and processing satellite files
# with ocean color products AND SST specified in a par file.
# NOTE: we can't get SST for SeaWiFS (and JPSS through ocssw code)
# so a separate par file needs to be submitted for it. This script
# assumes that par file is named the same as the MODIS / VIIRS file
# but with a _seawifs suffix e.g. pardefaults_seawifs.par
#
# Specifically, this script:
# 1) reads a text file with seawifs, modis and viirs download urls
# 2) calls the appropriate workflow for a given satellite
# 3) forks the while loop, so multiple instances run at once
#
#-----------------------------------
# Defining input variables
#-----------------------------------

# -g: unique granule file
# -S: satellite directory filepath including trailing slash
# -p: par filepath, name, and extension
# -s: script path
# -c: earth data login cookies file including path and filename
# -t: option to include SST; 1 = True, 0 = False
# -n: number of cpus. Make sure this agrees with the submission script.

while getopts "g:S:p:s:c:t:n:d:" flag
do
    case "${flag}" in
        g) granuleFile=${OPTARG};;
        S) satDir=${OPTARG};;
        p) parFile=${OPTARG};;
        s) scriptPath=${OPTARG};;
        c) cookieFile=${OPTARG};;
        t) includeSST=${OPTARG};;
        n) ncpus=${OPTARG};;
        d) dataPath=${OPTARG};;
    esac
done

if [ ! -d $dataPath/satellite ]
then mkdir $dataPath/satellite
fi

# function to loop over for every download url
satproc_init()
{
#--------------------------------
# Defining function inputs
#--------------------------------
# -l: granule link
# -i: granule id
# -S: satellite directory filepath including trailing slash
# -p: par filepath, name, and extension
# -s: script path
# -c: earth data login cookies file including path and filename
# -t: option to include SST; 1 = True, 0 = False

while getopts "l:i:S:p:s:c:t:" flag
do
    case "${flag}" in
        l) granlink=${OPTARG};;
        i) granid=${OPTARG};;
        S) satDir=${OPTARG};;
        p) parFile=${OPTARG};;
        s) scriptPath=${OPTARG};;
        c) cookieFile=${OPTARG};;
        t) includeSST=${OPTARG};;
    esac
done

sat=${granid:0:1}
year=${granid:1:4}
doy=${granid:5:3}
L2file=$granid.L2
savedir=$satDir
cd $scriptPath

####  SST Option: designate par files ####
# the default par file that is read in is par without sst.
# seawifs needs to ALWAYS call on par without sst, even if includeSST=1
# if includeSST=1, reset par file to par file with sst for modis and viirs workflows.

sw_parfile=$parFile

if [ $includeSST -eq 1 ]; then
    parFile=${parFile%%.*}_sst.par
fi
##############################################

if [[ $sat = A ]]
then
	satellite="aqua"
	if [[ ! -f $savedir$satellite/$year/$doy/$L2file ]]; then
		$scriptPath/05a-modis_workflow.sh -l $granlink -S $savedir -p $parFile -c $cookieFile
	else
		echo $L2file" already exists"
	fi
elif [[ $sat = T ]]
then
	satellite="terra"
	if [[ ! -f $savedir$satellite/$year/$doy/$L2file ]]; then
		$scriptPath/05a-modis_workflow.sh -l $granlink -S $savedir -p $parFile -c $cookieFile
	else
		echo $L2file" already exists"
	fi
elif [[ $sat = S ]]
then
	satellite="seawifs"
	if [[ ! -f $savedir$satellite/$year/$doy/$L2file ]]; then
        $scriptPath/05b-seawifs_workflow.sh -l $granlink -S $savedir -p $sw_parfile -c $cookieFile
	else
		echo $L2file" already exists"
	fi
elif [[ $sat = V ]]
then
	satellite="viirs"
	if [[ ! -f $savedir$satellite/$year/$doy/$L2file ]]; then
		$scriptPath/05c-viirs_workflow.sh -l $granlink -S $savedir -p $parFile -c $cookieFile
	else
		echo $L2file" already exists"
	fi
else
	echo "ERROR: unrecognized satellite sensor. Aborting..."
fi
}


while IFS=, read -r granid granlink ; do
	while [ $(jobs | wc -l) -ge $ncpus ] ; do
		sleep 1s
	done
	satproc_init -l $granlink -i $granid -S $satDir -p $parFile -s $scriptPath -c $cookieFile -t $includeSST &
done < $granuleFile
wait