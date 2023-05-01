## This script reads in the full granules files (of L2 urls). It sends each granule id within this
# file, line by line, as input to 06a-matchup_outputDatarows.py.  Script 06a uses the information 
# in the full granules file to read in the field dataframe, read in the satellite granules, and 
# match each field data point with its corresponding satellite data. It outputs a single row csv
# containing both field and satellite data for each matchup.

##!/bin/bash

##########################################################################################

# -d: data directory path
# -f: formatted field df
# -g: full granule links
# -s: script path defined in main
# -S: satellite path and directory

while getopts "d:f:g:s:S:n:" flag
do
    case "${flag}" in
        d) dataPath=${OPTARG};;
        f) fieldDf=${OPTARG};;
        g) granuleFile=${OPTARG};;
        s) scriptPath=${OPTARG};;
        S) satDir=${OPTARG};;
        n) ncpus=${OPTARG};;
    esac
done

if [ ! -d $dataPath/matchups ]
then mkdir $dataPath/matchups
fi

underscore="_"

while IFS=, read -r id granid url ; do
    while [ $(jobs | wc -l) -ge $ncpus ] ; do
        sleep 1s
    done  
    if [[ ! -f $dataPath/matchups/$id$underscore$granid.csv ]]; then
        python $scriptPath/06a-matchup_outputDatarows.py --id $id --granid $granid --fieldDf $fieldDf --matchupDir $dataPath/matchups --satDir $satDir --ofile_excludedMatchupLog $dataPath/ofile7-excluded-matchup-log.txt &
    else
        continue
    fi    
done < $granuleFile
wait