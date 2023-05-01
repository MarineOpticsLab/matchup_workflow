##!/bin/bash

#This script loops through 4 ocean color satetllites and passes the seabass file as input to 04a-find_matchup.py with each satellite.  The seabass file contains datetimes, cruise ids, and latitudes/longitudes for Script 04a searches the CMR data repository for granules within a 6 hour window of the field datapoint/location.  CMR only contains L2 files.  04a outputs a file that contains field latitude, longitude, datetime, cruise id, and the matching satellite granule L2 download url. However, we need to download L1a files and process them ourselves.
# After 04a is done, this script feeds the output file of 04a into 04b-editurls.py.  04b edits its input file and outputs a file that contains just cruise id, granule id, and L1a download urls. 04b also outputs a file containing the same columns as its other output file, but this time containing only rows with unique download urls.


### INPUTS: ###

# -b = sb file
# -F = output from find_matchup and input file to editurls
# -u = unique output file path from editurls (used as input for satproc_initialize)
# -s = scriptPath defined in main
# -f = optional full output file from editurls. If not specified
# the info in the input file is overwritten


while getopts "b:F:u:s:f:" flag
do
    case "${flag}" in
        b) sbFile=${OPTARG};;
        F) fullGranLoc=${OPTARG};;
        u) uqGran=${OPTARG};;
        s) scriptPath=${OPTARG};;
        f) fullGran=${OPTARG};;
    esac
done


for sensor in modisa modist viirsn seawifs viirsj1 #srp edited this lin 08/01/2022. It has not been tested yet. Simply included viirsj1 in list of satellites.
do
    python $scriptPath/04a-find_matchup.py --sat $sensor --seabass_file $sbFile \
    --output_file $fullGranLoc --data_type oc --max_time_diff 6 --verbose
done
wait
python $scriptPath/04b-editurls.py --ifile $fullGranLoc --uofile $uqGran --fofile ${fullGran-$fullGranLoc}