##!/bin/bash

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


for sensor in modisa modist viirsn seawifs
do
    python $scriptPath/04a-find_matchup.py --sat $sensor --seabass_file $sbFile \
    --output_file $fullGranLoc --data_type oc --max_time_diff 6
done

python $scriptPath/04b-editurls.py --ifile $fullGranLoc --uofile $uqGran --fofile ${fullGran-$fullGranLoc}