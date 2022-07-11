#!/bin/bash

# -u: underway file name and extension, do not include path
# -d: discrete file name and extension, do not include path
# -s: scriptPath, do not include trailing slash
# -D: dataPath, do not include trailing slash
# -t: include sst data?  Input 1 for yes/include, input 0 for no/exclude
# -c: cookie file: include full path and filename of earthdata login cookies file

############################ Define script inputs: ####################################

while getopts "u:d:s:D:t:c:g:n:" flag
do
    case "${flag}" in
        u) uwFileName=${OPTARG};;
        d) discreteFileName=${OPTARG};;
        s) scriptPath=${OPTARG};;
        D) dataPath=${OPTARG};;
        t) includeSST=${OPTARG};;
        c) cookieFile=${OPTARG};;
        g) gnatsatFormatting=${OPTARG};;
        n) ncpus=${OPTARG};;
    esac
done

##################### Sequence Scripts ####################################################

# merge field data:
#python $scriptPath/01-mergeUwDiscrete.py --uwFile $dataPath/$uwFileName --discreteFile $dataPath/$discreteFileName --ofile $dataPath/ofile01-merged-field-df.csv --gnats 2

# format data:
#python $scriptPath/02-datasetFormatting.py --datafile $dataPath/ofile01-merged-field-df.csv --ofile_formattedDatafile $dataPath/ofile02-formatted-field-df.csv --ofile_formattingErrorLog $dataPath/ofile03-data-#formatting-error-log.csv --gnats 2 --dataType 2 --errorLUT $dataPath/errorLUT.csv

# Produce seabass file of field data points and location:
#python $scriptPath/03-createSeabassStationList.py --fieldDf $dataPath/ofile02-formatted-field-df.csv --ofile $dataPath/ofile04-sbfile.sb

# create granule files matching gnats data points to satellite granules:
#$scriptPath/04-find_granules.sh -b $dataPath/ofile04-sbfile.sb -F $dataPath/ofile05a-full-granules-with-location.csv -u $dataPath/ofile06-unique-granules.csv -s $scriptPath -f $dataPath/ofile05b-full-granules.csv
#wait

# Download and process satellite files
#$scriptPath/05-satproc_initialize.sh -g $dataPath/ofile06-unique-granules.csv -S $dataPath/satellite/ -p $scriptPath/pardefaults.par -s $scriptPath -c $cookieFile -t $includeSST -n $ncpus -d $dataPath

#output matchup data rows:
$scriptPath/06-matchup_readGranLinks.sh -d $dataPath -f $dataPath/ofile02-formatted-field-df.csv -g $dataPath/ofile05b-full-granules.csv -s $scriptPath -S $dataPath/satellite -n $ncpus

# merge matchup data rows into matchup dataframe:
python $scriptPath/07-mergeDatarows.py --matchupDirectory $dataPath/matchups --ofile_matchupDf $dataPath/ofile08-matchup-df.csv

# group wavelengths into bands in matchup dataframe:
python $scriptPath/08-groupWaveBands.py --matchupDf $dataPath/ofile08-matchup-df.csv --matchupBandDf $dataPath/ofile09-matchup-df-with-color-bands.csv

# if gnatsat data, run a formatting script:
if [ $gnatsatFormatting -eq 1 ]; then
    python $scriptPath/09-gnatsatSpecificFormatting.py --matchupDf $dataPath/ofile09-matchup-df-with-color-bands.csv --gnatsatV1 $dataPath/ofile10-gnatsat-v1.csv
fi

echo "All Scripts are Finished."