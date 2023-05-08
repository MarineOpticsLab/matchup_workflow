#!/bin/bash

scriptDir=/mnt/storage/labs/mitchell/spinkham/gitHubRepos/matchup_workflow_dev
dataDir=/mnt/storage/labs/mitchell/projects/matchup-workflow-data/publish-dev-data/gnatsat

# From the field data file, create a SeaBASS formatted station list containing time, location, and ID data:
#python $scriptDir/02-seabass-station-list.py --fieldFile $dataDir/01-gnats-field-data.csv --idField ID --datetimeField yyyy-mm-ddThh:mm:ss --latitudeField Latitude --longitudeField Longitude --ofile $dataDir/02-seabass-station-list.sb


# For a user defined list of satellites, search the CMR data repository for L2 download urls that matchup field data described in the SeaBASS station list within a given time window.
# Note, list of satellites may include: ['modisa','modist','viirsn','viirsj1','viirsj2','meris','goci','czcs','seawifs','octs'].
# If user desires matchups with other instruments, user must add key and values to the dict_plat in 03-find-matchup.py.

# for sensor in modisa modist seawifs viirsn viirsj1 viirsj2
for sensor in modisa modist seawifs viirsn viirsj1
do
    python $scriptDir/03-find-matchup.py --sat $sensor --seabass_file $dataDir/02-seabass-station-list.sb \
    --output_file $dataDir/03-L2-granlinks.csv --data_type oc --max_time_diff 3 --verbose --includeGnatsCheck 1
done


# Edit the L2 urls to L1a urls which we will download:
python $scriptDir/04-edit-L2-urls.py --L2granlinksFile $dataDir/03-L2-granlinks.csv --ofile $dataDir/04-L1a-granlinks.csv

# Expand Bounding Boxes if multiple records point to single L1a granule. 
# Output unique L1a granule list for downloading.
python $scriptDir/05-create-L1a-download-list.py --L1aGranlinksFile $dataDir/04-L1a-granlinks.csv --ofile $dataDir/05-download-urls.csv

#############################
### Satellite Processing ###
############################
# For Script 06, first specify user's earthdata login cookies file.
# Create satellite file save directory if does not already exist.
cookieFile=/home/spinkham/.urs_cookies
satFileDir=$dataDir/satellite-files
if [ ! -d $satFileDir ]
then mkdir $satFileDir
fi


### Download and Process Seawifs Files: ###
satellite=seawifs
if [ ! -d $satFileDir/$satellite ]
then mkdir $satFileDir/$satellite
fi
parFile=$scriptDir/06e-pardefaults.par

while IFS=, read -r granid granlink wlon slat elon nlat;
	while [ $(jobs | wc -l) -ge 40 ] ; do
		sleep 1s
	done
    year=${granid: -13:-9}
    doy=${granid: -9:-6}
    L2file=$granid.L2
    if [[ ! -f $satFileDir/$satellite/$year/$doy/$L2file ]]; then
        $scriptDir/06a-seawifs-workflow.sh -g $granlink -S $satFileDir -p $parFile -c $cookieFile -w $wlon -s $slat -e $elon -n $nlat
    else
        echo $L2file" already exists"
    fi 
done < $dataDir/05-download-urls-$satellite.csv


### Download and Process Aqua Files: ###
satellite=aqua
if [ ! -d $satFileDir/$satellite ]
then mkdir $satFileDir/$satellite
fi
parFile=$scriptDir/06d-pardefaults-sst.par

while IFS=, read -r granid granlink wlon slat elon nlat; do
	while [ $(jobs | wc -l) -ge 40 ] ; do
		sleep 1s
	done
    year=${granid: -13:-9}
    doy=${granid: -9:-6}
    L2file=$granid.L2
    if [[ ! -f $satFileDir/$satellite/$year/$doy/$L2file ]]; then
        $scriptDir/06b-modis-workflow.sh -g $granlink -S $satFileDir -p $parFile -c $cookieFile -w $wlon -s $slat -e $elon -n $nlat
    else
        echo $L2file" already exists"
    fi 
done < $dataDir/05-download-urls-$satellite.csv


### Download and Process Terra Files: ###
satellite=terra
if [ ! -d $satFileDir/$satellite ]
then mkdir $satFileDir/$satellite
fi
parFile=$scriptDir/06d-pardefaults-sst.par

while IFS=, read -r granid granlink wlon slat elon nlat; do
	while [ $(jobs | wc -l) -ge 40 ] ; do
		sleep 1s
	done
    year=${granid: -13:-9}
    doy=${granid: -9:-6}
    L2file=$granid.L2
    if [[ ! -f $satFileDir/$satellite/$year/$doy/$L2file ]]; then
        $scriptDir/06b-modis-workflow.sh -g $granlink -S $satFileDir -p $parFile -c $cookieFile -w $wlon -s $slat -e $elon -n $nlat
    else
        echo $L2file" already exists"
    fi 
done < $dataDir/05-download-urls-$satellite.csv


### Download and Process Viirs SNPP Files: ###
satellite=snpp
if [ ! -d $satFileDir/$satellite ]
then mkdir $satFileDir/$satellite
fi
parFile=$scriptDir/06d-pardefaults-sst.par

while IFS=, read -r granid granlink wlon slat elon nlat; do
	while [ $(jobs | wc -l) -ge 40 ] ; do
		sleep 1s
	done
    year=${granid: -13:-9}
    doy=${granid: -9:-6}
    L2file=$granid.L2
    
    if [[ ! -f $satFileDir/$satellite/$year/$doy/$L2file ]]; then
        $scriptDir/06c-viirs-workflow.sh -i $granid -g $granlink -S $satFileDir -p $parFile -c $cookieFile -w $wlon -s $slat -e $elon -n $nlat
    else
        echo $L2file" already exists"
    fi 
done < $dataDir/05-download-urls-$satellite.csv


### Download and process JPSS1 Files: ###
satellite=jpss1
if [ ! -d $satFileDir/$satellite ]
then mkdir $satFileDir/$satellite
fi
parFile=$scriptDir/06d-pardefaults-sst.par

while IFS=, read -r granid granlink wlon slat elon nlat; do
	while [ $(jobs | wc -l) -ge 40 ] ; do
		sleep 1s
	done
    year=${granid: -13:-9}
    doy=${granid: -9:-6}
    L2file=$granid.L2
    
    if [[ ! -f $satFileDir/$satellite/$year/$doy/$L2file ]]; then
        $scriptDir/06c-viirs-workflow.sh -i $granid -g $granlink -S $satFileDir -p $parFile -c $cookieFile -w $wlon -s $slat -e $elon -n $nlat
    else
        echo $L2file" already exists"
    fi 
done < $dataDir/05-download-urls-$satellite.csv


### Download and process JPSS2 Files: ###
#satellite=jpss2
#if [ ! -d $satFileDir/$satellite ]
#then mkdir $satFileDir/$satellite
#fi
#parFile=$scriptDir/06d-pardefaults-sst.par

#while IFS=, read -r granid granlink wlon slat elon nlat; do
#	while [ $(jobs | wc -l) -ge 40 ] ; do
#		sleep 1s
#	done
#    year=${granid: -13:-9}
#    doy=${granid: -9:-6}
#    L2file=$granid.L2
    
#    if [[ ! -f $satFileDir/$satellite/$year/$doy/$L2file ]]; then
#        $scriptDir/06c-viirs-workflow.sh -i $granid -g $granlink -S $satFileDir -p $parFile -c $cookieFile -w $wlon -s $slat -e $elon -n $nlat
#    else
#        echo $L2file" already exists"
#    fi 
#done < $dataDir/05-download-urls-$satellite.csv


### Report percentages of satellite files that successfully processed to L2: ### 
python $scriptDir/07-report-L2-percent-processed.py --downloadUrlsFile $dataDir/05-download-urls.csv --satelliteFileDirectory $dataDir/satellite-files


#########################################################################################################
### Open Satellite L2 files, calculate pixel grid statistics, output field-satellite merged datarows: ###
#########################################################################################################

# Break apart the field dataframe by field datarows that are matched to specific satellites.
python $scriptDir/08-partition-field-by-satellite.py --fieldFile $dataDir/01-gnats-field-data.csv --idField ID --granlinksFile $dataDir/04-L1a-granlinks.csv --ofile_base_name $dataDir/01-gnats-field-data


underscore="_"

### Seawifs Matchups ###
satellite=seawifs
matchupDir=$dataDir/matchups/$satellite
if [ ! -d $matchupDir ]
then mkdir -p $matchupDir
fi

while IFS=, read -r id granid url ; do
    while [ $(jobs | wc -l) -ge 40 ] ; do
        sleep 1s
    done  
    if [[ ! -f $matchupDir/$id$underscore$granid.csv ]]; then
        python $scriptDir/09-matchup-datarows.py --id $id --granid $granid --fieldDf $dataDir/01-gnats-field-data-$satellite.csv --matchupDir $matchupDir --satDir $dataDir/satellite-files --ofile_excludedMatchupLog $matchupDir/x01-excluded-matchup-log-$satellite.txt &
    else
        continue
    fi    
done < $dataDir/04-L1a-granlinks-$satellite.csv
wait


### Aqua Matchups ###
satellite=aqua
matchupDir=$dataDir/matchups/$satellite
if [ ! -d $matchupDir ]
then mkdir -p $matchupDir
fi

while IFS=, read -r id granid url ; do
    while [ $(jobs | wc -l) -ge 40 ] ; do
        sleep 1s
    done  
    if [[ ! -f $matchupDir/$id$underscore$granid.csv ]]; then
        python $scriptDir/09-matchup-datarows.py --id $id --granid $granid --fieldDf $dataDir/01-gnats-field-data-$satellite.csv --matchupDir $matchupDir --satDir $dataDir/satellite-files --ofile_excludedMatchupLog $matchupDir/x01-excluded-matchup-log-$satellite.txt &
    else
        continue
    fi    
done < $dataDir/04-L1a-granlinks-$satellite.csv
wait

### Terra Matchups ###
satellite=terra
matchupDir=$dataDir/matchups/$satellite
if [ ! -d $matchupDir ]
then mkdir -p $matchupDir
fi


while IFS=, read -r id granid url ; do
    while [ $(jobs | wc -l) -ge 40 ] ; do
        sleep 1s
    done  
    if [[ ! -f $matchupDir/$id$underscore$granid.csv ]]; then
        python $scriptDir/09-matchup-datarows.py --id $id --granid $granid --fieldDf $dataDir/01-gnats-field-data-$satellite.csv --matchupDir $matchupDir --satDir $dataDir/satellite-files --ofile_excludedMatchupLog $matchupDir/x01-excluded-matchup-log-$satellite.txt &
    else
        continue
    fi    
done < $dataDir/04-L1a-granlinks-$satellite.csv
wait


### Snpp Matchups ###
satellite=snpp
matchupDir=$dataDir/matchups/$satellite
if [ ! -d $matchupDir ]
then mkdir -p $matchupDir
fi


while IFS=, read -r id granid url ; do
    while [ $(jobs | wc -l) -ge 40 ] ; do
        sleep 1s
    done  
    if [[ ! -f $matchupDir/$id$underscore$granid.csv ]]; then
        python $scriptDir/09-matchup-datarows.py --id $id --granid $granid --fieldDf $dataDir/01-gnats-field-data-$satellite.csv --matchupDir $matchupDir --satDir $dataDir/satellite-files --ofile_excludedMatchupLog $matchupDir/x01-excluded-matchup-log-$satellite.txt &
    else
        continue
    fi    
done < $dataDir/04-L1a-granlinks-$satellite.csv
wait

### Jpss1 Matchups ### 
satellite=jpss1
matchupDir=$dataDir/matchups/$satellite
if [ ! -d $matchupDir ]
then mkdir -p $matchupDir
fi


while IFS=, read -r id granid url ; do
    while [ $(jobs | wc -l) -ge 40 ] ; do
        sleep 1s
    done  
    if [[ ! -f $matchupDir/$id$underscore$granid.csv ]]; then
        python $scriptDir/09-matchup-datarows.py --id $id --granid $granid --fieldDf $dataDir/01-gnats-field-data-$satellite.csv --matchupDir $matchupDir --satDir $dataDir/satellite-files --ofile_excludedMatchupLog $matchupDir/x01-excluded-matchup-log-$satellite.txt &
    else
        continue
    fi    
done < $dataDir/04-L1a-granlinks-$satellite.csv
wait


###  Jpss2 Matchups ###
#satellite=jpss2
#matchupDir=$dataDir/matchups/$satellite
#if [ ! -d $matchupDir ]
#then mkdir -p $matchupDir
#fi


#while IFS=, read -r id granid url ; do
#    while [ $(jobs | wc -l) -ge 40 ] ; do
#        sleep 1s
#    done  
#    if [[ ! -f $matchupDir/$id$underscore$granid.csv ]]; then
#        python $scriptDir/09-matchup-datarows.py --id $id --granid $granid --fieldDf $dataDir/01-gnats-field-data-$satellite.csv --matchupDir $matchupDir --satDir $dataDir/satellite-files --#ofile_excludedMatchupLog $matchupDir/x01-excluded-matchup-log-$satellite.txt &
#    else
#        continue
#    fi    
#done < $dataDir/04-L1a-granlinks-$satellite.csv
#wait

############################################################
### MERGE DATAROWS INTO MATCHUP DATAFRAMES PER SATELLITE ###
############################################################

### Seawifs ###
satellite=seawifs
matchupDir=$dataDir/matchups/$satellite
python $scriptDir/10-merge-datarows.py --matchupDirectory $matchupDir --ofile $dataDir/06-matchup-$satellite.csv

### Aqua ###
satellite=aqua
matchupDir=$dataDir/matchups/$satellite
python $scriptDir/10-merge-datarows.py --matchupDirectory $matchupDir --ofile $dataDir/06-matchup-$satellite.csv

### Terra ###
satellite=terra
matchupDir=$dataDir/matchups/$satellite
python $scriptDir/10-merge-datarows.py --matchupDirectory $matchupDir --ofile $dataDir/06-matchup-$satellite.csv

### Snpp ###
satellite=snpp
matchupDir=$dataDir/matchups/$satellite
python $scriptDir/10-merge-datarows.py --matchupDirectory $matchupDir --ofile $dataDir/06-matchup-$satellite.csv

### Jpss1 ###
satellite=jpss1
matchupDir=$dataDir/matchups/$satellite
python $scriptDir/10-merge-datarows.py --matchupDirectory $matchupDir --ofile $dataDir/06-matchup-$satellite.csv

### Jpss2 ###
#satellite=jpss2
#matchupDir=$dataDir/matchups/$satellite
#python $scriptDir/10-merge-datarows.py --matchupDirectory $matchupDir --ofile $dataDir/06-matchup-$satellite.csv

####################################################################################
### Merge satellite-specific matchup dataframes into a single matchup dataframe: ###
####################################################################################

python $scriptDir/11-merge-matchup-dfs.py --matchupDf1 $dataDir/06-matchup-seawifs.csv --matchupDf2 $dataDir/06-matchup-aqua.csv --matchupDf3 $dataDir/06-matchup-terra.csv --matchupDf4 $dataDir/06-matchup-snpp.csv --matchupDf5 $dataDir/06-matchup-jpss1.csv --datetimeField yyyy-mm-ddThh:mm:ss --ofile $dataDir/07-matchup-dataframe.csv