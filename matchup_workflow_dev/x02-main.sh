#!/bin/bash

scriptDir=/mnt/storage/labs/mitchell/spinkham/gitHubRepos/matchup_workflow_dev
dataDir=/mnt/storage/labs/mitchell/projects/matchup-workflow-data/publish-dev-data


# From the field data file, create a SeaBASS formatted station list containing time, location, and ID data:
#python $scriptDir/02-seabass-station-list.py --fieldFile $dataDir/01-field-data.csv --idField ID --datetimeField yyyy-mm-ddThh:mm:ss --latitudeField Latitude --longitudeField Longitude --ofile $dataDir/02-seabass-station-list.sb


# For a user defined list of satellites, search the CMR data repository for L2 download urls that matchup field data within the SeaBASS station list within the given parameters:
# Note, list of satellites may include: ['modisa','modist','viirsn','viirsj1','meris','goci','czcs','seawifs','octs'].
# If user desires matchups with other instruments, user must add key and values to the dict_plat in 04a-find_matchup.py.


#for sensor in modisa modist viirsn seawifs viirsj1
#do
#    python $scriptDir/03-find-matchup.py --sat $sensor --seabass_file $dataDir/temp/01-seabass-station-list.sb \
#    --output_file $dataDir/temp/02-L2-granlinks.csv --data_type oc --max_time_diff 6 --verbose --includeGnatsCheck 1
#done

# Edit the L2 urls to L1a urls which we will download:
python $scriptDir/04-edit-L2-urls.py --L2granlinksFile $dataDir/temp/02-L2-granlinks.csv --ofile $dataDir/temp/03-L1a-granlinks.csv

#python $scriptDir/05-create-L1a-download-list.py --L1aGranlinksFile $dataDir/temp/03-L1a-granlinks.csv --ofile $dataDir/temp/04-download-urls.csv


# The following three lines fork the process via a PBS scheduler.
# If user is not working with a PBS scheduler, comment out the following three lines.
# Job will then not be forked.
#
#	while [ $(jobs | wc -l) -ge $ncpus ] ; do
#		sleep 1s
#	done

#cookieFile=/home/spinkham/.urs_cookies
#satFileDir=$dataDir/satellite-files
#if [ ! -d $satFileDir ]
#then mkdir $satFileDir
#fi

##########################################################################################
# Download and Process Seawifs Files: 
#mkdir -p $dataDir/satellite/seawifs
#parFile=$scriptDir/06e-pardefaults.par
#satellite="seawifs"

#while IFS=, read -r granid granlink wlon slat elon nlat;
#	while [ $(jobs | wc -l) -ge $ncpus ] ; do
#		sleep 1s
#	done
#    sat=${granid:0:1}
#    year=${granid:1:4}
#    doy=${granid:5:3}
#    L2file=$granid.L2
#    if [[ ! -f $satFileDir/$satellite/$year/$doy/$L2file ]]; then
#        $scriptDir/06a-seawifs-workflow.sh -g $granlink -S $satFileDir/ -p $parFile -c $cookieFile -w $wlon -s $slat -e $elon -n $nlat
#    else
#        echo $L2file" already exists"
#    fi 
#done < $dataDir/L1a-granlinks-seawifs.csv

#################################################################################################
# Download and Process Aqua Files: 
#mkdir -p $dataDir/satellite/aqua
#parFile=$scriptDir/06d-pardefaults-sst.par
#satellite="aqua"

#while IFS=, read -r granid granlink wlon slat elon nlat; do
#	while [ $(jobs | wc -l) -ge $ncpus ] ; do
#		sleep 1s
#	done
#    sat=${granid:0:1}
#    year=${granid:1:4}
#    doy=${granid:5:3}
#    L2file=$granid.L2
#    if [[ ! -f $satFileDir/$satellite/$year/$doy/$L2file ]]; then
#        $scriptDir/06b-modis-workflow.sh -g $granlink -S $satFileDir/ -p $parFile -c $cookieFile -w $wlon -s $slat -e $elon -n $nlat
#    else
#        echo $L2file" already exists"
#    fi 
#done < $dataDir/L1a-granlinks-aqua.csv

################################################################################################
# Download and Process Terra Files: 
#if [ ! -d $satFileDir/terra ]
#then mkdir $satFileDir/terra
#fi
#parFile=$scriptDir/06d-pardefaults-sst.par
#satellite="terra"

#while IFS=, read -r granid granlink wlon slat elon nlat; do
#	while [ $(jobs | wc -l) -ge 40 ] ; do
#		sleep 1s
#	done
#    sat=${granid:0:1}
#    year=${granid:1:4}
#    doy=${granid:5:3}
#    L2file=$granid.L2
#    if [[ ! -f $satFileDir/$satellite/$year/$doy/$L2file ]]; then
#        $scriptDir/06b-modis-workflow.sh -g $granlink -S $satFileDir -p $parFile -c $cookieFile -w $wlon -s $slat -e $elon -n $nlat
#    else
#        echo $L2file" already exists"
#    fi 
#done < $dataDir/temp/04-download-urls-terra.csv

################################################################################################
# Download and Process Viirs Files: 
#mkdir -p $dataDir/satellite/viirs
#parFile=$scriptDir/06d-pardefaults-sst.par
#satellite="viirs"

#while IFS=, read -r granid granlink wlon slat elon nlat; do
#	while [ $(jobs | wc -l) -ge $ncpus ] ; do
#		sleep 1s
#	done
#    sat=${granid:0:1}
#    year=${granid:1:4}
#    doy=${granid:5:3}
#    L2file=$granid.L2
#    if [[ ! -f $satFileDir/$satellite/$year/$doy/$L2file ]]; then
#        $scriptDir/06c-viirs-workflow.sh -g $granlink -S $satFileDir/ -p $parFile -c $cookieFile -w $wlon -s $slat -e $elon -n $nlat
#    else
#        echo $L2file" already exists"
#    fi 
#done < $dataDir/L1a-granlinks-viirs.csv


#################################################################################################################
### Open Satellite files, calculate pixel grid statistics, output field-satellite merged datarows: ###
#################################################################################################################
# We have found that this next step also goes faster if we break it apart per satellite. Therefore the next script simply breaks apart the field dataframe by satellite. #

#python $dataDir/07-partition-field-by-satellite.py --fieldFile $dataDir/01-field-data.csv --idField ID --granlinksFile $dataDir/L1a-granlinks.csv --ofile_base_name $dataDir/field-data

#mkdir -p $dataDir/matchups
#underscore="_"

# Seawifs Matchup Datarows:
#while IFS=, read -r id granid url ; do
#    while [ $(jobs | wc -l) -ge $ncpus ] ; do
#        sleep 1s
#    done 
#    if [[ ! -f $dataDir/matchups/$id$underscore$granid.csv ]]; then
#        python $scriptDir/08-matchup-datarows.py --id $id --granid $granid --fieldDf $dataDir/field-data-seawifs.csv --matchupDir $dataDir/matchups --satDir $dataDir/satellite --ofile_excludedMatchupLog $dataDir/excluded-matchup-log-seawifs.txt &
#    else
#        continue
#    fi
#done < $granuleFile