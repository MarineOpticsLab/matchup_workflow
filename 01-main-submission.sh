#!/bin/bash

#PBS -N publish-matchups
#PBS -q route

#PBS -l ncpus=40,mem=512gb
#PBS -l walltime=24:00:00
#PBS -l vnode=c4-4
#PBS -o /mnt/storage/labs/mitchell/projects/matchup-workflow-data/publish-dev-data/logs
#PBS -e /mnt/storage/labs/mitchell/projects/matchup-workflow-data/publish-dev-data/logs

# Load modules and environment
module use /mod/bigelow
module load anaconda3
source activate ~/ocssw_env

scriptDir=/mnt/storage/labs/mitchell/spinkham/gitHubRepos/matchup_workflow_dev
dataDir=/mnt/storage/labs/mitchell/projects/matchup-workflow-data/publish-dev-data


# From the field data file, create a SeaBASS formatted station list containing time, location, and ID data:
#python $scriptDir/02-seabass-station-list.py --fieldFile $dataDir/01-field-data.csv --idField ID --datetimeField yyyy-mm-ddThh:mm:ss --latitudeField Latitude --longitudeField Longitude --ofile $dataDir/02-seabass-station-list.sb


# For a user defined list of satellites, search the CMR data repository for L2 download urls that matchup field data within the SeaBASS station list within the given parameters:
# Note, list of satellites may include: ['modisa','modist','viirsn','viirsj1','meris','goci','czcs','seawifs','octs'].
# If user desires matchups with other instruments, user must add key and values to the dict_plat in 04a-find_matchup.py.


#for sensor in modisa modist viirsn seawifs viirsj1
#do
#    python $scriptDir/03-find-matchup.py --sat $sensor --seabass_file $dataDir/temp/02-seabass-station-list.sb \
#    --output_file $dataDir/temp/L2-granlinks.csv --data_type oc --max_time_diff 6 --verbose --includeGnatsCheck 1
#done

# Edit the L2 urls to L1a urls which we will download:
#python $scriptDir/04-edit-L2-urls.py --L2granlinksFile $dataDir/temp/L2-granlinks.csv --ofile $dataDir/temp/L1a-granlinks.csv

python $scriptDir/05-create-L1a-download-list.py --L1aGranlinksFile $dataDir/temp/amt-l1a-bounding-box-expansion-test-file.csv --ofile $dataDir/temp/amt-test-output.csv