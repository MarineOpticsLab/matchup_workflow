#!/bin/bash

scriptDir=/mnt/storage/labs/mitchell/spinkham/gitHubRepos/matchup_workflow_dev
#dataDir=/mnt/storage/labs/mitchell/projects/matchup-workflow-data/publish-dev-data/gnatsat
dataDir=/mnt/storage/labs/mitchell/projects/matchup-workflow-data/publish-dev-data/gnatsat/troubleshoot

for sensor in viirsn
do
    python $scriptDir/03-find-matchup.py --sat $sensor --seabass_file $dataDir/02-seabass-station-list-viirs.sb \
    --output_file $dataDir/03-L2-granlinks-viirsnpp.csv --data_type oc --max_time_diff 3 --verbose --includeGnatsCheck 1
done