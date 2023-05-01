# Matchup Workflow:

### Overview:

This repository takes in a field dataframe with valid Id, Latitude, Longitude, and Datetime Fields. Using this information, the scripts search the CMR data repository for satellite files that match up to the field data within a 6 hour window. The matchup L2 granule urls are written into a text file. The L2 extensions are edited to L1a extensions. Through wget, regions specified via bounding boxes are extracted and downloaded from the L1a files.   A 5x5 pixel grid is drawn around the pixel nearest in location to the field data point. Pixel grid statistics as per Bailey and Werdell are reported.  The satellite data is then merged to the field data, ultimately outputting a matched up field-satellite-dataframe.

### Scripts: