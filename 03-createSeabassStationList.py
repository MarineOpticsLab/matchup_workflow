### This script takes in the formatted, merged field dataframe and from it, creates a station list that can be used for matchups. ###
### The output file is formatted as a SeaBASS file so that we can use a modified version of an already written script that searches the CMR data repository for matchups using a seabass data file. ###
def main():
    
    import pandas as pd
    import argparse
    
    parser = argparse.ArgumentParser(description='''\
    This script takes as input the formatted, merged, field dataframe and from it, creates a seabass formatted station list that will be used to for satellite matchups.''')
    
    parser.add_argument('--fieldDf', nargs=1, type=str, required=True, help='''\
    Full path of the formatted, merged, field dataframe output from script 02-datasetFormatting.py''')
    
    parser.add_argument('--ofile', nargs=1, type=str, required=True, help='''\
    Full path including file name with.sb extension for where to write seabass formatted station list output file.''')
    
        
    args = parser.parse_args()
    dict_args = vars(args)
    
    
    fieldDf = pd.read_csv(dict_args['fieldDf'][0])
    fieldDf = fieldDf[['ID','yyyy-mm-ddThh:mm:ss_uw','Longitude_uw','Latitude_uw']]
    
    #split the datetime column into six datetime components
    seabassDf = pd.DataFrame(databaseStrSplit(fieldDf['yyyy-mm-ddThh:mm:ss_uw']),columns=['year','month','day','hour','mins','sec'])
    
    # add in ID and lat/lon columns:
    seabassDf['ID'] = fieldDf['ID']
    seabassDf['lon'] = fieldDf['Longitude_uw']
    seabassDf['lat'] = fieldDf['Latitude_uw']
    
    with open(dict_args['ofile'][0],'w') as file:
        file.write('/begin_header\n')
        file.write('/delimiter=comma\n')
        file.write('/missing=NaN\n')
        file.write('/fields=year,month,day,hour,minute,second,station,lon,lat\n')
        file.write('/end_header\n')
        seabassDf.to_csv(file, header=None, index=None, sep=',', mode='a')
        
        
def databaseStrSplit(databaseStr):
    ''' splitting Bruce's database output date-time string into
    individual parts
    INPUT: databaseStr = either a single value or an array/list of
                        strings output from Bruce's database
                        i.e. in the format: yyyy-mm-ddThh:mm:ss
    OUTPUT: N x 6 array, where N is the number of date-time strings
            and the columns are years, months, days, hours, minutes
            and seconds respectively.'''
    import numpy as np
    import re
    
    if type(databaseStr) != str:
        dates = np.empty((len(databaseStr),3),dtype=int)
        times = np.empty((len(databaseStr),3),dtype=int)
        for ix,dts in enumerate(databaseStr): 
            dateSplit = re.split('-',dts)
            dateSplit[2] = dateSplit[2][0:2]
            timeSplit = re.split(':',dts)
            timeSplit[0] = timeSplit[0][-2:]
            
            dateSplit = np.asarray([int(d) for d in dateSplit])
            dates[ix,:] =  dateSplit
            timeSplit = np.asarray([int(t) for t in timeSplit])
            times[ix,:] =  timeSplit
    else:
        dateSplit = re.split('-',databaseStr)
        dateSplit[2] = dateSplit[2][0:2]
        timeSplit = re.split(':',databaseStr)
        timeSplit[0] = timeSplit[0][-2:]
        
        dates = np.asarray([int(d) for d in dateSplit])
        times = np.asarray([int(t) for t in timeSplit])
        
    dateTime = np.concatenate((dates,times),axis=1)
    
    return dateTime

if __name__ == "__main__": main()

