def main():
    
    import pandas as pd
    import argparse
    
    parser = argparse.ArgumentParser(description='''\
    This script takes in a field dataframe, containing an id field, and creates a SeaBASS formatted file containing station info: datetime, latitude, longitude, station.''')
    
    parser.add_argument('--fieldFile', nargs=1, type=str, required=True, help='''\
   Full path, filename, and extension of the field data, which must contain an id field.''')
    
    parser.add_argument('--idField', nargs=1, type=str, required=True, help='''\
    Name of id field/column.''')
    
    parser.add_argument('--datetimeField', nargs=1, type=str, required=True, help='''\
    Name of datetime field/column. Note that this script works on datetime strings formatted as yyyy-mm-ddThh:mm:ss.  If the datetimes are formatted otherwise, the splitDatetime function included in this script will need to be modified.''')
    
    parser.add_argument('--latitudeField', nargs=1, type=str, required=True, help='''\
    Name of latitude field. Note that the latitudes must be given in decimal degrees.''')
    
    parser.add_argument('--longitudeField', nargs=1, type=str, required=True, help='''\
    Name of longitude field. Note that the longitudes must be given in decimal degrees.''')
    
    parser.add_argument('--ofile', nargs=1, type=str, required=True, help='''\
    Full path including file name with.sb extension for where to write the seabass formatted station list.''')
    
    
    args = parser.parse_args()
    dict_args = vars(args)
    
    # Define input arguments as variables for easier access:
    field_fn = dict_args['fieldFile'][0]
    data_id = dict_args['idField'][0]
    datetime = dict_args['datetimeField'][0]
    latitude = dict_args['latitudeField'][0]
    longitude = dict_args['longitudeField'][0]
    ofile_fn = dict_args['ofile'][0]
    
    # Read In Data:
    field = pd.read_csv(field_fn)
    
    # Split the datetime column into six datetime components: year, month, day, hour, minute, second.
    # Note that the splitDatetime function may need to be edited depending on the format of the datetimes in the field dataframe. The default function is based on datetime strings formatted as: 'yyyy-mm-ddThh:mm:ss'.
    seabassDf = pd.DataFrame(splitDatetime(field[datetime]), columns=['year','month','day','hour','min','sec'])
    
    # Insert Latitude and Longitude columns into the seabass dataframe:
    seabassDf['ID'] = field[data_id]
    seabassDf['lon'] = field[longitude]
    seabassDf['lat'] = field[latitude]
    
    # Write SeaBASS station list to csv file with seabass formatted header:
    with open(ofile_fn,'w') as file:
        file.write('/begin_header\n')
        file.write('/delimiter=comma\n')
        file.write('/missing=NaN\n')
        file.write('/fields=year,month,day,hour,minute,second,station,lon,lat\n')
        file.write('/end_header\n')
        seabassDf.to_csv(file, header=None, index=None, sep=',', mode='a')
        
def splitDatetime(datetimeArray):
    ''' splitting Bruce's database output date-time string into
    individual parts
    INPUT: datetimeArray = either a single value or an array/list of
                        strings output from Bruce's database
                        i.e. in the format: yyyy-mm-ddThh:mm:ss
    OUTPUT: N x 6 array, where N is the number of date-time strings
            and the columns are years, months, days, hours, minutes
            and seconds respectively.'''
    import numpy as np
    import re
    
    if type(datetimeArray) != str:
        dates = np.empty((len(datetimeArray),3),dtype=int)
        times = np.empty((len(datetimeArray),3),dtype=int)
        for ix,dts in enumerate(datetimeArray): 
            dateSplit = re.split('-',dts)
            dateSplit[2] = dateSplit[2][0:2]
            timeSplit = re.split(':',dts)
            timeSplit[0] = timeSplit[0][-2:]
            
            dateSplit = np.asarray([int(d) for d in dateSplit])
            dates[ix,:] =  dateSplit
            timeSplit = np.asarray([int(t) for t in timeSplit])
            times[ix,:] =  timeSplit
    else:
        dateSplit = re.split('-',datetimeArray)
        dateSplit[2] = dateSplit[2][0:2]
        timeSplit = re.split(':',datetimeArray)
        timeSplit[0] = timeSplit[0][-2:]
        
        dates = np.asarray([int(d) for d in dateSplit])
        times = np.asarray([int(t) for t in timeSplit])
        
    dateTime = np.concatenate((dates,times),axis=1)
    
    return dateTime

if __name__ == "__main__": main()