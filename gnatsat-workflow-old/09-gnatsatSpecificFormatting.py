## This is a script that is specific to creating the gnatsat dataset.
## It averages (mean) and propagates the error of the field data that map to one satellite pixel.
## It organizes the columns. Left: Id columns. Middle: field columns. Right: satellite columns.

def main():
    
    import pandas as pd
    import numpy as np
    import argparse
    
    parser = argparse.ArgumentParser(description='''\
    This script performs gnatsat-specific formatting functions. Specifically, it averages field data that map to one satellite pixel, and it organizes the columns in the dataframe.''')
    
    parser.add_argument('--matchupDf', nargs=1, type=str, required=True, help='''\
    Full path and name of matchup dataframe. Include .csv extension.''')
    
    parser.add_argument('--gnatsatV1', nargs=1, type=str, required=True, help='''\
    Full path and name of where to save the output dataframe.''')
    
    args = parser.parse_args()
    dict_args = vars(args)
    
    df = pd.read_csv(dict_args['matchupDf'][0])
    
    #We identify the field data that map to a unique pixel by a unique pixel index and granid combination
    uqPixelList = pd.Series(zip(df.Pixel_idx, df.granid)).unique()
    
    # field variables will be averaged, and their error will be propagated. For now, standard deviations are averaged. Subset out columns based on averaging or error propagation:
    #due to how the matchup data rows were created, columns to the left of granid are field columns; columns to the right are satellite columns:
    granidLoc = df.columns.get_loc('granid')
    
    fieldCols = [element for element in df.columns[0:granidLoc]]
    satCols = [element for element in df.columns[granidLoc:]]
    idCols = ['ID','Cruise_uw','Station_uw','Cruise_d','Station_d','Type','StationNumber','CastNumber','StationTime']
    nonIdCols = [element for element in fieldCols if element not in idCols]
    dtCols = [element for element in nonIdCols if 'yyyy' in element]
    errCols = [element for element in nonIdCols if '_err' in element]
    stdCols = [element for element in nonIdCols if 'Std' in element]
    varCols = [element for element in nonIdCols if element not in dtCols + errCols + stdCols]
    
    # average the rows and propagate the error
    avgdRows = []
    for uqPxl in uqPixelList:
        currDf = df.loc[(df['Pixel_idx']==uqPxl[0])&(df['granid']==uqPxl[1])]
        avgdRows.append(compressField(currDf, idCols, varCols, errCols, stdCols, dtCols, satCols))

    compressedDf = pd.concat(avgdRows)
    compressedDf.reset_index(drop=True)
    
######################  ORGANIZE THE COLUMNS  ###############################################
    
    granidLoc = compressedDf.columns.get_loc('granid')
    fieldCols = [element for element in compressedDf.columns[0:granidLoc]]
    satCols = [element for element in compressedDf.columns[granidLoc:]]

    rrsCols = [element for element in satCols if 'Rrs' in element]
    rhosCols = [element for element in satCols if 'rhos' in element]
    rhotCols = [element for element in satCols if 'rhot' in element]
    adgCols = [element for element in satCols if 'adg' in element]
    aCols = [element for element in satCols if all(['a_' in element, element[0]=='a'])]
    aphCols = [element for element in satCols if 'aph' in element]
    bbCols = [element for element in satCols if 'bb_' in element]
    otherSatCols = [element for element in satCols if element not in rrsCols + rhosCols + rhotCols + adgCols + aCols + aphCols + bbCols]

    rrsCols.sort()
    rhosCols.sort()
    rhotCols.sort()
    adgCols.sort()
    aCols.sort()
    aphCols.sort()
    bbCols.sort()   
    
    organizedSatCols = rrsCols + rhosCols + rhotCols + adgCols + aCols + aphCols + bbCols + otherSatCols
    idCols = ['ID','granid','Num_Avgd_Field_Data','Cruise_uw','Station_uw','yyyy-mm-ddThh:mm:ss_uw','Longitude_uw','Latitude_uw','Cruise_d','Station_d','yyyy-mm-ddThh:mm:ss_d','Longitude_d','Latitude_d','Depth[m]','StationNumber','CastNumber','StationTime','DeltaT[Minutes]','DeltaD[Km]','MixedLayerDepth[m]','Gradient']
    lastCol = ['Location_Flag']
    sstCols = [element for element in df.columns if 'sst' in element]
    leftCols = [element for element in fieldCols if element not in idCols + lastCol + sstCols]
    rightCols = [element for element in organizedSatCols if element not in idCols + lastCol + sstCols]
    finalOrder = idCols + leftCols + sstCols + rightCols + lastCol

    organizedCompressedDf = compressedDf[finalOrder]
    organizedCompressedDf.to_csv(dict_args['gnatsatV1'][0], index=False)

#################################################################################################
def error_propagation(column):
    import numpy as np
    import pandas as pd
    
    if len(column[column.isnull()==False])==0:
        error_value = np.nan
    else:
        error_value = np.sqrt(np.sum(np.square(column[column.isnull()==False])))/len(column[column.isnull()==False])        
    return error_value

def compressField(df, id_cols, variable_cols, error_cols, std_cols, datetime_cols, satellite_cols):
    import pandas as pd
    
    sat = df.iloc[0][satellite_cols] #takes the satellite column values of the first row
    
    avgdCols = []
    
    avgdCols.append(df[id_cols].iloc[0])
    avgdCols.append(df[variable_cols].mean())
    avgdCols.append(df[std_cols].mean())
    
    errVals = []
    for col in error_cols:
        errVals.append(error_propagation(df[col]))
        
    avgdCols.append(pd.Series(errVals, index=error_cols))
    avgdCols.append(sat)
    
    compressedRow = pd.DataFrame(pd.concat(avgdCols)).swapaxes(axis1='index',axis2='columns')
    
    for col in datetime_cols:
        dtCol = pd.to_datetime(df[col])
        compressedRow.insert(0, col, dtCol.mean())

    compressedRow[df.columns]
    compressedRow.insert(0, 'Num_Avgd_Field_Data', len(df))

    return compressedRow

if __name__ == "__main__": main()