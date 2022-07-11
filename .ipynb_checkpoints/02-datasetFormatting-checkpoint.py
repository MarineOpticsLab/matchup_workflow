# This script takes in a dataframe/csv, and runs a series of tests/formatting steps on it
def main():
    
    import pandas as pd
    import numpy as np
    import argparse
    pd.options.mode.chained_assignment = None
    
    parser = argparse.ArgumentParser(description='''\
    This script takes in a csv dataframe file and runs a series of formatting checks and corrections on the dataframe.  It can take in an UW csv, a discrete csv, or a merged csv output from mergeUwDiscrete.py.''')
    
    parser.add_argument('--datafile', nargs=1, type=str, required=True, help='''\
    Full path of csv datafile to be formatted. Can be an underway file, a discrete file, or a merged file.''')
    
    parser.add_argument('--ofile_formattedDatafile', nargs=1, type=str, required=True, help='''\
    Full path and file name to save formatted datafile.''')

    parser.add_argument('--ofile_formattingErrorLog', nargs=1, type=str, required=True, help='''\
    Full path and file name to save dataframe of recorded formatting errors.''')
    
    parser.add_argument('--gnats', nargs=1, type=int, help='''\
    If the input files are only gnats data, input 2.  If they contain gnats data but are not only gnats data, input 1.  If they do not contain gnats data, input 0 or no input.''')
    
    parser.add_argument('--dataType', nargs=1, type=int, required=True, help='''\
    Input 0 for underway only data; input 1 for discrete only data; input 2 for both underway and discrete data.''')
    
    parser.add_argument('--errorLUT', nargs=1, type=str, required=False, help='''\
    File name of the error LUT that corresponds to the input dataFile. For this script to run fully without breaking, it is necessary to input an errorLUT. If this LUT does not exist yet, and this argument is not input, the script will break. It will read out an empty errorLUT csv that needs to be filled in. The script will need to be re-run.''')
    
#    parser.add_argument('--errorTableExists', nargs=1, type=int, required=True, help='''\
#    Input 0 if an error Lookup Table has not yet been created.  Input 1 if the error look up table already exists.''')
    
#    parser.add_argument('--ofile_errorLUT', nargs=1, type=str, required=True, help='''\
#    Filepath and name for where the errorLUT should be, or already is saved.''')


    args = parser.parse_args()
    dict_args = vars(args)
    
    if dict_args['dataType'][0]==2:
        df = pd.read_csv(dict_args['datafile'][0], na_values=[-999.,-99.0])
    else:
        df = pd.read_csv(dict_args['datafile'][0], sep='\t', na_values=[-999.,-99.0])
    print('We are starting with a dataframe of shape:', df.shape)

########## datetime formatting: ######################################################################
# datetimes in csvs are read in as strings. Convert these to pandas datetime objects.

    for element in df.columns:
        if 'yyyy-mm-ddThh:mm:ss' in element:
            df[element] = pd.to_datetime(df[element])
            
   
    if ((dict_args['dataType'][0]==0)|(dict_args['dataType'][0]==1)): #for datatype==2 (merged data) these steps were already done in the merge script:
        df.sort_values(by='yyyy-mm-ddThh:mm:ss', inplace=True, ignore_index=True)
        df = dropDuplicateDatetimeRows(df, 'yyyy-mm-ddThh:mm:ss')
        
        #Eliminate whitespace from uw cruise column for gnats entries
        if dict_args['gnats'][0]==2:
            df['Cruise'] = [element[-8:] for element in df['Cruise']]
            df = cruiseDatetimeAgreement(df, 'Cruise', 'yyyy-mm-ddThh:mm:ss')
        if dict_args['gnats'][0]==1:
            df['Cruise'] = [element[-8:] if all([element[-8]=='s', element[-1] in ['e','w']]) else element for element in df['Cruise']]            
            
############# general formatting ################################################################
    df.rename(columns=dict(zip(df.columns, [element.replace(' ','_') for element in df.columns])), inplace=True) #replace whitespace with underscores
    #drop known, unwanted columns:
    for element in df.columns:
        if element in ['UWTime','UWLatitude','UWLongitude','Latitude[Degrees_East]',
                       'Longitude[Degrees_North]','Temperature[Deg_C]']:
            df.drop(columns=element, inplace=True)
    if dict_args['dataType'][0]==1: #if discrete file, drop the uw specific columns
        df.drop(columns=['Salinity[PSU]','bbtot470[1/m]','bbtot532[1/m]','bbtot532Std[1/m]','bbtot676[1/m]','bbacid[1/m]','bbacidStd[1/m]','bbprime[1/m]','bbprimeStd[1/m]','numSamples','ChlCalibrated[mg/m^3]','ChlFluorometer[Factory_calibration_as_mg/m^3]','CDOMfluorometer[QSDE_as_ppb]', 'Unnamed:_161'], inplace=True)
    #drop other columns and rows that may have slipped through the cracks:    
    df.drop_duplicates(ignore_index=True, inplace=True) #drops duplicate rows
    dropDuplicateColumns(df) #drop identical columns (by value) that have the same name
    df.dropna(axis=0, how='all', inplace=True) #drop nanrows
    #df.dropna(axis=1, how='all', inplace=True) #drop nan columns ##############*************************$$$$$$$$$$$$$$$$$$$ SRP 05/6/22 testing
    
    # later on in the workflow, in find_matchup.py, if the seabass file contains nans in either of the lat or longitude columns, the script throws an error, and the processing breaks. Therefore, here, I am counter acting that by eliminating any field data points that have any null values in their lats/lons.
    df  = df.loc[(df.Latitude_uw.isnull()==False)&(df.Longitude_uw.isnull()==False)]
    
    
    print('done with general formatting')
################ Detect and Nullify Gross Errors due to Formatting ########################

    formatErrorsRecord = []
    for column in df.columns[:]:
        if column in ['Latitude_uw','Longitude_uw']:  #SRP: Included this if statement, because without it, 2 latitudes were nullified due to an order of magnitude check, and this created issues with the check lat/lons seabass file in find_matchup.py.
            continue
        else:
            formatErrors_idx = []
            originalValues = []
            # if dtype==object and all non-nan elements are string, call stringCheck
            if df[column].dtype=='object':
                if all([type(element)==str for element in df[column].loc[df[column].isnull()==False]]): #if all non-nan elements are strings:
                    idx, vals = stringCheck(df[column])
                    formatErrors_idx.append(idx)
                    originalValues.append([str(element)+'_stringCheck' for element in vals])
                else: #if dtype==object but not all non-nan elements are string, call typeCheck
                    idx, vals = typeCheck(df[column])
                    formatErrors_idx.append(idx)
                    originalValues.append([str(element)+'_typeCheck' for element in vals])
            elif ((df[column].dtype==float)|(df[column].dtype==int)): #if dtype is either float or integer, call signCheck and orderMagnitudeCheck
                idx, vals = signCheck(df[column])
                formatErrors_idx.append(idx)
                originalValues.append([str(element)+'_signCheck' for element in vals])

                idx, vals = orderMagnitudeCheck(df[column])
                formatErrors_idx.append(idx)
                originalValues.append([str(element)+'_magnitudeCheck' for element in vals])

            combinedIdx = [i for specificCheckFunctionIdx in formatErrors_idx for i in specificCheckFunctionIdx] #combine and flatten error index arrays
            combinedVals = [value for specificCheckFunctionVals in originalValues for value in specificCheckFunctionVals]

            formatErrorsPerColumnDict = {}
            for idx in combinedIdx:
                formatErrorsPerColumnDict[idx] = []
            for idx,val in zip(combinedIdx, combinedVals):
                formatErrorsPerColumnDict[idx].append(val)

            formatErrorsPerColumn = pd.Series(formatErrorsPerColumnDict, name=column, dtype='object')
            formatErrorsRecord.append(formatErrorsPerColumn)
        
    formatErrorsDataframe = pd.concat(formatErrorsRecord, axis=1)
    formatErrorsDataframe.to_csv(dict_args['ofile_formattingErrorLog'][0], index=True)
    print('done with errors due to formatting issues')
    
##NOTE: In the error section just above, I needed to utilize a dictionary, or else for underway only data, an error was thrown about duplicate axis..what happened was that a few indices triggered multiple checks..ie both a magnitude check and a sign check. There for in my combined idx array, I had a few repeated values. A series could not be made on an index with duplicate values.###        
    
################## New Error LUT procedure SRP 04/20/2020 ###################################

    if 'errorLUT' not in dict_args:
        cols = pd.Series(df.columns, name='Variable')
        err_abs = pd.Series(data=None, name='Error_Absolute')
        err_per = pd.Series(data=None, name='Error_Percent')
        error_df = pd.concat([cols, err_abs, err_per], axis=1)
        error_df.to_csv('empty_'+dict_args['errorLUT'][0], index=False)
        print('To proceed with this script, it is necessary to fill in the error LUT output to: empty_' + dict_args['errorLUT'][0] +' . If error values are unknown, leave cells blank.  Once the error LUT is filled in, re-run this script.')
    
    error_df = pd.read_csv(dict_args['errorLUT'][0])
    
    
################ READ IN ERROR TABLE AND CREATE ERROR COLUMNS ########################
## This bit is clunky.  This script requires two input variables: errorTableExists, and ofile_errorLUT.  If the error Table does not exist, the script stops because it reads out an empty error table to be filled in, and throws an error.  Then the script needs to be re-run from the start once an error table is present.  If the error table is present from the beginning, then the script runs smoothly.

#    if dict_args['errorTableExists'][0]==0:
#        cols = pd.Series(df.columns, name='Variable')
#        err_abs = pd.Series(data=None, name='Error_Absolute')
#        err_per = pd.Series(data=None, name='Error_Percent')
#        error_df = pd.concat([cols, err_abs, err_per], axis=1)
#        error_df.to_csv(dict_args['ofile_errorLUT'][0], index=False)
#        print('To proceed with this script, it is necessary to fill in the error LUT output to:' + #dict_args['ofile_errorLUT'][0] +' . If error values are unknown, leave cells blank.  Once the error #LUT is filled in, re-run this script.')
        
#    if dict_args['errorTableExists'][0]==1:
#        error_df = pd.read_csv(dict_args['ofile_errorLUT'][0])'''

    err_abs = error_df.loc[error_df['Error_Absolute'].isnull()==False, ['Variable','Error_Absolute']]
    err_percent = error_df.loc[error_df['Error_Percent'].isnull()==False, ['Variable','Error_Percent']]
    
    for variable in err_abs.Variable:
        err = err_abs['Error_Absolute'].loc[err_abs.Variable==variable].values[0]
        err_col_vals = [element if np.isnan(element)==True else err for element in df[variable]]
        err_col_name = variable+'_err'
        idx = df.columns.get_loc(variable)+1
        df.insert(idx, err_col_name, err_col_vals)
    
    for variable in err_percent.Variable:
        err = err_percent['Error_Percent'].loc[err_percent.Variable==variable].values[0]
        err_col_vals = [element*0.01*err for element in df[variable]]
        err_col_name = variable+'_err'
        idx = df.columns.get_loc(variable)+1
        df.insert(idx, err_col_name, err_col_vals)
        

    print('Done with creating and filling in error columns')
#################### Calculate Discrete and Underway Specific Columns. Include Error Propagation. #####################################

    if ((dict_args['dataType'][0]==1)|(dict_args['dataType'][0]==2)): #if the datafile contains discrete data
        df.insert(df.columns.get_loc('PIC[ug/l]')+1, 'PIC[mol/m3]_d', [element*(0.001/12) for element in df['PIC[ug/l]']])
        df.insert(df.columns.get_loc('PIC[ug/l]_err')+1, 'PIC[mol/m3]_d_err', [element*0.06*(0.001/12)/element for element in df['PIC[ug/l]']]) #multiplying and dividing by element is effectively multiplying by 1, but it means if element=nan, then error=nan
        df.drop(columns=['PIC[ug/l]','PIC[ug/l]_err'], inplace=True)
        

    if ((dict_args['dataType'][0]==0)|(dict_args['dataType'][0]==2)): #if the datafile contains underway data
        print('Calculating UW PIC based on bbprime and bbstar.  Note, this script currently uses static bbstar = 1.628.  If new satellite files are downloaded and bbstar is updated, this script will need to be updated.')
        df.insert(df.columns.get_loc('bbprimeStd[1/m]')+1, 'PIC[mol/m3]_uw', df['bbprime[1/m]']/1.628)
        df.insert(df.columns.get_loc('bbprimeStd[1/m]')+2, 'PIC[mol/m3]_uw_err', df['bbprimeStd[1/m]']/1.628)
        
        ########  RRS columns for underway data #################################
        
        wavelengths = [412,441,490,510,555,671,684]
        rho = 0.028
        i = df.columns.get_loc('es684[uW/cm^2/nm]')+1
        
        for element in wavelengths:
            li = 'li'+str(element)+'[uW/cm^2/nm/sr]'
            lt = 'lt'+str(element)+'[uW/cm^2/nm/sr]'
            es = 'es'+str(element)+'[uW/cm^2/nm]'
            Rrs = (df[lt] - 0.028*df[li])/df[es]
            name = 'Rrs'+str(element)+'[1/sr]'
            
            li_sd = 'li'+str(element)+'[uW/cm^2/nm/sr]_err'
            lt_sd = 'lt'+str(element)+'[uW/cm^2/nm/sr]_err'
            es_sd = 'es'+str(element)+'[uW/cm^2/nm]_err'
            Rrs_sd = np.sqrt(((df[lt_sd]/df[es])**2)+((df[li_sd]*0.028/df[es])**2)+(df[es_sd]*(0.028*df[li] - df[lt])/(df[es]**2))**2)
            name_sd = 'Rrs'+str(element)+'[1/sr]_err'
            
            df.insert(i, name, Rrs)
            df.insert(i+1, name_sd, Rrs_sd)
            i = i+2
################################ Check for consistency of null values between variable columns and corresponding error columns.################################
# set error columns to null if variable columns are null for that index

    err_cols = []
    var_cols = []
    std_cols = []
    var_std_cols = []
    for col in df.columns:
        if '_err' in col:
            err_cols.append(col)
            var_cols.append(col[0:-4])
        if 'Std' in col:
            std_cols.append(col)
            var_std_cols.append(col.replace('Std',''))
    
    for err,var in zip(err_cols, var_cols):
        df.loc[df[var].isnull()==True, err] = np.nan
    for std,var in zip(std_cols, var_std_cols):
        df.loc[df[var].isnull()==True, std] = np.nan            
            
 ###################### ORGANIZE COLUMNS #############################################
    if dict_args['dataType'][0]==2: #if the datafile is a merged uw and discrete data file:      
        id_cols = ['Cruise_uw','Station_uw','yyyy-mm-ddThh:mm:ss_uw','Longitude_uw','Latitude_uw','Cruise_d','Station_d',
           'yyyy-mm-ddThh:mm:ss_d','Longitude_d','Latitude_d','Type']
        other_cols = [element for element in df.columns if element not in id_cols]
        organized_cols = id_cols + other_cols
        df = df[organized_cols]
        
        #eliminate unmatched discrete entries (discrete data points with no underway within 5 minutes)
        print('Note that there are 33 discrete data points with no matching underway stations within 5 minutes.  We have decided to eliminate these data points, as in order to merge the field data with the satellite data, we merge on ID = cruise_stationUW.  Without an underway station number, it is impossible to merge using the current structure.  This should be revisited later.  Comment out the next command if it is decided to include these unmatched discrete datapoints.')
        #df = df.loc[~((df['Station_uw'].isnull()==True)&(df['Station_d'].isnull()==False))]
        df = df.loc[df['Station_uw'].isnull()==False]
        df['Station_uw'] = [int(element) for element in df['Station_uw']]
        
  ############### Create an ID column that will be used to merge field data with satellite data #####################
        # Note, this is done with UW ids.  If we only have discrete data, we will need to find a new way to merge the field data with the satellite data.
    if dict_args['dataType'][0]==0:
        df.insert(0, 'ID', [str(cruise)+'_'+str(station) for cruise, station in zip(df['Cruise'],df['Station'])])
    elif dict_args['dataType'][0]==2:
        df.insert(0, 'ID', [str(cruise)+'_'+str(station) for cruise, station in zip(df['Cruise_uw'],df['Station_uw'])])
    elif dict_args['dataType'][0]==1:
        print('For discrete only files, we can not create an ID column used to merge discrete field data with satellite data. With underway data, this merge id is created from cruise and station ids. The station naming is different for discrete data and a merge ID cannot be created. This script will still format and error check the discrete file, but it will not configure it to be merged with satellite data until a new matchup/merge ID scheme is created.')

        
    print('The final error-edited dataframe shape is:', df.shape)
        
    df.to_csv(dict_args['ofile_formattedDatafile'][0], index=True)

            
        
def dropDuplicateDatetimeRows(dataframe, datetimeColumnName): #if there are duplicate datetimes, drop the rows of the non-first datetime occurrence
    import pandas as pd
    import numpy as np
    
    if len(dataframe[datetimeColumnName])!=len(dataframe[datetimeColumnName].unique()):
        dataframe.drop(index=dataframe[dataframe[datetimeColumnName].duplicated()==True].index, inplace=True)
    dataframe.reset_index(drop=True, inplace=True)
    return dataframe

def cruiseDatetimeAgreement(dataframe, cruiseColumnName, datetimeColumnName): #only call this function after the uw cruise strings whitespaceCheck has been called
    import pandas as pd
    import numpy as np
    
    dataframe[datetimeColumnName] = pd.to_datetime(dataframe[datetimeColumnName])
       
    for i in dataframe.index:
        if dataframe[cruiseColumnName][i][1:3]!=str(dataframe[datetimeColumnName][i].year)[-2:]: #if the cruise name year and the datetime year don't agree, drop the row
            dataframe.drop(index=i, inplace=True)
    dataframe.reset_index(drop=True, inplace=True)
    return dataframe

def dropDuplicateColumns(df):
    import pandas as pd
    import numpy as np
    
    col_names = [element for element in df.columns]
    for i in col_names:
        for j in col_names:
            if i!=j:
                if df[i].eq(df[j]).all():
                    df.drop(columns=j, inplace=True)
                    col_names.remove(j)

        
############################# Formatting Error Functions ########################################

#call the following funciton if column.dtype==object
def typeCheck(column): #check to make sure all cells agree with type, aside from nans; floats and ints may be interchangeable
    #Note: Nans do not affect column dtype
    import numpy as np
    import pandas as pd
    
    col_na = column.loc[column.isnull()==False]
    idx_na = column.loc[column.isnull()==False].index
    
    if all([type(element)==str for element in col_na]): #if all non-nans are string exit out of function
        return [], []
    
    else:
        unique_types = []
        for element in col_na:
            if type(element) in unique_types:
                continue
            else:
                unique_types.append(type(element))
                
        type_counts = []
        for element in unique_types:
            count = 0
            for cell in col_na:
                if type(cell)==element:
                    count = count+1
            type_counts.append(count)
        majority_type = unique_types[type_counts.index(max(type_counts))] #gives the type with the most occurrences
        
        if np.isin(majority_type, [int,float]): #if the majority type is an int or float AND the element is an integer or float, continue, if it is something else, flag it
            idx = []
            values = []
            for i in idx_na:
                if np.isin(type(column[i]), [int,float])==True:
                    continue
                else:
                    idx.append(i)
                    values.append(column[i])
            column.iloc[idx] = np.nan
            return idx, values

        else:
            idx = []
            values = []
            for i in idx_na:
                if type(column[i])==majority_type:
                    continue
                else:
                    idx.append(i)
                    values.append(column[i])
            column.iloc[idx] = np.nan
            return idx, values
    
def signCheck(column):  #call this function if type is float or integer ## checks to see neg/pos sign of individual cells match the rest of the column
    import numpy as np
    import pandas as pd
    
    upper_limit = column.quantile(0.75) + 1.5*(column.quantile(0.75) - column.quantile(0.25)) #pd.quantile ignores nans
    lower_limit = column.quantile(0.25) - 1.5*(column.quantile(0.75) - column.quantile(0.25))
    
    positive_count = len(column.loc[column>0])
    negative_count = len(column.loc[column<0])
    
    if positive_count >= 0.98*(positive_count + negative_count): #if 98% or more of values are positive:
        changed_values = column.loc[(column < 0) & (column < lower_limit)]
        column.loc[(column < 0) & (column < lower_limit)] = np.nan #nullify negative values that are outside of 1.5*IQR
        return list(changed_values.index), list(changed_values.values)
        
    elif negative_count >= 0.98*(positive_count + negative_count):
        changed_values = column.loc[(column > 0) & (column > upper_limit)]
        column.loc[(column > 0) & (column > upper_limit)] = np.nan #nullify positive values that are outside of 1.5*IQR
        return list(changed_values.index), list(changed_values.values)
    
    else:
        return [],[]    


#The following function checks to see that if expected, all elements will have the same number of characters, and that they have the same first and last characters, again, if expected, with expectation being defined as >98% of the entries are of uniform string formatting.

def stringCheck(column):
    import numpy as np
    import pandas as pd

    col_na = column.loc[column.isnull()==False]
    idx_na = column.loc[column.isnull()==False].index
    
    num_characters = [len(element) for element in col_na]
    uq_num_characters = np.unique(num_characters)
    occurrences = [num_characters.count(element) for element in uq_num_characters]
    max_occurrences = max(occurrences)
    majority_num_characters = uq_num_characters[occurrences.index(max_occurrences)]
    
    idx = []
    vals = []
    
    if max_occurrences > 0.98*len(col_na): #if there is >98% majority
        i=0
        for i in idx_na:
            if len(column[i])!=majority_num_characters:
                idx.append(i)
                vals.append(column[i])
                
    first_char = [element[0] for element in col_na]
    uq_first_char = np.unique(first_char)
    first_char_counts = [first_char.count(element) for element in uq_first_char]
    max_first_char_counts = max(first_char_counts)
    majority_first_char = uq_first_char[first_char_counts.index(max_first_char_counts)]
    
    if max_first_char_counts > 0.98*len(col_na):
        for i in idx_na:
            if column[i][0]!=majority_first_char:
                idx.append(i)
                vals.append(column[i])

    last_char = [element[-1] for element in col_na]
    uq_last_char = np.unique(last_char)
    last_char_counts = [last_char.count(element) for element in uq_last_char]
    max_last_char_counts = max(last_char_counts)
    majority_last_char = uq_last_char[last_char_counts.index(max_last_char_counts)]
    
    if max_last_char_counts > 0.98*len(col_na):
        i=0
        for i in idx_na:
            if column[i][-1]!=majority_last_char:
                idx.append(i)
                vals.append(column[i])
            i = i+1
    
    column.iloc[idx] = np.nan
    return idx, vals


# The next function checks the expected order of magnitude based on 98% or more agreement in the data.  If a data point has a different order of magnitude, and falls outside of boxplot whiskers, it is flagged and nullified.
## The following functions bothers me because it is on the verge of formatting error detection to normal outlier detection.  The function was written to capture errors due to digits being cut off from cycle sampling stops and starts.

def orderMagnitudeCheck(column):
    import numpy as np
    import pandas as pd
    import math
    
    if len(column.loc[(column.isnull()==False)&(column!=0)])==0:
        return [],[]
    
    else:

        idx = []
        vals = []
        idx_na_zero = column.loc[(column.isnull()==False)&(column!=0)].index

        upper_limit = column.quantile(0.75) + 1.5*(column.quantile(0.75) - column.quantile(0.25))
        lower_limit = column.quantile(0.25) - 1.5*(column.quantile(0.75) - column.quantile(0.25))

        order = [math.floor(math.log10(abs(element))) for element in column[idx_na_zero]] #gives order of magnitude
        uq_order = np.unique(order)
        counts = [order.count(element) for element in uq_order] #how many occurrences of each OofM
        max_count = max(counts)
        majority_order = uq_order[counts.index(max_count)]

        if max_count > 0.98*len(column.loc[column.isnull()==False]): #if there is >98% majority
            for i in idx_na_zero:
                if math.floor(math.log10(abs(column[i])))!=majority_order:
                    if ((column[i]<lower_limit) | (column[i]>upper_limit)):  #if the value is also considered an outlier
                        idx.append(i)
                        vals.append(column[i])
        column.iloc[idx] = np.nan

        return idx, vals  

if __name__ == "__main__": main()