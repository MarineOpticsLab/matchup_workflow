### Merge Underway data with Discrete Data and correct various formatting issues. ###

def main():
    
    import pandas as pd
    import argparse
    
    parser = argparse.ArgumentParser(description='''\
    This script takes in and merges an underway database extraction with a discrete database extraction. The UW extraction must have only nav+basics selected. The discrete extraction must have bottle, bates, aiken, and flowcam selected. The script merges the underway with the discrete based on nearest time within a 5 minute time tolerance.''')
    
    parser.add_argument('--uwFile', nargs=1, type=str, required=True, help='''\
    Full path of database extracted underway file. The extraction must have had these boxes selected from the database extractor: nav + basics.''')
    
    parser.add_argument('--discreteFile', nargs=1, type=str, required=True, help='''\
    Full path of database extracted discrete file.  The extraction must have had these boxes selected from the database extractor: bottle, bates, aiken, flowcam.''')
    
    parser.add_argument('--ofile', nargs=1, type=str, required=True, help='''\
    Full path and file name to save merged datafile.''')
    
    parser.add_argument('--gnats', nargs=1, type=int, help='''\
    If the input files are only gnats data, input 2.  If they contain gnats data but are not only gnats data, input 1.  If they do not contain gnats data, input 0 or no input.''')
    
    args = parser.parse_args()
    dict_args = vars(args)

############################################ IMPORT THE DATA ########################################################################

    uw = pd.read_csv(dict_args['uwFile'][0], sep='\t', na_values = [-999.,-99.0])
    d = pd.read_csv(dict_args['discreteFile'][0], sep='\t', na_values = [-999.,-99.0])
    
    ### Check to see that the database extraction was correct by checking the number of columns is as expected. ###
    if len(uw.columns)!=92:
        parser.error('Problem with underway database extraction. There are supposed to be 92 columns.  The current underway file has '+str(len(uw.columns))+' columns.')
    if len(d.columns)!=162:
        parser.error('Problem with discrete database extraction.  There are supposed to be 162 columns.  The current discrete file has ' +str(len(d.columns))+' columns.')
    
######################## DEAL WITH FORMATTING ISSUES THAT SHOULD LATER BE TAKEN CARE OF DIRECTLY WITHIN THE DATABASE ITSELF #############################

    ### Eliminate whitespace from uw cruise column for gnats entries ###
    if dict_args['gnats'][0]==2:
        uw['Cruise'] = [element[-8:] for element in uw['Cruise']]
    if dict_args['gnats'][0]==1:
        uw['Cruise'] = [element[-8:] if all([element[-8]=='s', element[-1] in ['e','w']]) else element for element in uw['Cruise']]
    
    ### Calculate bbprime standard deviation for uw file: ###
    uw['bbprimeStd[1/m]'] = (uw['bbtot532Std[1/m]']**2 + uw['bbacidStd[1/m]']**2)**0.5
    
    ### Swap the Lt and Li columns for specific cruises: ###
    badCruises = ['s130821e','s130822w','s131105e','s131106w','s140601w','s140623w','s140722w','s140827w','s140917w','s141009w', 's141205e','s141206w']
    lt_cols = ['lt412[uW/cm^2/nm/sr]', 'lt441[uW/cm^2/nm/sr]', 'lt490[uW/cm^2/nm/sr]', 'lt510[uW/cm^2/nm/sr]', 'lt533[uW/cm^2/nm/sr]',
               'lt555[uW/cm^2/nm/sr]', 'lt671[uW/cm^2/nm/sr]', 'lt684[uW/cm^2/nm/sr]', 'lt780[uW/cm^2/nm/sr]', 'lt866[uW/cm^2/nm/sr]']
    li_cols = ['li412[uW/cm^2/nm/sr]', 'li441[uW/cm^2/nm/sr]', 'li490[uW/cm^2/nm/sr]', 'li510[uW/cm^2/nm/sr]', 'li533[uW/cm^2/nm/sr]',
               'li555[uW/cm^2/nm/sr]', 'li671[uW/cm^2/nm/sr]', 'li684[uW/cm^2/nm/sr]', 'li780[uW/cm^2/nm/sr]', 'li866[uW/cm^2/nm/sr]']
    uw2 = uw.copy()
    
    for lt, li in zip(lt_cols, li_cols):
        uw2.loc[uw2.Cruise.isin(badCruises), lt] = uw.loc[uw.Cruise.isin(badCruises), li]
        uw2.loc[uw2.Cruise.isin(badCruises), li] = uw.loc[uw.Cruise.isin(badCruises), lt]
        
    ### In the gnats discrete data, half of the depths are set to zero, half to null. Set all gnats depths to zero. ###
    if dict_args['gnats'][0]==2:
        d['Depth[m]'] = d['Depth[m]'].replace(-999,0)
    if dict_args['gnats'][0]==1: # set gnats depths to zero, if not gnats cruise ids, keep depths as is
        d['Depth[m]'] = [0 if all([element[0][0]=='s', element[0][-1] in ['e','w']]) else element[1] for element in zip(dis['Cruise'], dis['Depth[m]'])]
        
    ### Rename HPCL column to HPLC. ###
    d.rename(columns={'HPCLChla[ug/l]':'HPLCChla[ug/l]'}, inplace=True)
        
        
########################################################################################################################################################    
############### OTHER FORMATTING PRIOR TO MERGE ########################
    
    ### The Discrete file extracted from the database contains some columns that overlap with the underway file. Drop these overlapping columns from the discrete file. ###    
    overlapping_cols = [element for element in d.columns if element in uw2.columns]
    overlapping_cols_to_remove = [element for element in overlapping_cols if element not in ['Cruise','Station','yyyy-mm-ddThh:mm:ss','Longitude','Latitude']]
    d.drop(columns=overlapping_cols_to_remove, inplace=True)
    
    ### Convert datetime columns into datetime objects and sort by datetime. ###
    uw2['yyyy-mm-ddThh:mm:ss'] = pd.to_datetime(uw2['yyyy-mm-ddThh:mm:ss'])
    d['yyyy-mm-ddThh:mm:ss'] = pd.to_datetime(d['yyyy-mm-ddThh:mm:ss'])
    
    uw2.sort_values(by='yyyy-mm-ddThh:mm:ss',inplace=True,ignore_index=True)
    d.sort_values(by='yyyy-mm-ddThh:mm:ss',inplace=True,ignore_index=True)
    
    ### Some of our rows have duplicated datetimes. Each row should have an individual datetime based on sampling rates. This function drops rows containing secondary occurrences of duplicated datetimes. ###
    uw2 = dropDuplicateDatetimeRows(uw2, 'yyyy-mm-ddThh:mm:ss')
    d = dropDuplicateDatetimeRows(d, 'yyyy-mm-ddThh:mm:ss')

    ### For a few (gnats-only) cruises/dates, the cruise name and the recorded datetime do not agree. Drop the rows where they do not agree. ###
    uw2 = cruiseDatetimeAgreement(uw2, 'Cruise', 'yyyy-mm-ddThh:mm:ss')
    d = cruiseDatetimeAgreement(d, 'Cruise','yyyy-mm-ddThh:mm:ss')
    
    
#################################################### NOW WE ARE READY TO MERGE THE UNDERWAY AND DISCRETE FILES #######################################################
    ### Manually find underway datapoints with datetimes that nearest match discrete datapoints within 5 minutes. ###
    uw_nearest_dts = []
    d_nearest_dts = []
    d_unmatched_dts = []
    
    for element in d['yyyy-mm-ddThh:mm:ss']:
        delta = abs(uw2['yyyy-mm-ddThh:mm:ss'] - element)
        
        if delta.min() <= pd.Timedelta('5min'):
            uw_nearest_dts.append(uw2['yyyy-mm-ddThh:mm:ss'][delta.idxmin()])  #returns datetime value at the index where delta is a minimum
            d_nearest_dts.append(element)
        else:
            d_unmatched_dts.append(element)
            
         
    ### Create an extra discrete datetime column, that way when it gets merged with the underway data, it won't be dropped. ###
    d['yyyy-mm-ddThh:mm:ss_d'] = d['yyyy-mm-ddThh:mm:ss']

    #partition the uw and d dfs into 'nearest' and 'non-nearest/unmatched' subsets
    uw_nearest_df = uw2.loc[uw2['yyyy-mm-ddThh:mm:ss'].isin(uw_nearest_dts)]
    d_nearest_df = d.loc[d['yyyy-mm-ddThh:mm:ss'].isin(d_nearest_dts)]
    
    uw_unmatched_df = uw2.loc[~uw2['yyyy-mm-ddThh:mm:ss'].isin(uw_nearest_dts)]
    d_unmatched_df = d.loc[~d['yyyy-mm-ddThh:mm:ss'].isin(d_nearest_dts)]

    
    ### Merge the nearest dfs by merge_asof with direction nearest, merge the unmatched dfs normally, concat the two merged dfs and sort by datetime. ###
    nearestMerge = pd.merge_asof(uw_nearest_df, d_nearest_df, on='yyyy-mm-ddThh:mm:ss', direction='nearest', suffixes=('_uw','_d'),
                          allow_exact_matches=True)
    unmatchedMerge = pd.merge(uw_unmatched_df, d_unmatched_df, how='outer', on='yyyy-mm-ddThh:mm:ss', suffixes=('_uw','_d'))
    mergedFieldDf = pd.concat([nearestMerge, unmatchedMerge])
    mergedFieldDf.sort_values('yyyy-mm-ddThh:mm:ss', inplace=True, ignore_index=True)
    mergedFieldDf.rename(columns={'yyyy-mm-ddThh:mm:ss':'yyyy-mm-ddThh:mm:ss_uw'}, inplace=True)
    
    print('Number of rows in formatted uw df:', len(uw2))
    print('Number of rows in formatted discrete df:', len(d))
    print('Number of discrete rows merged to uw rows:', len(nearestMerge))
    print('We expect the final number of rows to be the number of uw rows(',len(uw2),') + total num of unmatched discrete rows, which is the total number of discrete rows(',len(d),') minus the number of discrete rows matched to uw rows(',len(nearestMerge),') =',len(uw2)+len(d)-len(nearestMerge),'.')
    print('Shape of the final merged field df:', mergedFieldDf.shape)
    
    ### Check to make sure the merge happened correctly. ###
    if len(uw_nearest_df) != len(d_nearest_df):
        parser.error('Problem with merge. Num rows of nearest subset of uw df does not match num rows of nearest subset of discrete df.  Most likely, there are duplicate datetimes in the uw or d df.')
    if len(uw2)+len(d)-len(nearestMerge) != len(mergedFieldDf):
        parser.error('Problem with merge.  The final field merged df is not the correct length.  It should have the same number of rows as the formatted uw df, + formatted discrete df, minus the merge row overlap found by the number of rows in the nearestMerge df.')

    ### Save the merged field dataframe. ###
    mergedFieldDf.to_csv(dict_args['ofile'][0], index=False)
   
    



def cruiseDatetimeAgreement(dataframe, cruiseColumnName, datetimeColumnName): #only call this function after the uw cruise strings whitespaceCheck has been called
    import pandas as pd
    import numpy as np
    
    dataframe[datetimeColumnName] = pd.to_datetime(dataframe[datetimeColumnName])
       
    for i in dataframe.index:
        if (dataframe[cruiseColumnName][i][0]=='s')&(dataframe[cruiseColumnName][i][-1] in ['e','w']):
            if dataframe[cruiseColumnName][i][1:3]!=str(dataframe[datetimeColumnName][i].year)[-2:]: #if the cruise name year and the datetime year don't agree, drop the row
                dataframe.drop(index=i, inplace=True)
    dataframe.reset_index(drop=True, inplace=True)
    return dataframe
        
        
def dropDuplicateDatetimeRows(dataframe, datetimeColumnName): #if there are duplicate datetimes, drop the rows of the non-first datetime occurrence
    import pandas as pd
    import numpy as np
    
    if len(dataframe[datetimeColumnName])!=len(dataframe[datetimeColumnName].unique()):
        dataframe.drop(index=dataframe[dataframe[datetimeColumnName].duplicated()==True].index, inplace=True)
    dataframe.reset_index(drop=True, inplace=True)
    return dataframe

if __name__ == "__main__": main()