# This script reads in the four satellite specific matchup dataframes and concats them together.

def main():
    
    import pandas as pd
    import numpy as np
    import argparse
    
    parser = argparse.ArgumentParser(description='''\
    This script reads in the satellite specific matchup dataframes and concats them into a single matchup dataframe. Note that this script requires two matchup dataframes and by default take in up to 6 matchup dataframes. If user has more tha 6 satellite specific matchup dataframes to merge together, user must use parser to add more arguments.''')
        
    parser.add_argument('--matchupDf1', nargs=1, type=str, required=True, help='''\
    Full path, name, and extension of the first satellite specific matchup dataframe.''')
    
    parser.add_argument('--matchupDf2', nargs=1, type=str, required=True, help='''\
    Full path, name, and extension of the second satellite specific matchup dataframe.''')
    
    parser.add_argument('--matchupDf3', nargs=1, type=str, required=False, help='''\
    Full path, name, and extension of the third satellite specific matchup dataframe.''')
    
    parser.add_argument('--matchupDf4', nargs=1, type=str, required=False, help='''\
    Full path, name, and extension of the fourth satellite specific matchup dataframe.''')
    
    parser.add_argument('--matchupDf5', nargs=1, type=str, required=False, help='''\
    Full path, name, and extension of the fifth satellite specific matchup dataframe.''')
    
    parser.add_argument('--matchupDf6', nargs=1, type=str, required=False, help='''\
    Full path, name, and extension of the sixth satellite specific matchup dataframe.''')
    
    parser.add_argument('--datetimeField', nargs=1, type=str, required=True, help='''\
    Name of datetime field/column.''')
    
    parser.add_argument('--ofile', nargs=1, type=str, required=True, help='''\
    Full path, name, and extension of where to save the concatted all-satellite-inclusive matchup dataframe.''')
    
    args = parser.parse_args()
    dict_args = vars(args)
    
    fp1 = dict_args['matchupDf1']
    fp2 = dict_args['matchupDf2']
    fp3 = dict_args['matchupDf3']
    fp4 = dict_args['matchupDf4']
    fp5 = dict_args['matchupDf5']
    fp6 = dict_args['matchupDf6']
    dt_col = dict_args['datetimeField'][0]
    ofilepath = dict_args['ofile'][0]
    
    filepaths = [fp1,fp2,fp3,fp4,fp5,fp6]
    
    matchup_dfs = []
    for fp in filepaths:
        if fp:
            matchup_dfs.append(pd.read_csv(fp[0]))
    
    merged_matchup_df = pd.concat(matchup_dfs)
    merged_matchup_df.sort_values(dt_col, inplace=True, ignore_index=True)
    
    merged_matchup_df.to_csv(ofilepath, index=False)
    
if __name__ == "__main__": main()