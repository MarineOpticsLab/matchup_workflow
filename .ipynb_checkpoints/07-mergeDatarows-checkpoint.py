def main():

    import pandas as pd
    import numpy as np
    import os
    from os import listdir
    import argparse
    
    parser = argparse.ArgumentParser(description='''\
    This script reads in the individual datarows saved in the matchup directory. It merges the datarows together into one dataframe.''')
    
    parser.add_argument('--matchupDirectory', nargs=1, type=str, required=True, help='''\
    Full path and name of matchup directory where individual datarows are saved. Do NOT include trailing slash.''')
    
    parser.add_argument('--ofile_matchupDf', nargs=1, type=str, required=True, help='''\
    Full path and name of where to save the matchup dataframe--the output of this script. Include .csv extension.''')
    
    args = parser.parse_args()
    dict_args = vars(args)

    matchupDirPath = dict_args['matchupDirectory'][0]
    fnames = listdir(path = matchupDirPath)
    fpaths = [matchupDirPath + '/' + fileName for fileName in fnames]

    matchupDf = pd.read_csv(fpaths[0])
    ##################################################################################
    # Note that this concat command takes 5.5 hours to run. In future, we should consider other less memory-intensive methods. Supposedly turning each row into a dictionary, and then pd.DataFrame(Dict) is the fastest, least memory intensive method.
    
    for path in fpaths[1:]:
        matchupDf = pd.concat([matchupDf,pd.read_csv(path)])
        
    ##################################################################################
        
    matchupDf.to_csv(dict_args['ofile_matchupDf'][0], index=False)
    
if __name__ == "__main__": main()
