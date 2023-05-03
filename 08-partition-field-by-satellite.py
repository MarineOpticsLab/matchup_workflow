def main():
    
    import argparse
    import pandas as pd
    import numpy as np
    
    
    parser = argparse.ArgumentParser(description='''\
      This script partitions the field dataframe by satellite matchups. Note that this script is based on 6 satellites only: \
      Seawifs, Aqua, Terra, Viirssnpp, Viirsjp1 and viirsjp2. If user desires other satellite, user must edit this script.''')
    
    parser.add_argument('--fieldFile', nargs=1, type=str, required=True, help='''\
    Full path, name, and extension of field datafile containing field ids.''')
    
    parser.add_argument('--idField', nargs=1, type=str, required=True, help='''\
    Give the name of the field/column denoting the field station/id in the field dataframe.''')

    parser.add_argument('--granlinksFile', nargs=1, type=str, required=True, help='''\
    Full path of input file that contains the granule info matched up to the corresponding field ids and locations. ''')
    
    parser.add_argument('--ofile_base_name', nargs=1, type=str, required=True, help='''\
    File path and name (excluding extension) for the output files. The satellite name will be appended to this filepath \
      and base name.''')

    args=parser.parse_args()
    dict_args=vars(args)
    
    field_fp = dict_args['fieldFile'][0]
    id_col = dict_args['idField'][0]
    granfile_fp = dict_args['granlinksFile'][0]
    ofile_base = dict_args['ofile_base_name'][0]
    
    # Read in data files:
    field = pd.read_csv(field_fp)
    granfile = pd.read_csv(granfile_fp, names=['station','granid','granurl','wlon','slat','elon','nlat'])
    
    # Per Satellite, generate a list of stations in the l2file. Then find the corresponding rows in the field dataframe.
    satellite_names = {'S':'seawifs','A':'aqua','T':'terra','VS':'snpp','V1J':'jpss1','V2J':'jpss2'}
    
    for key in satellite_names:
        sat_stations = granfile.loc[granfile['granid'].str[0:-13]==key, 'station']
        sat_df = field.loc[field[id_col].isin(sat_stations)]
        sat_df.to_csv(ofile_base+'-'+satellite_names[key]+'.csv', index=False)
        
if __name__ == "__main__": main()