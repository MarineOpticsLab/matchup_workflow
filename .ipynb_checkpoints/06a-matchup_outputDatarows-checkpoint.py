def main():

    import xarray as xr
    import numpy as np
    import pandas as pd
    import argparse


    parser = argparse.ArgumentParser(description='''\
      This program does matchups for a given station''')

    parser.add_argument('--id', nargs=1, type=str, required=True, help='''\
      id''')
    parser.add_argument('--granid', nargs=1, type=str, required=True)
    parser.add_argument('--fieldDf', nargs=1, type=str, required=True, help='''\
    Full path to formatted field csv file.''')
    parser.add_argument('--matchupDir', nargs=1, type=str, required=True, help='''\
    Full path to the directory in which the matchup datarows should be saved.''')
    parser.add_argument('--satDir', nargs=1, type=str, required=True, help='''\
    Full path to the directory where the satellite files are stored.''')
    parser.add_argument('--ofile_excludedMatchupLog', nargs=1, type=str, required=True, help='''\
    Full path and .txt extension of file in which to record matchups excluded due to satellite file import errors or 1km distance.''')

    args=parser.parse_args()
    dict_args=vars(args)

    # read in field df and locate the single row containing the unique id read in this iteration/row of the granule-links-full file
    field = pd.read_csv(dict_args['fieldDf'][0])
    field.drop(columns='Unnamed: 0', inplace=True)
    datarow = field.loc[field['ID']==dict_args['id'][0]].reset_index(drop=True)
    #maybe put a check here to ensure we only pulled out one row, not more than one
    datarow['granid'] = dict_args['granid'][0]


    outputdir = dict_args['matchupDir'][0]
    satdir = dict_args['satDir'][0]

    granid = datarow.granid[0]
    lat_gnats = datarow.Latitude_uw[0]
    lon_gnats = datarow.Longitude_uw[0]

    satfiledir = sat_filepath(granid, satdir)
   

    try:
        satData, satNav = import_satfile(satfiledir)
    except (FileNotFoundError, KeyError, AttributeError, OSError):
        print('File import error. Granid: ', granid)
        #print(satfiledir)
        file = open(dict_args['ofile_excludedMatchupLog'][0], 'a+')
        file.write(datarow.ID[0]+','+granid+','+'FIE \n')
        file.close()

    else:
        lat_sat, lon_sat = sat_lon_lat(satNav)

        dist_array = haversine(lon_gnats, lat_gnats, lon_sat, lat_sat)

        num_rows = dist_array.shape[0]
        num_cols = dist_array.shape[1]
        pixel_grid_row_max = num_rows-3
        pixel_grid_col_max = num_cols-3
        
        #check to make sure sat nav is not entirely nan:
        try:
            idx = np.nanargmin(dist_array.values)
        except (ValueError):
            print('Value Error. SatNav contains only nans. Granid: ', granid)
            file = open(dict_args['ofile_excludedMatchupLog'][0], 'a+')
            file.write(datarow.ID[0]+','+granid+','+'Nav \n')
            file.close()

        else:
            row, col, idx, min_dist = pixel_location(dist_array)
            
            if min_dist<=1: #limit matchups by 1km distance

                grid_idx, location_flag = loc_flag(min_dist, row, col, num_rows, num_cols)

                variable_dict = {'ID':datarow.ID[0]}
                variable_dict['Pixel_row'] = row
                variable_dict['Pixel_col'] = col
                variable_dict['Pixel_idx'] = idx

                for var_name in satData.data_vars:
                    if var_name == 'l2_flags':
                        continue

                    var_data = satData[var_name]  #Are SST and TOA RRS stored in satData.data_vars?
                    num_nans, variable_grid = grid_nans(var_data, grid_idx) 

                    num_grid_elem = np.size(variable_grid)  

                    var_flag = variable_flag(num_nans.values, num_grid_elem)
                    mean, stdev, median = grid_stats(variable_grid, var_flag)

                    filtered_pixels = filter_pixels(variable_grid, mean, stdev)
                    filtered_mean, filtered_stdev, filtered_pixel_count = filtered_stats(filtered_pixels)

                    variable_dict[var_name + '_mean'] = mean
                    variable_dict[var_name + '_stdev'] = stdev
                    variable_dict[var_name + '_median'] = median

                    variable_dict[var_name + '_filtered_mean'] = filtered_mean
                    variable_dict[var_name + '_filtered_stdev'] = filtered_stdev

                    variable_dict[var_name + '_grid_size'] = num_grid_elem
                    variable_dict[var_name + '_valid_pixel_count'] = num_grid_elem - num_nans.values
                    variable_dict[var_name + '_filtered_pixel_count'] = filtered_pixel_count

                    variable_dict[var_name + '_flag'] = var_flag

                variable_dict['Location_Flag'] = location_flag
                var_row = pd.DataFrame([variable_dict])
                compiled_row = datarow.merge(var_row, how = 'outer')
                compiled_row.to_csv(outputdir + '/' + datarow.ID[0] + '_' + granid + '.csv',index = False)
            else:
                print('>1km: ID:', datarow.ID[0], 'Granid:', granid)
                file = open(dict_args['ofile_excludedMatchupLog'][0], 'a+')
                file.write(datarow.ID[0]+','+granid+','+'1km \n')
                file.close()
                


def sat_filepath(granid, filepath_starter):
    sat = granid[0]
    year = granid[1:5]
    doy = granid[5:8]
    satLUT = {'S' : 'seawifs', 'A' : 'aqua', 'T' : 'terra', 'V' : 'viirs'}
    satfiledir = filepath_starter + '/' + satLUT[sat]+'/'+year+'/'+doy+'/'+granid+'.L2'
    return satfiledir

def import_satfile(satfiledir):
    import xarray as xr
    satData = xr.load_dataset(satfiledir, group='geophysical_data')
    satNav = xr.load_dataset(satfiledir, group='navigation_data')
    return satData, satNav

def sat_lon_lat(satNav):
    import xarray as xr
    lat_sat = satNav.latitude
    lon_sat = satNav.longitude
    return lat_sat, lon_sat

def haversine(lon1, lat1, lon2, lat2):
    import numpy as np
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a)) 
    dist_array = 6367 * c
    return dist_array  #units in kilometers

def pixel_location(dist_array):
    import numpy as np   
    idx = np.nanargmin(dist_array.values)
    min_dist = np.nanmin(dist_array.values)
    row, col = np.unravel_index(idx, dist_array.shape)
    return row, col, idx, min_dist

def pixel_grid(row,col):
    grid_idx = [row-2, row+3, col-2, col+3]
    return grid_idx

def pixel_side_grid(row, col, num_rows, num_cols):
    last_row_idx = num_rows -1
    last_col_idx = num_cols -1
    
    if row <2:
        row_min = 0
    else:
        row_min = row -2
    
    if row > num_rows -3:
        row_max = num_rows  #careful on indexing
    else:
        row_max = row + 3
    
    if col < 2:
        col_min = 0
    else: 
        col_min = col - 2
    
    if col > num_cols -3:
        col_max = num_cols
    else:
        col_max = col + 3
        
    grid_idx = [row_min, row_max, col_min, col_max]

    return grid_idx

def loc_flag(min_dist, row, col, num_rows, num_cols):
    location_flag = 0
    
    if min_dist>1:
        location_flag = 1
    if row<2 or row>num_rows-3 or col<2 or col>num_cols-3:
        location_flag = 2
        grid_idx = pixel_side_grid(row, col, num_rows, num_cols)
        if min_dist>1:
            location_flag = 3
    else:
        grid_idx = pixel_grid(row, col)
    
    return grid_idx, location_flag

def grid_nans(satData_column, grid_idx):
    import numpy as np
    variable_grid = satData_column[grid_idx[0]:grid_idx[1], grid_idx[2]:grid_idx[3]]
    num_nans = np.sum(np.isnan(variable_grid))
    return num_nans, variable_grid

def variable_flag(num_nans_values, num_grid_elem):  #careful of num_nans formatting, do I need num_nans.values
    
    if num_nans_values == num_grid_elem:
        var_flag = 1    
    elif num_nans_values >= num_grid_elem/2:
        var_flag = 2    
    else:
        var_flag = 0
        
    return var_flag

def grid_stats(variable_grid, var_flag):
    import numpy as np
    if var_flag == 1:
        mean, stdev, median = np.nan, np.nan, np.nan
    else: 
        mean = np.nanmean(variable_grid)
        stdev = np.nanstd(variable_grid)
        median = np.nanmedian(variable_grid)
    return mean, stdev, median

def filter_pixels(variable_grid, mean, stdev): # how will this deal with nans???
    import numpy as np
    import pandas as pd
    filtered_pixels = []
    lower_bound = 1.5*stdev - mean
    upper_bound = 1.5*stdev + mean
    flattened_var_grid = variable_grid.values.flatten()
    
    for element in flattened_var_grid:
        if np.isnan(element)==True:
            continue
        if lower_bound < element < upper_bound:
            filtered_pixels.append(element)
    return filtered_pixels

def filtered_stats(filtered_pixels):
    import numpy as np
    if len(filtered_pixels)>0:
        filtered_mean = np.mean(filtered_pixels)
        filtered_stdev = np.std(filtered_pixels)
        filtered_pixel_count = len(filtered_pixels)
    else:
        filtered_mean = np.nan
        filtered_stdev = np.nan
        filtered_pixel_count = 0
    return filtered_mean, filtered_stdev, filtered_pixel_count

if __name__ == "__main__": main()