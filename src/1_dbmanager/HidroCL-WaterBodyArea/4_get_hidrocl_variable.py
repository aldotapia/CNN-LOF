import geopandas as gpd
import pandas as pd
import config

v = gpd.read_file(config.vpath)
reservoirs, _, _ = config.configure_layers(option='new')
df = pd.read_csv('Cleaned/reservoirs.csv')
out_path = config.dbpath

def create_df(v, field):
    """
    Create a pandas dataframe with the date range of the vector features

    Args:
        v (geopandas dataframe): vector features
        field (str): field name

    Returns:
        pandas dataframe: dataframe with the date range of the vector features
    """
    l = v[field].values.tolist()
    l.insert(0, 'date')
    return l
    

def get_reservoirs_inside(v, reservoirs):
    """
    Get reservoirs inside vector features

    Args:
        v (geopandas dataframe): vector features
        reservoirs (geopandas dataframe): reservoir features

    Returns:
        list: list of dictionaries with v_id and reservoirs inside
    """
    # create an empty list for storing the reservoirs inside by v feature
    reservoirs_inside = []
    for i in range(len(v)):
        # get the v feature
        vtemp = v.iloc[i]
        # get the reservoirs inside
        reservoirs_inside_temp = reservoirs[reservoirs.geometry.within(vtemp.geometry)]
        # get the id of the reservoirs inside
        reservoirs_inside_temp = reservoirs_inside_temp['fid'].tolist()
        out  = {'v_id': vtemp['gauge_id'],
                'reservoirs_inside': reservoirs_inside_temp}
        reservoirs_inside.append(out)
    return reservoirs_inside

reservoirs_inside = get_reservoirs_inside(v, reservoirs)

def get_reservoir_data(reservoirs_inside, df, idx):
    """
    Get reservoirs data according to HidroCL vector features

    Args:
        reservoirs_inside (list): list of dictionaries with v_id and reservoirs inside
        df (pandas dataframe): reservoirs data
        idx (pandas dataframe): index

    Returns:
        pandas dataframe: reservoirs data
    """
    reservoirs_data = []
    for i in range(len(reservoirs_inside)):
        # get ids
        ids = reservoirs_inside[i].get('reservoirs_inside')
        if len(ids) > 0:
            # zfill to 7 characters, depends on ID format
            ids = [str(i).zfill(7) for i in ids]
            # get data
            data = df[ids]
            # get sum of rows
            data = (data.sum(axis=1)/1000000).round(2).to_list()
            # get v_id
            v_id = reservoirs_inside[i].get('v_id')
            dft = pd.DataFrame({f'{v_id}':data}, index=idx)
        else:
            v_id = reservoirs_inside[i].get('v_id')
            dft = pd.DataFrame({f'{v_id}':0}, index=idx)
        reservoirs_data.append(dft)
    # concat
    reservoirs_data = pd.concat(reservoirs_data, axis=1)
    return reservoirs_data

reservoirs_data = get_reservoir_data(reservoirs_inside, df, idx=df.date)

reservoirs_data.to_csv(out_path)
