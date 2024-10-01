import geopandas as gpd
import os
import dotenv

dotenv.load_dotenv()

servacc_cred = os.getenv('GEE_JSON')
service_account = os.getenv('SERVICE_ACC')

vpath = 'Data/hidrocl_boundaries.geojson'
dbpath = os.getenv('WB_PATH')
geojson = os.getenv('GEOJSON')

def configure_layers(option='old'):
    """Set layers to be used in the code

    Args:
        option (str, optional): 'old' or 'new'. Defaults to 'old'.

    Returns:
        list: list of waterbodies ids
        str: layer name
    """

    if option=='old':
        gdf = gpd.read_file("Data/waterbodies_chile.geojson",
                            driver='GeoJSON')
        ids = gdf.fid.to_list()

        lyr = "projects/global-water-watch/assets/reservoirs-all-v1-0"

    if option=='new':
        gdf = gpd.read_file(geojson,
                           driver='GeoJSON')
        ids = gdf.fid.to_list()

        lyr = "projects/ee-atapia/assets/waterbodies"

    return gdf, ids, lyr