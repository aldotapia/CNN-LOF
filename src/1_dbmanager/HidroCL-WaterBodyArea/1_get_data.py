import gc
import time

import ee
import pandas as pd
from eepackages.applications.waterbody_area import computeSurfaceWaterArea
from tqdm import tqdm

import config

# general conf
GS_OUTPUT_DIR = 'Output'
missing_only = False

# initialize using current user
servacc_cred = config.servacc_cred
service_account = config.service_account

# initialize using service account
credentials = ee.ServiceAccountCredentials(service_account, servacc_cred)
ee.Initialize(credentials)

# get today's date
today = pd.Timestamp.today().strftime('%Y-%m-%d')

# configure layers
_, ids, lyr = config.configure_layers(option='new')

start = pd.Timestamp.today().replace(day=1) - pd.DateOffset(months=1)
end = pd.Timestamp.today().replace(day=1)
period = [start, end]

# load water occurrence layer
waterOccurrence = (ee.Image("JRC/GSW1_3/GlobalSurfaceWater")
                   .select('occurrence')
                   .unmask(0)
                   .resample('bicubic')
                   .divide(100))

# mask water occurrence layer
waterOccurrence = (waterOccurrence.mask(waterOccurrence))


def get_time_series(waterbody):
    scale = waterbody.geometry().area().sqrt().divide(200).max(10).getInfo()

    start = EXPORT_TIME_START
    start_filter = EXPORT_TIME_START_FILTER
    # start_filter = start
    stop = EXPORT_TIME_STOP

    missions = ['L4', 'L5', 'L7', 'L8', 'S2']

    water_area = computeSurfaceWaterArea(waterbody, start_filter, start, stop, scale, waterOccurrence, missions)

    water_area = (water_area
                  .filter(
        ee.Filter.And(
            ee.Filter.neq('p', 101),
            ee.Filter.gt('ndwi_threshold', -0.15),
            ee.Filter.lt('ndwi_threshold', 0.5),
            ee.Filter.lt('filled_fraction', 0.6)
        )
    )
                  .sort('system:time_start')
                  )
    properties = ['MISSION', 'ndwi_threshold', 'quality_score', 'area_filled', 'filled_fraction', 'p',
                  'system:time_start', 'area']
    properties_new = ["mission", "ndwi_threshold", "quality_score", "water_area_filled", "water_area_filled_fraction",
                      "water_area_p", "water_area_time", "water_area_value"]

    water_area = ee.FeatureCollection(water_area).select(properties, properties_new, False).set('scale', scale)

    return water_area


def update_time_series(waterbody, date, use_task):
    filename = ee.Number(waterbody.get('fid')).format('%07d').getInfo().zfill(5)
    date = date.replace('-', '')[0:6]
    gs_path = f'{GS_OUTPUT_DIR}/{filename}_{date}'

    water_area = get_time_series(waterbody)

    if use_task:  # start task
        # always False for direct download
        pass

    else:  # write directly to the bucket
        water_area = water_area.getInfo()

        df = pd.DataFrame(list(map(lambda f: f['properties'], water_area['features'])))
        df.to_csv(gs_path + '.csv')


def get_number_of_running_tasks():
    statuses = ee.data.getTaskList()
    return len([s['state'] for s in statuses if s['state'] == 'READY' or s['state'] == 'RUNNING'])


def wait_for_running_tasks_to_complete():
    '''
    Watit until number of running tasks < 500
    '''

    n_running_tasks = get_number_of_running_tasks()

    while n_running_tasks > 500:
        time.sleep(600)  # 10 min
        n_running_tasks = get_number_of_running_tasks()


waterbodiesmain = ee.FeatureCollection(lyr)

for id in ids:
    gc.collect()
    print(f"Doing {id} {period[0].strftime('%Y-%m-%d')}")
    EXPORT_TIME_START_FILTER = period[0].strftime('%Y-%m-%d')
    EXPORT_TIME_START = period[0].strftime('%Y-%m-%d')
    EXPORT_TIME_STOP = period[1].strftime('%Y-%m-%d')
    waterbodies = ee.FeatureCollection(waterbodiesmain.filter(ee.Filter.eq('fid', id)))
    count = waterbodies.size().getInfo()
    start = 0
    offset = 0
    t_start = time.time()
    retry_count = 0
    use_task = False  # False for download directly
    missing_ids = []
    if missing_only:
        missing_ids = pd.read_csv('missing.csv').missing.values
    waterbody_ids = waterbodies.aggregate_array('fid').getInfo()
    while True:
        try:
            progress = tqdm(range(start, count), initial=start)
            for offset in progress:
                # every 500 tasks check if we need to wait before other tasks complete before starting new ones
                if offset % 500 == 0:
                    wait_for_running_tasks_to_complete()
                waterbody_id = waterbody_ids[offset]
                if missing_only:
                    if waterbody_id not in missing_ids:
                        progress.set_description(f'Skipping {waterbody_id}')
                        continue
                    waterbody = ee.Feature(waterbodies.toList(1, offset).get(0))
                    assert waterbody_id == ee.Number(waterbody.get('fid')).getInfo()
                    progress.set_description(f'Downloading {waterbody_id}')
                    update_time_series(waterbody, date=EXPORT_TIME_START, use_task=False)
                    # sys.exit()
                else:
                    waterbody = ee.Feature(waterbodies.toList(1, offset).get(0))
                    assert waterbody_id == ee.Number(waterbody.get('fid')).getInfo()
                    progress.set_description(f'Downloading {waterbody_id}')
                    update_time_series(waterbody, date=EXPORT_TIME_START, use_task=False)
                    t_end = time.time()
                    t_elapsed = str(t_end - t_start)
                    meta = {'elapsed': t_elapsed, 'retry_count': retry_count}
                    id_str = ee.Number(waterbody_id).format('%07d').getInfo().zfill(5)
                    filename = id_str + '.meta.json'
                    t_start = time.time()
        except Exception as e:
            retry_count = retry_count + 1
            retry_max = 30
            progress.set_description(f'Retrying {waterbody_id}, {retry_count} of {retry_max}')
            if retry_count > retry_max:
                t_end = time.time()
                t_elapsed = str(t_end - t_start)
                meta = {'elapsed': t_elapsed, 'retry_count': retry_count}
                filename = ee.Number(waterbody.get('fid')).format('%07d').getInfo().zfill(5) + '.meta.json'
                start = offset + 1
            else:
                start = offset  # retry
            print(e)
        if offset == count - 1:
            break
