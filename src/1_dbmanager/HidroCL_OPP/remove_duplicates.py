import pandas as pd
import os
import dotenv

dotenv.load_dotenv()

project_path = os.getenv('PROJECT_PATH')

def handle_duplicates(file):
    df = pd.read_csv(file, usecols=['date'])
    if df.duplicated(subset='date').any():
        df_name = file.split('/')[-1]
        print(f'File {df_name} has duplicated rows:')
        if not os.path.exists(f'backups/{today}'):
            os.makedirs(f'backups/{today}')
        df = pd.read_csv(file)
        df_name = f'{today}_{df_name}'
        df.to_csv(f'backups/{today}/{df_name}', index=False)
        print(df['date'][df.duplicated(subset='date')])
        df = df.drop_duplicates(subset='date')
        df.to_csv(file, index=False)

os.chdir(project_path)

today = pd.Timestamp.today().strftime('%Y%m%d')

def list_files(path):
    files = []
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv' in file:
                files.append(os.path.join(r, file))
    return files

dbs = list_files('databases')

dbs = [f for f in dbs if 'observed' in f or 'forecasted' in f]
dbs = [f for f in dbs if '._' not in f]
dbs = [f for f in dbs if f.endswith('.csv')]
dbs = [f for f in dbs if 'lulc' not in f]

for i in range(len(dbs)):
    print(dbs[i])
    print(handle_duplicates(dbs[i]))

dbs = list_files('pcdatabases')

dbs = [f for f in dbs if 'observed' in f or 'forecasted' in f]
dbs = [f for f in dbs if '._' not in f]
dbs = [f for f in dbs if f.endswith('.csv')]
dbs = [f for f in dbs if 'lulc' not in f]

for i in range(len(dbs)):
    print(dbs[i])
    print(handle_duplicates(dbs[i]))