import shutil

def getDeployNames(database, schema, name) -> dict:
    stage = f'{database}.{schema}.deployments'
    path = f'/{name.lower()}/app.zip'
    directory = f'/{name.lower()}'
    return { 
        'stage': stage, 
        'path': path, 
        'full_path': f'@{stage}{path}',
        'directory': directory 
        }

# create a temporary directory, copy the file_path to it and rename to app.zip
def prepareAppZip(file_path, temp_dir) -> str:
    temp_path = temp_dir + '/app.zip'
    shutil.copy(file_path, temp_path)
    return temp_path
