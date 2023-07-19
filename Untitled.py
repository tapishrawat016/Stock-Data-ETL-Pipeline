import os
from pathlib import Path
path = os.getcwd()
directory = f'{path}/Data/Parqueted_Data'
folder=f'Data/Transformed_Data'

for filename in os.listdir(folder):
            folder_path = os.path.join(folder, filename)
            for filename in os.listdir(folder_path):
                    file_path = os.path.join(folder_path, filename)
                    print((Path(file_path)))