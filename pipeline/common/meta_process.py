"""
Methods for processing the meta files
"""

import pandas as pd
import os
import numpy as np

class Meta:

    """
    Class for working with the meta file
    """

    def __init__(self, now):

        self.now = now

    @staticmethod
    def writeMetaFile(data, now):

        folderString = now.strftime('%Y-%m-%d')

        os.chdir('meta_files')

        try:
            os.chdir(folderString)
        except:
            os.mkdir(folderString)
            os.chdir(folderString)



        write_data = data[['Patente', 'Id_evento', 'Fecha_gps']]
        file_string = now.strftime('%Y-%m-%dT%H:%M:%S')
        file_string = ''.join([file_string, '.csv'])
        write_data.to_csv(file_string, mode='w', index=False)

        os.chdir('..')
        os.chdir('..')

    @staticmethod
    def readLatestMetaFile(fileIndex = 2):

        dates = [pd.to_datetime(name) for name in os.listdir('meta_files') if os.path.isdir(os.path.join('meta_files', name))]

        dates.append(pd.to_datetime('now') - pd.Timedelta(days=2))
        dates.sort()
        latestDate = dates[-1]

        folder = latestDate.strftime('%Y-%m-%d')

        listOfFiles = [file for file in os.listdir(os.path.join('meta_files', folder)) if file.endswith('.csv')]

        if len(listOfFiles) >= fileIndex:

            listOfTimestamps = [pd.to_datetime(file[:-4]) for file in listOfFiles]
            listOfTimestamps.sort()
            latestFile = ''.join([listOfTimestamps[-fileIndex].strftime('%Y-%m-%dT%H:%M:%S'), '.csv'])

            data = pd.read_csv(os.path.join('meta_files', folder, latestFile))

            return data

        else:

            return pd.DataFrame()

    @staticmethod
    def updatePlateMetafile(data):

        try:
            old_data = pd.read_csv('plates.csv')
            old_data = pd.Series(old_data)
        except:
            old_data = pd.Series([])

        plates = pd.Series(data["Patente"].unique())

        all_plates = pd.concat([plates, old_data])
        unique_plates = pd.Series(all_plates.unique())

        filepath = os.path.join('meta_files', 'plates.csv')

        unique_plates.to_csv(filepath, index=False)


