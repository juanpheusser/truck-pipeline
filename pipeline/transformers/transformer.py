import pandas as pd
import numpy as np
import logging
from pipeline.common.meta_process import Meta
from datetime import datetime


class Transformer:

    def __init__(self, now: datetime, dtypes: dict, timeWindow: dict):

        self.dtypes = dtypes
        self.now = now
        self.timeWindow = {timeWindow['unit']: timeWindow['length']}
        self.cache = pd.DataFrame()
        self._logger = logging.getLogger(__name__)

    def transformData(self, data):

        df = pd.DataFrame(data)

        out_df = df.copy()

        for col, dtype in self.dtypes.items():

            try:
                if col == 'Fecha_gps':
                    out_df[col] = pd.to_datetime(df[col])
                else:
                    out_df[col] = df[col].astype(dtype)

            except:
                pass

        return out_df

    def filterByTimeWindow(self, data):
        
        timeThreshold = self.now - pd.Timedelta(**self.timeWindow)
        out_df = data[data['Fecha_gps'] >= timeThreshold]

        return out_df


    def filterByLastData(self, data, method = 'meta'):

        if (not self.cache.empty) and method == 'cache':
            old_df = self.cache.copy()
        else:
            old_df = Meta.readLatestMetaFile()

            if not old_df.empty:
                old_df = self.transformData(old_df)

            
        if not old_df.empty:
            data.loc[:, 'marker'] = 0
            old_df.loc[:, 'marker'] = 1
            df2 = pd.concat([data, old_df])
            df2.drop_duplicates(subset=['Patente', 'Id_evento', 'Fecha_gps'], keep=False, inplace=True)
            out_df = df2[df2.marker == 0]
            out_df.drop(['marker'], inplace=True, axis=1)
            
            return out_df

        else:
            return data


    def setCache(self, data):

        self.cache = data

    def filterData(self, data):

        d = data.copy()

        d = self.transformData(d)
        self.setCache(d)
        d = self.filterByTimeWindow(d)
        d = self.filterByLastData(d)

        return d











