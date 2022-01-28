"""Connector and methods for accesing MongoDB"""

import os
import logging
from pymongo import MongoClient
from itertools import compress
import pandas as pd
from bson.objectid import ObjectId



class MongoConnector:

    """
    Class for interacting with MongoDB databases
    """

    def __init__(self, url: str, db: str, reportCollection: str, truckCollection: str):

        """
        Constructor for MongoDB Connector
        """

        #TODO parameter strings

        self._cluster = MongoClient(url)
        self._db = self._cluster[db]
        self.reportCollection = self._db[reportCollection]
        self.truckCollection = self._db[truckCollection]
        self._logger = logging.getLogger(__name__)

    def dataframeToDict(self, df: pd.DataFrame) -> dict:
        return df.to_dict(orient="records")

    def insertReports(self, reports: list):
        if reports:
            self.reportCollection.insert_many(reports)
 
    def insertTrucks(self, trucks: list):
        if trucks:
            res = self.truckCollection.insert_many(trucks)
            return res
        else:
            return []


    def filterReportsByTruckExistence(self, listOfReports, boolList):

        reportsOfExistingTrucks = list(compress(listOfReports, boolList))
        notBoolList = [not x for x in boolList]
        reportsOfUnexistingTrucks = list(compress(listOfReports, notBoolList))

        return reportsOfExistingTrucks, reportsOfUnexistingTrucks

    def truckExists(self, listOfReports):

        try:
            filepath = os.path.join('meta_files', 'plates.csv')
            plates = pd.read_csv(filepath, header=None)
            plates = plates.to_numpy()
            plates = plates.reshape((plates.shape[0]))
        except:
            plates = []


        def singleTruckExists(report):
            plate = report["Patente"]

            if plate in plates:
                return True
            else:
                return False


        boolList = list(map(singleTruckExists, listOfReports))

        return boolList

                             
    def singleTruckExists(self, report):

        plate = report["Patente"]

        truck = self.truckCollection.find_one({"Patente": plate})

        if truck is None:
            return False

        else:
            return True



    def updateTrucks(self, listOfReports):

        listOfIds = list(map(self.getTruckID, listOfReports))

        UpdatedTrucksIdReportTuple = [x for x in zip(listOfIds, listOfReports)]

        self.truckCollection.update_many({"_id": {"$in": listOfIds}}, {'$pop': {
            "Ignicion": 1,
            "Id_evento": 1,
            "Timestamp": 1,
            "Posicion": 1,
            "Altitud": 1,
            "Velocidad": 1,
            "Rumbo": 1
        }})

        r = list(map(self.updateSingleTruck, UpdatedTrucksIdReportTuple))

        return UpdatedTrucksIdReportTuple

    def updateSingleTruck(self, IdReportTuple):

        Id, report = IdReportTuple

        self.truckCollection.update_one({"_id": Id}, {"$push": {
            "Ignicion": {
                "$each": [report["Ignicion"]],
                "$position": 0
            },
            "Id_evento": {
                "$each": [report["Id_evento"]],
                "$position": 0
            },
            "Timestamp": {
                "$each": [report["Fecha_gps"]],
                "$position": 0
            },
            "Posicion": {
                "$each": [[report["Latitud"], report["Longitud"]]],
                "$position": 0
            },
            "Altitud": {
                "$each": [report["Altitud"]],
                "$position": 0
            },
            "Velocidad": {
                "$each": [report["Velocidad"]],
                "$position": 0
            },
            "Rumbo": {
                "$each": [report["Rumbo"]],
                "$position": 0
            },
            
        }})

        return 0

    
    def getTruckID(self, report):

        plate = report["Patente"]

        truck = self.truckCollection.find_one({"Patente": plate})

        if truck is None:
            return plate
        else:
            return truck["_id"]


    def buildSingleTruckSchema(self, truck):

        out_truck = {
            "Patente": truck["Patente"],
            "Ignicion": [truck["Ignicion"], None],
            "Id_evento": [truck["Id_evento"], None],
            "Timestamp": [truck["Fecha_gps"], None],
            "Posicion": [[truck["Latitud"], truck["Longitud"]], [None, None]],
            "Altitud": [truck["Altitud"], None],
            "Velocidad": [truck["Velocidad"], None],
            "Rumbo": [truck["Rumbo"], None]
        }

        return out_truck

    def buildTruckSchemas(self, listOfReports):

        listOfTrucks = list(map(self.buildSingleTruckSchema, listOfReports))

        return listOfTrucks

    def buildReportSchema(self, IdReportTuple):

        def buildSingleReportSchema(IdReportTuple):

            id, report = IdReportTuple

            outReport = {
                "TruckId": id,
                "Ignicion": report["Ignicion"],
                "Id_evento": report["Id_evento"],
                "Timestamp": report["Fecha_gps"],
                "Posicion": [report["Latitud"], report["Longitud"]],
                "Altitud": report["Altitud"],
                "Velocidad": report["Velocidad"],
                "Rumbo": report["Rumbo"]
            }

            return outReport

        if IdReportTuple:
            return list(map(buildSingleReportSchema, IdReportTuple))
        else:
            return []


    def pipelineFlow(self, df):

        if df.empty:
            pass

        else:

            listOfReports = self.dataframeToDict(df)
            boolList = self.truckExists(listOfReports)
            reportsOfExistingTrucks, reportsOfUnexistingTrucks = self.filterReportsByTruckExistence(listOfReports, boolList)

            listOfTruckSchemas = self.buildTruckSchemas(reportsOfUnexistingTrucks)
            resObj = self.insertTrucks(listOfTruckSchemas)

            NewTrucksIdReportTuple = []

            if listOfTruckSchemas:
                NewTrucksIdReportTuple = [x for x in zip(list(resObj.inserted_ids), reportsOfUnexistingTrucks)]

            UpdatedTrucksIdReportTuple = self.updateTrucks(reportsOfExistingTrucks)

            NewTrucksIdReportTuple.extend(UpdatedTrucksIdReportTuple)

            listOfReportSchemas = self.buildReportSchema(NewTrucksIdReportTuple)

            self.insertReports(listOfReportSchemas)









        


        


