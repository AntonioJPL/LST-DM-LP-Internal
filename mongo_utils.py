import pymongo
from bson.json_util import dumps, loads
from bson import ObjectId
import os
import glob
from datetime import datetime
import datetime as DT
import pytz
from django.contrib.staticfiles import finders

#Class containing all the Database information and functions
class MongoDb:
    my_client = pymongo.MongoClient('localhost', 27005)
    dbname = my_client['Drive-Monitoring']
    collection_logs = dbname["Logs"]
    collection_data = dbname["Data"]
    #Function that initialize the general data into MongoDB
    def __init__(self):
        self.dbname["CommandStatus"].insert_many([
            {"name": "command sent"},
            {"name": "in progress"},
            {"name": "Done received"},
            {"name": "action error"}
        ])
        self.dbname["LogStatus"].insert_many([
            {"name": "Finished"},
            {"name": "Stopped"},
            {"name": "Error"},
            {"name": "Unknown"}
        ])
        self.dbname["Types"].insert_many([
            {"name": "Track"},
            {"name": "Park-in"},
            {"name": "Park-out"},
            {"name": "GoToPos"}
        ])
        self.dbname["Commands"].insert_many([
            {"name": "StopDrive"},
            {"name": "Drive Regulation Parameters Azimuth"},
            {"name": "Drive Regulation Parameters Elevation"},
            {"name": "[Drive] Track start"},
            {"name": "Start Tracking"},
            {"name": "Park_Out"},
            {"name": "Park_In"},
            {"name": "GoToTelescopePosition"},
            {"name": "Start_Tracking"},
            {"name": "GoToPosition"}
        ])
    #INFO: All the store functions check if there is an index created and if not creates it. This is meant to happen just the first time. This is done right before trying to insert the data
    #Function that stores an operation entry
    def storeOperation(self, data):
        if len(self.dbname["Operations"].index_information()) == 1:
            self.dbname["Operations"].create_index([('Date', pymongo.ASCENDING), ("Tmin", pymongo.ASCENDING), ("Tmax", pymongo.ASCENDING)], unique=True)
        try:
            self.dbname["Operations"].insert_one(data)
        except Exception:
            print("Duplicated Operation entry on Date: "+data["Date"])
    #Function that stores a log entry
    def storeLogs(self, data):
        if data["LogStatus"] != None:
            statusId = self.dbname["LogStatus"].find_one({"name":data["LogStatus"]}, {"name": 0})
            data["LogStatus"] = str(statusId["_id"]) #Id value of the status

        commandId = self.dbname["Commands"].find_one({"name":data["Command"]}, {"name": 0})
        data["Command"] = str(commandId["_id"]) #Id value of the command
        
        if data["Status"] != None:
            commandStatusId = self.dbname["CommandStatus"].find_one({"name":data["Status"]}, {"name": 0})
            data["Status"] = str(commandStatusId["_id"]) #Id value of the commandstatus
        stringTime = data["Date"].replace("-", "/")+" "+data["Time"]
        timeStamp = datetime.strptime(stringTime, '%Y/%m/%d %H:%M:%S')
        timeStamp = timeStamp.timestamp()

        if len(self.dbname["Logs"].index_information()) == 1:
            self.dbname["Logs"].create_index([('Command', pymongo.ASCENDING), ("Date", pymongo.ASCENDING), ("LogStatus", pymongo.ASCENDING), ("Status", pymongo.ASCENDING), ("Time", pymongo.ASCENDING)], unique=True)

        try:
            self.dbname["Logs"].insert_one(data)
        except Exception as e:
            #print("error: "+str(e))
            pass
    #Function that stores a data entry
    def storeGeneralData(self, data):
        typeId = self.dbname["Types"].find_one({"name": data["type"]}, {"name": 0})
        data["type"] = str(typeId["_id"])
        if len(self.dbname["Data"].index_information()) == 1:
            self.dbname["Data"].create_index([('addText', pymongo.ASCENDING), ("DEC", pymongo.ASCENDING), ("Edate", pymongo.ASCENDING), ("Etime", pymongo.ASCENDING), ("file", pymongo.ASCENDING), ("RA", pymongo.ASCENDING), ("Sdate", pymongo.ASCENDING), ("Stime", pymongo.ASCENDING), ("type", pymongo.ASCENDING)], unique=True)
        try:
            self.dbname["Data"].insert_one(data)
            return True
        except Exception as e:
            #print("error: "+str(e))
            pass
    #Function that stores a position entry
    def storePosition(self, data):
        if len(self.dbname["Position"].index_information()) == 1:
            self.dbname["Position"].create_index([('T', pymongo.ASCENDING), ("Az", pymongo.ASCENDING), ("ZA", pymongo.ASCENDING)], unique=True)
        try:
            self.dbname["Position"].insert_many(data, False)
        except Exception as e:
            #print("error: "+str(e))
            pass
    #Function that stores a loadpin entry        
    def storeLoadPin(self, data):
        if len(self.dbname["Load_Pin"].index_information()) < 1:
            #The index is not being created? FIX
            self.dbname["Load_Pin"].create_index([('T', pymongo.ASCENDING), ("LoadPin", pymongo.ASCENDING), ("Load", pymongo.ASCENDING)], unique=True)
        try: 
            self.dbname["Load_Pin"].insert_many(data, False)
        except Exception as e:
            #print("error: "+str(e))
            pass
    #Function that stores a track entry
    def storeTrack(self, data):
        if len(self.dbname["Track"].index_information()) == 1:
            self.dbname["Track"].create_index([('T', pymongo.ASCENDING), ("Azth", pymongo.ASCENDING), ("ZAth", pymongo.ASCENDING), ("vsT0", pymongo.ASCENDING), ("Tth", pymongo.ASCENDING)], unique=True)
        try:
            self.dbname["Track"].insert_many(data, False)
        except Exception as e:
            #print("error: "+str(e))
            pass
    #Function that stores a torque entry               
    def storeTorque(self, data):
        if len(self.dbname["Torque"].index_information()) == 1:
            self.dbname["Torque"].create_index([('T', pymongo.ASCENDING), ("Az1_mean", pymongo.ASCENDING), ("Az1_min", pymongo.ASCENDING), ("Az1_max", pymongo.ASCENDING), ("Az2_mean", pymongo.ASCENDING), ("Az2_min", pymongo.ASCENDING), ("Az2_max", pymongo.ASCENDING), ("Az3_mean", pymongo.ASCENDING), ("Az3_min", pymongo.ASCENDING), ("Az3_max", pymongo.ASCENDING), ("Az4_mean", pymongo.ASCENDING), ("Az4_min", pymongo.ASCENDING), ("Az4_max", pymongo.ASCENDING), ("El1_mean", pymongo.ASCENDING), ("El1_min", pymongo.ASCENDING), ("El1_max", pymongo.ASCENDING), ("El2_mean", pymongo.ASCENDING), ("El2_min", pymongo.ASCENDING), ("El2_max", pymongo.ASCENDING)], unique=True)
        try:
            self.dbname["Torque"].insert_many(data, False)
        except Exception as e:
            #print("error: "+str(e))
            pass
    #Function that stores a accuracy entry    
    def storeAccuracy(self, data):
        if len(self.dbname["Accuracy"].index_information()) == 1:
            self.dbname["Accuracy"].create_index([('T', pymongo.ASCENDING), ("Azmean", pymongo.ASCENDING), ("Azmin", pymongo.ASCENDING), ("Azmax", pymongo.ASCENDING), ("Zdmean", pymongo.ASCENDING), ("Zdmin", pymongo.ASCENDING), ("Zdmax", pymongo.ASCENDING)], unique=True)
        try:
            self.dbname["Accuracy"].insert_many(data, False)
        except Exception as e:
            #print("error: "+str(e))
            pass
    #Function that stores a bend_model entry        
    def storeBendModel(self, data):
        if len(self.dbname["Bend_Model"].index_information()) == 1:
            self.dbname["Bend_Model"].create_index([('T', pymongo.ASCENDING), ("AzC", pymongo.ASCENDING), ("ZAC", pymongo.ASCENDING)], unique=True)
        try:
            self.dbname["Bend_Model"].insert_many(data, False)
        except Exception as e:
            #print("error: "+str(e))
            pass
    #Function that checks if the date passed is equal to the last date stored, in case it is it returns true, in case the database has no operations it returns Empty and in case the date is not equal it returns the last date stored, if there is an error it returns False. All of this is returned in an object with a "lastDate" attribute
    def checkDates(self, date):
        try:
            lastElementFound = list(self.dbname["Operations"].aggregate([{"$sort": {"Date": -1}}, {"$limit": 1}]))
        except Exception:
            return {"lastDate": False}
        if len(lastElementFound) > 0:
            lastElementFound = lastElementFound[0]
        else:
            return {"lastDate": "Empty"}
        if lastElementFound["Date"] == date:
            return {"lastDate": True}
        else:
            return {"lastDate": lastElementFound["Date"]}