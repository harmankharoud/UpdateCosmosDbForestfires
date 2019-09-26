import logging
import os
import json  
import csv
import azure.functions as func
import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

class ModifyFile:
    def __init__(self, fileName=os.environ["downloading_filename"]):
        self.fileName = fileName

    def convertCSVtoJSON(self):
        currentDirectoryPath = os.path.dirname(os.path.realpath(__file__))
        csvFilePath = currentDirectoryPath.replace("/home/site/wwwroot/UpdateForestFiresDb", self.fileName + ".txt")
        f = open(csvFilePath, 'rU')
        reader = csv.DictReader( f, fieldnames = ( "latitude","longitude","bright_ti4","scan","track","acq_date","acq_time","satellite","confidence","version","bright_ti5","frp","daynight" ))  
        next(reader, None)
        out = json.dumps( [ self.forestFireDataStructure(row) for row in reader ] )  
        logging.info("JSON parsed!")  
        jsonFilePath = currentDirectoryPath.replace("/home/site/wwwroot/UpdateForestFiresDb", self.fileName + ".json")
        f = open(jsonFilePath, 'w')  
        f.write(out)
        logging.info("JSON saved!")
        os.remove(csvFilePath)
        return jsonFilePath
    
    def tryFloat(self, data):
        try: 
            return float(data)
        except:
            return data

    
    def forestFireDataStructure(self, data):
        return {
            "latitude": self.tryFloat(data["latitude"]),
            "longitude": self.tryFloat(data["longitude"]),
            "bright_ti4": self.tryFloat(data["bright_ti4"]),
            "scan": self.tryFloat(data["scan"]),
            "track": self.tryFloat(data["track"]),
            "acq_date": data["acq_date"],
            "acq_time": data["acq_time"],
            "satellite": data["satellite"],
            "confidence": data["confidence"],
            "version": data["version"],
            "bright_ti5": self.tryFloat(data["bright_ti5"]),
            "frp": self.tryFloat(data["frp"]),
            "daynight": data["daynight"]
        }

class GetNRTDataFromDefinedSat:
    #intilize fetch with three default parameters
    #country can be any of the list from NRT site
    #how many past day do want the data from.
    #sattelite: 1) virrs(default), 2) c6
    def __init__(self, headers={"Authorization": os.environ["nrt_Auth_Token"], "Content-Type": "text/plain;charset=UTF-8"},
                    countryName="Canada", noOfDays=7, satteliteName="viirs"):
        self.headers = headers
        self.noOfDays = noOfDays
        self.downloadUrl = "https://nrt4.modaps.eosdis.nasa.gov/api/v2/content/archives/FIRMS/" + satteliteName + "/" + countryName
        self.savedFilesInAchive = []

    #get the list of file names and urls available in the archives
    def getListofFiles(self):
        response = self.downloadFromUrl("")
        files = re.findall("(?<=txt\">).*?(?=</a>)", response)
        for file in files:
            #keep the begining slash with file name so that download fucntion can be reused
            fileNameAtIndex = file.rfind("/")
            self.savedFilesInAchive.append(file[fileNameAtIndex:])

    def downloadFromUrl(self, fileName):
        reqUrl = self.downloadUrl + fileName
        logging.info("Get data from file: " + reqUrl)
        response = requests.get(reqUrl, stream=True, headers=self.headers)
        logging.info("Got response: ", )
        return response.text

    def updateCSVfile(self, data, fileName= os.environ["downloading_filename"] + ".txt"):
        with open(fileName, "a+") as f:
            f.write(data)
            return "Got data for this file: " + fileName

    
    def getDataFromArchives(self):
        #get data from server.
        filesToDownload = self.savedFilesInAchive[len(self.savedFilesInAchive) - self.noOfDays:]
        #need to acquire twice the number of threads so that we can download file and save to file.
        with ThreadPoolExecutor(max_workers= self.noOfDays * 2) as executor:
            futures = [executor.submit(self.downloadFromUrl, filename) for filename in filesToDownload]
            ensureDataWritentoFile = [executor.submit(self.updateCSVfile, result._result) for result in as_completed(futures)]
            logging.info("ALL TASKS COMPLETED")
            modifyFile = ModifyFile()
            return modifyFile.convertCSVtoJSON()
    
    def removeContainer(self):
        try:
            return requests.delete(os.environ["cosmos_collection_delete"])
        except:
            return "Collection doesn't exists."

def main(mytimer: func.TimerRequest, outputDocument:func.Out[func.Document]) -> None:
    getNRTData = GetNRTDataFromDefinedSat()
    logging.info("Delete container")
    deleteResponse = getNRTData.removeContainer()
    logging.info(deleteResponse)
    getNRTData.getListofFiles()
    jsonFilePath = getNRTData.getDataFromArchives()
    if os.path.isfile(jsonFilePath):
        logging.info("Database update STARTED!")
        newDocs = func.DocumentList()
        modifyFile = ModifyFile()
        with open(jsonFilePath, "r") as f:
            datastore = json.load(f)
            for data in datastore:
                newdoc_dict = modifyFile.forestFireDataStructure(data)
                newDocs.append(func.Document.from_dict(newdoc_dict))
        outputDocument.set(newDocs)
        os.remove(jsonFilePath)
        logging.info("Database update DONE!")
