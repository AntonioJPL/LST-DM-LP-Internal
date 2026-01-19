from datetime import datetime,timedelta,date,timedelta
import time as time
from time import strftime
import os, sys
import json
import pytz
#from datetime import timezone
from operator import itemgetter
import pandas as pd
import numpy as np
import requests
import asyncio
from mongo_utils import MongoDb
#from IP_Info import IP
from dotenv import load_dotenv
from pytz import UTC
from scipy.optimize import curve_fit


#General
load_dotenv()  # take environment variables from .env

IP = os.environ.get('EXT_SERVER_IP')
operationTimes = []
generallog = []
generalData = []
generalTypes = {
    "1" : "Track",
    "2" : "Park-out",
    "3" : "Park-in",
    "4" : "GoToPos"
}
selectedType = 0
#Used on GetAllDate function, normally recieves the filename variable and a string containing the text we are searching. Returns xbeg value
def getDate(filename,cmdstring):
    f = open(filename, "r") 
    xbeg=[]
    xbeg.clear()
    for line in f.readlines():
        if line.find(cmdstring) != -1:
            val =line.split(' ')
            #These are ANSI color codes being removed
            stringtime = val[0].replace("\x1b[32;1m","").replace("\x1b[m","").replace("\x1b[35;1m","").replace("\x1b[31;1m","")+ " " + val[1]
            stringtime = stringtime + ""
            begtime = datetime.strptime(stringtime,'%d/%m/%y %H:%M:%S')
            # add proper timezone
            pst = pytz.timezone('UTC')
            begtime = pst.localize(begtime)
            begtimes = begtime.timestamp()
            xbeg.append(begtimes)
            generallog.append([begtime,cmdstring])
    return xbeg
#Used in getAllDate function, recieves the filename and a string containing the text we are searching. Returns 3 values: ra, dec and radetime
def getRADec(filename,cmdstring):
    f = open(filename, "r") 
    ra=[]
    dec=[]
    radectime=[]
    for line in f.readlines():
        if line.find(cmdstring) != -1:
            val =line.split(' ')
            stringtime = val[0].replace("\x1b[32;1m","").replace("\x1b[m","").replace("\x1b[35;1m","").replace("\x1b[31;1m","")+ " " + val[1]
            stringtime = stringtime + ""
            begtime = datetime.strptime(stringtime,'%d/%m/%y %H:%M:%S')
            # add proper timezone
            pst = pytz.timezone('UTC')
            begtime = pst.localize(begtime)
            begtimes = begtime.timestamp()
            radectime.append(begtimes)
            for vali in val:
                if vali.find("RA=") != -1:
                    ra.append(float(vali[(vali.find('=')+1):(vali.find('['))])) #Gets the string value between the position 3 and 10 (RA value) and parses it into a float
                if vali.find("Dec=") != -1:
                    dec.append(float(vali[(vali.find('=')+1):(vali.find('['))]))
    return ra,dec,radectime
#Used in getAllDate function, recieves the filename and a string containing the text we are searching. Returns xbeg value val is teh array of strings in the found line. It works as getDate but the string structure is different so it needs to be treated in other way
def getDateTrack(filename,cmdstring):
    f = open(filename, "r") 
    xbeg=[]
    xbeg.clear()
    for line in f.readlines():
        if line.find(cmdstring) != -1:
            val =line.split(' ')
            #print(val)
            stringtime = val[6] + " " + val[7]
            stringtime = stringtime + ""
            begtime = datetime.strptime(stringtime,'%Y-%m-%d %H:%M:%S')
            pst = pytz.timezone('UTC')
            begtime = pst.localize(begtime)
            begtimes = begtime.timestamp()
            xbeg.append(begtimes)
    return xbeg
#Used in GenerateFig function. Returns df value which is a pandas.DataFrame Object containing the date, ra, dec between the given tmin and tmax values in the DrivePosition file 
def getPos(filename,tmin,tmax):    
    try:
        filenameBM = filename.replace('DrivePosition','BendingModelCorrection')
    except:
        print('%s not existing'%(filename))
        return None
    df = pd.read_csv(filename,sep=' ',header=None)
    df.columns=['T','Az','ZA']
    masktmin = df['T']>tmin
    masktmax = df['T']<tmax
    maskT = np.logical_and(masktmin,masktmax)
    df = df[maskT] #This is weird because maskT value should be true or false
    maskT1 = df['T']<1605657600 #2020/11/18 00:00:00
    maskt2 = df['T']>1611057600 #2021/01/19 12:00:00
    maskt3 = df['T']<1615161600 #2021/03/08 00:00:00
    maskT2 = np.logical_and(maskt2,maskt3)
    df['T'] = df['T'] + maskT1*-2 + maskT2*-2 
    df['T'] = df['T'].apply(lambda d: datetime.fromtimestamp(d, tz=pytz.utc))
    df_dict = df.to_dict('records')
    for rows in df_dict:
        rows["T"] = str(rows["T"].timestamp()).replace(".", "")
        rows["T"] = int(rows["T"].ljust(2+len(rows["T"]), '0'))
    MongoDb.storePosition(MongoDb, df_dict)
#Used in GenerateFig.  
def getBM(filename,tmin,tmax):    
    dfBM = pd.read_csv(filename,sep=' ',header=None)
    dfBM.columns=['T','AzC','ZAC']
    masktmin = dfBM['T']>tmin
    masktmax = dfBM['T']<tmax
    maskT = np.logical_and(masktmin,masktmax)
    dfBM = dfBM[maskT]
    maskT1 = dfBM['T']<1605657600
    maskt2 = dfBM['T']>1611057600
    maskt3 = dfBM['T']<1615161600
    maskT2 = np.logical_and(maskt2,maskt3)
    dfBM['T'] = dfBM['T'] + maskT1*-2 + maskT2*-2
    df_dict = dfBM.to_dict('records')
    MongoDb.storeBendModel(MongoDb, df_dict)      
def getPrecision(filename,tmin,tmax):    
    df = pd.read_csv(filename,sep=' ',header=None)
    df.columns = ['T','Azmean','Azmin','Azmax','Zdmean','Zdmin','Zdmax']
    masktmin = df['T']>tmin
    masktmax = df['T']<tmax
    maskT = np.logical_and(masktmin,masktmax)
    df = df[maskT]
    maskT1 = df['T']<1605657600
    maskt2 = df['T']>1611057600
    maskt3 = df['T']<1615161600
    maskT2 = np.logical_and(maskt2,maskt3)
    df['T'] = df['T'] + maskT1*-2 + maskT2*-2
    df['T'] = df['T'].apply(lambda d: datetime.fromtimestamp(d, tz=pytz.utc))
    df['Azmean'] = df['Azmean'] * 3600
    df['Azmin'] = df['Azmin'] * 3600
    df['Azmax'] = df['Azmax'] * 3600
    df['Zdmean'] = df['Zdmean'] * 3600
    df['Zdmin'] = df['Zdmin'] * 3600
    df['Zdmax'] = df['Zdmax'] * 3600
    mask0_1 = df['Azmean']!=0.
    mask0_2 = df['Zdmean']!=0. #This is why data is being ignored
    mask0 = np.logical_and(mask0_2,mask0_1)
    df = df[mask0]
    df_dict = df.to_dict('records')
    for rows in df_dict:
        rows["T"] = str(rows["T"].timestamp()).replace(".", "")
        rows["T"] = int(rows["T"].ljust(2+len(rows["T"]), '0'))
    MongoDb.storeAccuracy(MongoDb, df_dict)
#Works as getDate but returns date and line of the found cmdstring
def getDateAndLine(filename,cmdstring):
    f = open(filename, "r") 
    xbeg=[]
    xbeg.clear()
    lineout=[]
    lineout.clear()
    for line in f.readlines():
        if line.find(cmdstring) != -1:
            val =line.split(' ')
            stringtime = val[0].replace("\x1b[32;1m","").replace("\x1b[m","").replace("\x1b[35;1m","").replace("\x1b[31;1m","")+ " " + val[1]
            stringtime = stringtime + ""
            begtime = datetime.strptime(stringtime,'%d/%m/%y %H:%M:%S')
            # add proper timezone
            pst = pytz.timezone('UTC')
            begtime = pst.localize(begtime)
            begtimes = begtime.timestamp()
            xbeg.append(begtimes)
            lineout.append(line)
            print("Found %s %s %s"%(cmdstring,begtime,begtimes))
    return xbeg,lineout
def getTorqueNew(filename,tmin,tmax):    
    df = pd.read_csv(filename,sep=' ',header=None)
    df.columns=['T','Az1_mean','Az1_min','Az1_max','Az2_mean','Az2_min','Az2_max','Az3_mean','Az3_min','Az3_max','Az4_mean','Az4_min','Az4_max','El1_mean','El1_min','El1_max','El2_mean','El2_min','El2_max']
    masktmin = df['T']>tmin
    masktmax = df['T']<tmax
    maskT = np.logical_and(masktmin,masktmax)
    df = df[maskT]
    maskT1 = df['T']<1605657600
    maskt2 = df['T']>1611057600
    maskt3 = df['T']<1615161600
    maskT2 = np.logical_and(maskt2,maskt3)
    df['T'] = df['T'] + maskT1*-2 + maskT2*-2
    df['T'] = df['T'].apply(lambda d: datetime.fromtimestamp(d, tz=pytz.utc))
    df_dict = df.to_dict('records')
    for rows in df_dict:
        rows["T"] = str(rows["T"].timestamp()).replace(".", "")
        rows["T"] = int(rows["T"].ljust(2+len(rows["T"]), '0'))
    MongoDb.storeTorque(MongoDb, df_dict)
##### READ TRACK VALUES
def getTrackNew(filename3,tmin,tmax):
    print("getTrack %s %s %s"%(filename3,tmin,tmax))
    try:
        df = pd.read_csv(filename3,sep=' ',header=None)
    except:
        print("%s not existing"%(filename3))
        return None
    df.columns=['T','Azth','ZAth','vsT0']
    masktmin = df['T']>tmin
    masktmax = df['T']<tmax
    maskT = np.logical_and(masktmin,masktmax)
    df = df[maskT]
    mask0 = df['vsT0']!=0
    df = df[mask0]
    df['Tth'] = df['T'].apply(lambda d: datetime.fromtimestamp(d, tz=pytz.utc))
    df_dict = df.to_dict('records')
    for rows in df_dict:
        rows["Tth"] = str(rows["Tth"].timestamp()).replace(".", "")
        rows["Tth"] = int(rows["Tth"].ljust(2+len(rows["Tth"]), '0'))
    MongoDb.storeTrack(MongoDb, df_dict)
##### READ LOAD PIN
def getLoadPin(filename2):
    print("getLoadPin %s"%(filename2))
    t0=0
    dt=0
    t0=datetime(1970,1,1)
    pst = pytz.timezone('UTC')
    t0 = pst.localize(t0)
    f2 = open(filename2, "r")
    lp=0
    lpval=0
    values = 0
    pins = []
    lines = f2.readlines()
    for line in lines:
        val=line.split(' ')
        dval = int(val[0])
        lp=int(val[1])
        for v in range(2,len(val)):
            values += 1
            dvalinc = int(dval) + (v-2)*0.1
            lpval=int(val[v].replace("\n",""))
            pins.append({'T':str(dvalinc),'LoadPin':lp,'Load':lpval})
    print("Storing the data")
    print(values)
    MongoDb.storeLoadPin(MongoDb, pins)
#Used in checkDatev2
def GenerateFig(filename,filename2,filename3,filename4,tmin,tmax,cmd_status,ttrack,figname="",type=None,addtext='',ra=None,dec=None):
    print("GenerateFig %s %s %s %s %s %s %s "%(filename,filename2,filename3,tmin,tmax,ttrack,figname))
    #Position log.
    getPos(filename,tmin,tmax)
    #Precision log
    if ttrack != 0:
        getTrackNew(filename3,tmin,tmax)
        getPrecision(filename.replace("DrivePosition","Accuracy"),tmin,tmax)
    if ra is not None:
        getBM(filename.replace('DrivePosition','BendingModelCorrection'),tmin,tmax)
    getTorqueNew(filename4,tmin,tmax)
    start = datetime.fromtimestamp(tmin).strftime("%Y-%m-%d %H:%M:%S").split(" ")
    end = datetime.fromtimestamp(tmax).strftime("%Y-%m-%d %H:%M:%S").split(" ")
    if len(operationTimes)>0:
        file = figname.split("/")
        imageSplitEnd = file[-1].split(".")
        finalImage = file[-4]+"/"+file[-3]+"/"+file[-2]+"/"+imageSplitEnd[0]
        MongoDb.storeGeneralData(MongoDb, {"type": type, "Sdate": start[0], "Stime": start[1], "Edate": end[0], "Etime": end[1], "RA": ra, "DEC": dec, "file": finalImage, "addText": addtext})
#Used in checkDateV2 Gets the regulation parameters for Elevation and Azimuth from the cmd
def getRegulParameters(param,paramline,begtrack):
    paramout=""
    for i in range(len(param)-1,-1,-1):
        if param[i]<begtrack:
            for j in range(7,len(paramline[i].split(" "))):
                paramout+=str(paramline[i].split(" ")[j])
                paramout+=" "
            break            
    print("paramout %s"%(paramout))
    return paramout
#Used in GetAllDate function
def checkDatev2(cmd,beg,end,error,stop,track,repos,filename,filename2,filename3,filename4,figname,type,zoom=0,action="",lastone=0,azparam=None,azparamline=None,elparam=None,elparamline=None,ra=None,dec=None):
    beg_ok=[]
    end_ok=[]
    cmd_status=[]
    # Loop over beginning
    for k in range(len(beg)):
        endarray=[9999999999,9999999999,9999999999]
        #get first end
        for j in range(len(end)):
            if end[j] > beg[k] :
                endarray[0]=end[j]
                break
        #get first error
        for j in range(len(error)):
            if error[j] > beg[k] :
                endarray[1]=error[j]-1
                break
        #get first stop
        for j in range(len(stop)):
            if stop[j] > beg[k] :
                endarray[2]=stop[j]
                break
        beg_ok.append(beg[k])
        end_ok.append(min(endarray))
        cmd_status.append(endarray.index(min(endarray)))
    figpre = figname
    trackok=[]
    trackok.clear()
    raok=[]
    raok.clear()
    decok=[]
    decok.clear()
    for i in range(len(beg_ok)):
        trackok.append(0)
        raok.append(0)
        decok.append(0)
        if track is not None:
            for j in range(len(track)):
                if track[j]<beg_ok[i] :
                    if (beg_ok[i]-track[j])< (6*60):
                        trackok[i] = track[j]
                        raok[i] = ra[j]
                        decok[i] = dec[j]
                else :
                    continue
    #Used in GenerateFig
    addtext=''     
    if azparamline is not None:
        addtext = "Az " + getRegulParameters(azparam,azparamline,beg_ok[-1])
    if elparamline is not None:
        addtext += "El " + getRegulParameters(elparam,elparamline,beg_ok[-1])
    raok2 = None
    decok2 = None
    if lastone == 0:
        for i in range(len(end_ok)):
            begname = datetime.fromtimestamp(beg_ok[i], tz=pytz.utc)
            endname = datetime.fromtimestamp(end_ok[i], tz=pytz.utc)
            sbegname = begname.strftime("%Y%m%d_%Hh%Mm%Ss")
            sendname = endname.strftime("%Y%m%d_%Hh%Mm%Ss")
            figname = "_%s_%s"%(sbegname,sendname) + ".html"
            figname = figpre + figname.replace(":","")
            trackok2 = trackok[i]
            if ra is not None:
                raok2 = raok[i]
                decok2 = decok[i]
            if figname.find("Track") != -1 and (end_ok[i]-beg_ok[i])<5 :
                ii=0
            else:
                tmin = beg_ok[i]
                tmax = end_ok[i]
                if zoom==2:
                    tmin = tmax-53
                    tmax = tmax-20
                if zoom==1:
                    tmin = tmax-200
                    tmax = tmax
                GenerateFig(filename,filename2,filename3,filename4,tmin,tmax, cmd_status[i],trackok2,figname.replace(" ",""),type,addtext,raok2,decok2)
    else:
        begname = datetime.fromtimestamp(beg_ok[-1], tz=pytz.utc)
        endname = datetime.fromtimestamp(end_ok[-1], tz=pytz.utc)
        sbegname = begname.strftime("%Y%m%d_%Hh%Mm%Ss")
        sendname = endname.strftime("%Y%m%d_%Hh%Mm%Ss")
        figname = "_%s_%s"%(sbegname,sendname) + ".html"
        figname = figpre + figname.replace(":","")
        trackok2 = trackok[-1]
        raok2 = raok[i]
        decok2 = decok[i]
        if figname.find("Track") != -1 and (end_ok[-1]-beg_ok[-1])<5 :
            ii=0
        else:
            tmin = beg_ok[-1]
            tmax = end_ok[-1]
            if zoom==2:
                tmin = tmax-53
                tmax = tmax-20
            if zoom==1:
                tmin = tmax-200
                tmax = tmax
            GenerateFig(filename,filename2,filename3,filename4,tmin,tmax,cmd_status[-1],trackok2,figname.replace(" ",""),type,addtext,raok2,decok2)
#Stores the logs and the opreation into mongodb
def storeLogsAndOperation(logsorted):
    try: 
        logs = []
        data = {}
        operationTmin = None
        operationTmax = datetime.timestamp(logsorted[len(logsorted)-1][0])
        operationDate = logsorted[0][0].strftime("%Y-%m-%d")
        commandPosition = None
        for i in range(0,len(logsorted)):
            if logsorted[i][1].find("action error")!= -1 and commandPosition != None :
                logs[commandPosition]["LogStatus"] = "Error"
                commandPosition = None
            if logsorted[i][1].find("StopDrive")!= -1  and commandPosition != None :
                    logs[commandPosition]["LogStatus"] = "Stopped"
                    commandPosition = None
            if i == len(logsorted)-1 or logsorted[i+1][1].find("Park_Out command sent") != -1 or logsorted[i+1][1].find("Park_In command sent") != -1 or logsorted[i+1][1].find("GoToPosition") != -1 or logsorted[i+1][1].find("Start Tracking") != -1 or logsorted[i+1][1].find("Drive") != -1:    
                if logsorted[i][1].find("Park_Out Done")!= -1 and commandPosition != None :
                    logs[commandPosition]["LogStatus"] = "Finished"
                    commandPosition = None
                if logsorted[i][1].find("Park_In Done")!= -1 and commandPosition != None :
                    logs[commandPosition]["LogStatus"] = "Finished"
                    commandPosition = None
                if logsorted[i][1].find("GoToTelescopePosition Done")!= -1 and commandPosition != None :
                    logs[commandPosition]["LogStatus"] = "Finished"
                    commandPosition = None
                if logsorted[i][1].find("Start_Tracking Done received")!= -1 and commandPosition != None :
                    logs[commandPosition]["LogStatus"] = "Finished"
                    commandPosition = None
            if logsorted[i][1].find("Park_Out command sent") != -1 or logsorted[i][1].find("Park_In command sent") != -1 or logsorted[i][1].find("GoToPosition") != -1 or logsorted[i][1].find("Start Tracking") != -1:
                if commandPosition != None:
                    logs[commandPosition]["LogStatus"] = "Unknown"
                    commandPosition = i
                else:
                    commandPosition = i
            else:
                data["LogStatus"] = None
            if len(logsorted[i][1].split(" ")) <= 2:
                data["Command"] = logsorted[i][1]
                data["Status"] = None
            else:
                logParts = logsorted[i][1].split(" ")
                data["Command"] = logParts[0]
                data["Status"] = logParts[1]+" "+logParts[2]
            data["Date"] = logsorted[i][0].strftime("%Y-%m-%d")
            data["Time"] = logsorted[i][0].strftime("%H:%M:%S")
            if operationTmin == None and commandPosition != None:
                operationTmin = datetime.timestamp(logsorted[commandPosition][0])
            logs.append(data)
            data = {}
        for element in logs:
            if element["Command"] != "Drive":
                MongoDb.storeLogs(MongoDb, element)
        if operationTmin != None and operationTmax != None and operationDate != None:
            MongoDb.storeOperation(MongoDb, {"Date": operationDate, "Tmin": operationTmin, "Tmax": operationTmax})
            operationTimes.append(operationTmin)
            operationTimes.append(operationTmax)
    except Exception as e:
        print("Logs could not be stored: "+str(e))
#Function that stores the Damage
def calculateDamage(date):
    try:
        # --- S-N Curve Configuration (Stress-Number of Cycles) ---
        # Define known stress points and corresponding number of cycles to failure
        stress = np.array([292, 136, 63, 50, 37, 32, 20]) # Stress values in MPa
        cycles = np.array([1e4, 1e5, 1e6, 2e6, 5e6, 1e7, 1e8]) # Number of cycles

        # Define the S-N curve function (Basquin's equation form)
        def sn_curve(N, a, b):
            """
            Calculates stress (S) given number of cycles (N) based on S = a * N^(-b).
            Clips N to avoid issues with very low or very high cycle counts.
            """
            N = np.clip(N, 1e3, 1e12) # Clip N to a practical range
            return a * N**(-b)

        # Fit the S-N curve function to the experimental data to find parameters 'a' and 'b'
        params, _ = curve_fit(sn_curve, cycles, stress)
        a, b = params # Unpack the fitted parameters
        # Define helper functions based on the fitted S-N curve
        def estimate_cycles(stress_input):
            """Estimates the number of cycles to failure for a given stress input."""
            return (stress_input / a) ** (-1 / b)
        print("Calculating Damages...")
        # Prepare data for plotting and analysis
        datetime_objects = [datetime.fromtimestamp(item['T'] / 1000, tz=UTC) for item in dailyPosition] # Convert timestamps to datetime objects
        za_values = [item['ZA'] for item in dailyPosition] # Extract Zenith Angle values
        conversion = pd.read_csv('/opt/lst-drive/src/LST-DM-LP-Internal/deg_to_stress.csv')
        # --- Abrupt Movement Identification Logic ---
        # Initialize variables for detecting significant changes (cycles) in Zenith Angle
        abrupt_movements = [] # List to store identified abrupt movements
        prev_value = None # Stores the previous data point {ZA, T}
        # Variables for identifying start and end points of a potential cycle
        start_down_value = None # Marks the start of a downward ZA trend (potential cycle start)
        start_up_value = None   # Marks the start of an upward ZA trend (potential cycle end)
        deepest_point = None    # Tracks the minimum ZA value in the current segment
        deepest_time = None     # Timestamp of the deepest_point
        highest_point = None    # Tracks the maximum ZA value in the current segment
        highest_time = None     # Timestamp of the highest_point
        # Candidate points for cycle boundaries, refined as data is processed
        start_down_candidate = None
        start_up_candidate = None

        # Iterate through each data point (ZA and timestamp) for the current day
        for i, current_value in enumerate(dailyPosition):
            current_za = current_value['ZA']
            current_time = current_value['T']

            # Identify the overall highest and deepest ZA points for the day
            if not deepest_point or current_za < deepest_point:
                deepest_point = current_za
                deepest_time = current_time
                # Reset highest point if a new deepest point is found (implies a new trend segment)
                highest_point = None
                highest_time = None
            if not highest_point or current_za > highest_point:
                highest_point = current_za
                highest_time = current_time

            # Logic to identify cycles based on ZA changes (this is complex state-based logic)
            # This section attempts to define a "cycle" by looking for patterns of ZA decreasing then increasing.
            # Thresholds (e.g., 0.25, -0.1, -0.25, 0.1) define significant changes.

            if not start_up_value and not start_down_value: # Initial state or after a cycle is completed
                start_up_value = current_value # Assume an upward trend might start
            elif not start_down_value and start_up_value and start_down_candidate and current_value['ZA'] - start_down_candidate['ZA'] > 0.25:
                # Confirmed downward trend start after an upward phase
                start_down_value = start_down_candidate
                start_up_value = None # Reset start_up_value, looking for end of downward trend
                start_down_candidate = None
            elif not start_down_value and not start_down_candidate and start_up_value and current_value['ZA'] - prev_value['ZA'] > 0:
                # Potential start of a downward trend (ZA increased then previous was lower)
                if prev_value: # Ensure prev_value exists
                     start_down_candidate = prev_value
            elif not start_down_value and start_down_candidate and start_up_value and current_value['ZA'] - start_down_candidate['ZA'] < -0.1:
                # Downward candidate invalidated, ZA not decreasing enough from candidate
                start_down_candidate = None
            elif start_down_value and start_up_candidate and not start_up_value and current_value['ZA'] > start_down_value['ZA'] and current_value['ZA'] - start_up_candidate['ZA'] < -0.25:
                # Confirmed upward trend start after a downward phase (cycle completed)
                start_up_value = start_up_candidate
                abrupt_movements.append({
                                        'start_value': start_down_value['ZA'],
                                        'start_time': start_down_value['T'],
                                        'end_value': start_up_value['ZA'],
                                        'end_time': start_up_value['T']
                                        })
                # Reset for next cycle detection
                start_up_candidate = None
                start_down_value = None
            elif start_down_value and start_up_candidate and not start_up_value and current_value['ZA'] < start_down_value['ZA'] and current_value['ZA'] - start_up_candidate['ZA'] < -0.25:
                start_down_value = None
                start_up_value = start_up_candidate
                start_up_candidate = None
            elif start_down_value and not start_up_value and not start_up_candidate  and current_value['ZA'] - prev_value['ZA'] < 0:
                # Potential start of an upward trend (ZA decreased then previous was higher)
                if prev_value: # Ensure prev_value exists
                    start_up_candidate = prev_value
            elif start_down_value and start_up_candidate and not start_up_value and current_value['ZA'] - start_up_candidate['ZA'] > 0.1:
                # Upward candidate invalidated, ZA not increasing enough from candidate
                start_up_candidate = None

            prev_value = current_value # Update previous value for the next iteration

        # Handle a potential incomplete cycle at the end of the data
        if start_down_value and start_up_candidate:
            abrupt_movements.append({
                                        'start_value': start_down_value['ZA'],
                                        'start_time': start_down_value['T'],
                                        'end_value': start_up_candidate['ZA'], # Use candidate as end
                                        'end_time': start_up_candidate['T']
                                    })
        elif start_down_value:
            abrupt_movements.append({
                                        'start_value': start_down_value['ZA'],
                                        'start_time': start_down_value['T'],
                                        'end_value': prev_value['ZA'], # Use prev value as end
                                        'end_time': prev_value['T']
                                    })


        # Ensure the overall largest daily fluctuation (deepest to highest) is included as a movement
        if highest_point and deepest_point:
            element = {
                'start_value': deepest_point,
                'start_time': deepest_time,
                'end_value': highest_point,
                'end_time': highest_time
            }
            if element not in abrupt_movements: # Avoid duplicating if already identified
                 abrupt_movements.append(element)
            else:
                 print("Avoided duplicated general value (overall daily max-min)")

        # --- Damage Calculation ---
        def process_movement(element, grouped_values):
            """
            Processes a single abrupt movement to calculate its contribution to damage.
            Converts ZA change to stress and counts occurrences of each stress level.
            """
            start_value = element['start_value'] # ZA at the start of the movement
            end_value = element['end_value'] # ZA at the end of the movement
            start_stress = None
            end_stress = None
            stress_value = None

            # Check if ZA values are within a plausible range (0-100 degrees)
            if start_value < 100 and end_value < 100 and start_value >= 0 and end_value >= 0:
                # Query the conversion table to find MPa stress corresponding to rounded ZA degrees
                matching_row_start = conversion.query(f"Degree == {round(start_value)}")
                matching_row_end = conversion.query(f"Degree == {round(end_value)}")

                # Safely get the MPa value if a match was found
                if not matching_row_start.empty:
                    start_stress = round(matching_row_start.iloc[0]["MPa"])
                if not matching_row_end.empty:
                    end_stress = round(matching_row_end.iloc[0]["MPa"])

                # If both start and end stresses are found, calculate the stress amplitude
                if start_stress is not None and end_stress is not None:
                    stress_value = abs(start_stress - end_stress) # Stress amplitude of the cycle
                    if stress_value > 0:
                        # Group by stress value and count occurrences (cycles at that stress level)
                        if str(stress_value) in grouped_values:
                            grouped_values[str(stress_value)] += 1
                        else:
                            grouped_values[str(stress_value)] = 1

        grouped_values = {} # Dictionary to store stress amplitudes and their counts
        accumulated_damage_today = 0 # Initialize damage for the current day

        # Process each identified abrupt movement to populate grouped_values
        for element in abrupt_movements:
            process_movement(element, grouped_values)

        # Calculate Miner's rule damage for the current day
        for stress_amplitude_str, num_cycles_at_stress in grouped_values.items():
            rounded_stress = round(float(stress_amplitude_str), 2) # Convert stress key to float
            # Estimate max cycles to failure at this stress level using the S-N curve
            max_cycles_to_failure = estimate_cycles(rounded_stress)
            # Add the damage fraction (actual cycles / cycles to failure)
            if max_cycles_to_failure > 0: # Avoid division by zero
                accumulated_damage_today += num_cycles_at_stress / max_cycles_to_failure
        date = datetime.strptime(date, "%Y-%m-%d")
        total_cycles_today = sum(grouped_values.values())
        damage_dict = {'T': date, 'DMG': accumulated_damage_today, 'CYCLES':total_cycles_today}
        print("This is the damage: ", damage_dict)
        MongoDb.storeDamage(MongoDb, damage_dict)
    except Exception:
        pass

#Function that recieves all the Log File names and 
def getAllDate(filename,filename2,filename3,filename4,filename5, date, lastone=0):
    dirname = "/fefs/onsite/data/R1/LSTN-01/lst-drive/DMonitoring/static/html/Log_" + filename
    print("This is the no check script running for the date: "+date)
    generallog.clear()
    firstData = date
    #Genereal
    generalstop = getDate(filename,"StopDrive command sent")
    trackcmdinitiale = getDate(filename,"Start Tracking")
    gotocmdinitiale = getDate(filename,"GoToPosition") 
    #Param regulation
    azparam,azparamline = getDateAndLine(filename,"Drive Regulation Parameters Azimuth")    #This prints the found msg in console
    elparam,elparamline = getDateAndLine(filename,"Drive Regulation Parameters Elevation")  #This prints the found msg in console
    #Tracking
    trackcmd = getDate(filename,"Start_Tracking command sent")
    trackbeg = getDate(filename,"Start_Tracking in progress")
    trackend = getDate(filename,"Start_Tracking Done received")
    trackerror = getDate(filename,"Start_Tracking action error")
    track = getDateTrack(filename,"[Drive] Track start")
    ra,dec,radectime = getRADec(filename,"Start Tracking")
    #Parkout
    parkoutcmd = getDate(filename,"Park_Out command sent")
    parkoutbeg = getDate(filename,"Park_Out in progress")
    parkoutend = getDate(filename,"Park_Out Done received")
    parkouterror = getDate(filename,"Park_Out action error")
    #Parkin
    parkincmd = getDate(filename,"Park_In command sent")
    parkinbeg = getDate(filename,"Park_In in progress")
    parkinend = getDate(filename,"Park_In Done received")
    parkinerror = getDate(filename,"Park_In action error")
    #GoToTelPos
    gotocmd = getDate(filename,"GoToTelescopePosition command sent")
    gotobeg = getDate(filename,"GoToTelescopePosition in progress")
    gotoend = getDate(filename,"GoToTelescopePosition Done received")
    gotoerror = getDate(filename,"GoToTelescopePosition action error")
    generallogsorted =sorted(generallog, key=itemgetter(0)) #Orders generallog by date as position 0 contains begdate value
    print("START TIME")
    print(datetime.now().strftime("%H:%M:%S"))
    storeLogsAndOperation(generallogsorted)
    if len(operationTimes) > 0:
        if len(trackbeg) != 0:
            print("====== Track =======")
            selectedType = "1"
            checkDatev2(trackcmd,trackbeg,trackend,trackerror,generalstop,track,None,filename2,filename3,filename4,filename5,dirname+"/Track"+"/Track",generalTypes[selectedType],0,"Tracking",lastone,azparam,azparamline,elparam,elparamline,ra,dec)
        if lastone ==0 :
            if len(parkoutbeg) != 0:
                print("====== Parkout =======")
                selectedType = "2"
                checkDatev2(parkoutcmd,parkoutbeg,parkoutend,parkouterror,generalstop,None,None,filename2,filename3,filename4,filename5,dirname+"/Parkout"+"/Parkout",generalTypes[selectedType],0,"ParkOut")
            if len(parkinbeg) != 0:
                print("====== Parkin =======")
                selectedType = "3"
                checkDatev2(parkincmd,parkinbeg,parkinend,parkinerror,generalstop,None,None,filename2,filename3,filename4,filename5,dirname+"/Parkin"+"/Parkin",generalTypes[selectedType],1,"ParkIn")
            if len(gotobeg) != 0:
                print("====== GoToPos =======")
                selectedType = "4"
                checkDatev2(gotocmd,gotobeg,gotoend,gotoerror,generalstop,None,None,filename2,filename3,filename4,filename5,dirname+"/GoToPos"+"/GoToPos",generalTypes[selectedType],0,"GoToPsition")
    else:
        print("There is no general data or there was an error")
    try:
        calculateDamage(date)
    except Exception as e:
        print("Could not store the Damage: "+str(e))
    try:
        getLoadPin(filename3)
    except Exception as e:
        print("Could not store Load Pins: "+str(e))
    try: 
        if firstData is not None:
            req = requests.post(IP+"/storage/plotGeneration", json=[[firstData]])
    except Exception as e:
        print("Plot was not generated because there is no conection to Django or there was a problem: "+str(e))
    print("END TIME")
    print(datetime.now().strftime("%H:%M:%S"))
