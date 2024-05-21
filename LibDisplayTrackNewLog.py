import numpy as np
from numpy import fft

import matplotlib as mpl
mpl.use('Agg')

import matplotlib.pyplot as plt
import math
from math import log
from datetime import datetime,timedelta,date
import time as time
from time import gmtime, strftime
import os, sys, getopt
from os import path
import pytz
#from datetime import timezone
import matplotlib.dates as mdates
from matplotlib import gridspec
from operator import itemgetter

import astropy.units as u
from astropy.time import Time
from astropy.coordinates import SkyCoord, EarthLocation, AltAz, solar_system_ephemeris,ICRS


import pandas as pd
import numpy as np

shifttemps=0

SMALL_SIZE = 13
MEDIUM_SIZE = 13
BIGGER_SIZE = 13

plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=SMALL_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title


az_pbaz=[]
zd_pbaz=[]
T_pbaz=[]
dT_pbaz=[]
az_pbzd=[]
zd_pbzd=[]
T_pbzd=[]
dT_pbzd=[]
az_ok=[]
azrad_ok=[]
zd_ok=[]
erraz_ok=[]
errzd_ok=[]

valdif=60.

#General
generallog = []


def make_patch_spines_invisible(ax):
    ax.set_frame_on(True)
    ax.patch.set_visible(False)
    for sp in ax.spines.values():
        sp.set_visible(False)



def getDate(filename,cmdstring):
    #print("getDate %s %s"%(filename,cmdstring))
    f = open(filename, "r") 
    xbeg=[]
    xbeg.clear()
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
            #print("Found %s %s %s"%(cmdstring,begtime,begtimes))
            generallog.append([begtime,cmdstring])
    return xbeg

def getRADec(filename,cmdstring):
    #print("getDate %s %s"%(filename,cmdstring))
    f = open(filename, "r") 
    ra=[]
    dec=[]
    radectime=[]
    for line in f.readlines():
        if line.find(cmdstring) != -1:
            val =line.split(' ')
            #print(val)
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
                    ra.append(float(vali[(vali.find('=')+1):(vali.find('['))]))
                if vali.find("Dec=") != -1:
                    dec.append(float(vali[(vali.find('=')+1):(vali.find('['))]))
    return ra,dec,radectime


def getDateTrack(filename,cmdstring):
    #print("getDateTrack %s %s"%(filename,cmdstring))
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
            #print("Found %s %s %s "%(cmdstring,begtime,begtimes))
    return xbeg



def getRepos(filename,cmdstring):
    #print("getRepos %s %s"%(filename,cmdstring))
    f = open(filename, "r") 
    xbeg=[]
    for line in f.readlines():
        val =line.split(' ')
        #print(val)
        az = float(val[8])
        zd = float(val[11])
        tps = float(val[13])+ float(val[14])
        xbeg.append(az)
        xbeg.append(zd)
        xbeg.append(tps)
    return xbeg  

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
    df = df[maskT]

    maskT1 = df['T']<1605657600
    maskt2 = df['T']>1611057600
    maskt3 = df['T']<1615161600
    maskT2 = np.logical_and(maskt2,maskt3)
    df['T'] = df['T'] + maskT1*-2 + maskT2*-2
    
    df['T'] = df['T'].apply(lambda d: datetime.fromtimestamp(d, tz=pytz.utc))

#    dfBM = pd.read_csv(filename,sep=' ',header=None)
#    dfBM.columns=['T','AzC','ZAC']
#    masktmin = dfBM['T']>tmin
#    masktmax = dfBM['T']<tmax
#    maskT = np.logical_and(masktmin,masktmax)
#    dfBM = dfBM[maskT]

#    maskT1 = dfBM['T']<1605657600
#    maskt2 = dfBM['T']>1611057600
#    maskt3 = dfBM['T']<1615161600
#    maskT2 = np.logical_and(maskt2,maskt3)
#    dfBM['T'] = dfBM['T'] + maskT1*-2 + maskT2*-2
    
#    df['Az'] = df['Az']+dfBM['AzC']
#    df['ZA'] = df['ZA']+dfBM['ZAC']
    
    return df
    
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
    return dfBM
    
            
            
       
            
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
    mask0_2 = df['Zdmean']!=0.
    mask0 = np.logical_and(mask0_2,mask0_1)
    df = df[mask0]
    return df

def getDateAndLine(filename,cmdstring):
    #print("getDate %s %s"%(filename,cmdstring))
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
            #print(stringtime)
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
    return df
    
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
    return df
    #print("getTrack %s %s %s %s"%(filename3,tmin,tmax,ttrack))


##### READ LOAD PIN
def getLoadPin(filename2,tmin,tmax):
    #print("getLoadPin %s %s %s"%(filename2,tmin,tmax))
    t0=0
    dt=0
    
    t0=datetime(1970,1,1)
    pst = pytz.timezone('UTC')
    t0 = pst.localize(t0)

    f2 = open(filename2, "r") 
    df=pd.DataFrame(columns=['T','LoadPin','Load'])

    lp=0
    lpval=0
    for line in f2.readlines():
        val=line.split(' ')
        dval = int(val[0])
        dateval = datetime.fromtimestamp(dval)
        lp=int(val[1])
        if(lp!=107 and lp!=207):
            continue
        if dval>tmin and dval<tmax:
            for v in range(2,len(val)):
                dvalinc = int(dval) + (v-2)*0.1
                dateval = datetime.fromtimestamp(dvalinc, tz=pytz.utc)
                lpval=int(val[v].replace("\n",""))
                df2=pd.DataFrame({'T':[dateval],'LoadPin':[lp],'Load':[lpval]})
                df = pd.concat([df2,df],ignore_index=True)
    return df
                    
def GenerateFig(filename,filename2,filename3,filename4,tmin,tmax,cmd_status,ttrack,figname="",fichierhtml="",addtext='',ra=None,dec=None):
    print("GenerateFig %s %s %s %s %s %s %s "%(filename,filename2,filename3,tmin,tmax,ttrack,figname))
    if ra is not None:
        addhtmltitle(fichierhtml,datetime.fromtimestamp(tmin, tz=pytz.utc).strftime('%Y%m%d %H:%M:%S') + ' RA=%s Dec=%s'%(ra,dec))
    else:
        addhtmltitle(fichierhtml,datetime.fromtimestamp(tmin, tz=pytz.utc).strftime('%Y%m%d %H:%M:%S'))

    #Position log
    dfpos = getPos(filename,tmin,tmax)
    #print(dfpos)

    
    #Precision log
    #print(dfacc)

    dftrack = None
    dfacc = None
    if ttrack != 0:
        dftrack = getTrackNew(filename3,tmin,tmax)
        dfacc = getPrecision(filename.replace("DrivePosition","Accuracy"),tmin,tmax)
        #print(dftrack)
    
    dfloadpin = getLoadPin(filename2,tmin,tmax)
    #print(dfloadpin)
    
    dfbm = None
    if ra is not None:
        dfbm = getBM(filename.replace('DrivePosition','BendingModelCorrection'),tmin,tmax)
        #print(dfpos)

    #getTorque(filename4,tmin,tmax)
    dftorque = getTorqueNew(filename4,tmin,tmax)
    #print(dftorque)
        
    FigureTrack(tmin,tmax,cmd_status,figname,addtext,fichierhtml,dfpos,dfloadpin,dftrack,dftorque)
    if dfacc is not None:
        FigAccuracyTime(figname,addtext,fichierhtml,dfacc)
#        FigAccuracyHist(figname,fichierhtml,dfacc)
    if dfbm is not None:
        FigRADec(figname,fichierhtml,dfpos,dfbm,ra,dec,dfacc,dftrack)    

        
    

    
def FigureTrack(tmin,tmax,cmd_status,figname,addtext,fichierhtml,dfpos,dfloadpin,dftrack,dftorque):
    print("FigureTrack %s %s"%(tmin,tmax))
    fig = plt.figure(figsize = (20, 12))
    plt.gcf().subplots_adjust(left = 0.1, bottom = 0.1,right = 0.9, top = 0.9, wspace = 0, hspace = 0.1)
    
    spec = gridspec.GridSpec(ncols=1, nrows=2,height_ratios=[2, 1])
    host1 = fig.add_subplot(spec[0])
    
    formatter = mdates.DateFormatter("%H:%M:%S")
    formatter.set_tzinfo(pytz.utc); 
    plt.gca().xaxis.set_major_formatter(formatter)
    
    par1 = host1.twinx()
    par2 = host1.twinx()
    par2.spines["right"].set_position(("axes", 1.08))
    
    make_patch_spines_invisible(par2)
    par2.spines["right"].set_visible(True)

    lines = []
    
    if dfloadpin is not None:
        mask107 = dfloadpin['LoadPin']==107
        mask207 = dfloadpin['LoadPin']==207

        p1, = host1.plot(dfloadpin[mask107]['T'],dfloadpin[mask107]['Load'], color='blue', label="Cable 107")
        p1b, = host1.plot(dfloadpin[mask207]['T'],dfloadpin[mask207]['Load'], color='green', label="Cable 207")
        lines.append(p1)
        lines.append(p1b)
        
    p2, = par1.plot(dfpos['T'],dfpos['Az'], color='red', label="Azimuth")
    lines.append(p2)
    if dftrack is not None:
        p2b, = par1.plot(dftrack['Tth'],dftrack['Azth'], color='red',ls='dashed', label="Azimuth Th.")
        lines.append(p2b)
    p3, = par2.plot(dfpos['T'],dfpos['ZA'], color='black', label="Zenith Angle")
    lines.append(p3)
    if dftrack is not None:
        p3b, = par2.plot(dftrack['Tth'],dftrack['ZAth'], color='black',ls='dashed', label="Zenith Angle Th.")
        lines.append(p3b)
       
    host1.set_xlabel("", fontsize=15)
    host1.set_ylabel("Load [kg]", fontsize=15)
    host1.set_title(figname, fontsize=15)
    par1.set_ylabel("Azimuth [deg]", fontsize=15)
    par2.set_ylabel("Zenith Angle [deg]", fontsize=15)

    host1.yaxis.label.set_color(p1.get_color())
    par1.yaxis.label.set_color(p2.get_color())
    par2.yaxis.label.set_color(p3.get_color())

    tkw = dict(size=4, width=1.5)
    host1.tick_params(axis='y', colors=p1.get_color(), **tkw)
    par1.tick_params(axis='y', colors=p2.get_color(), **tkw)
    par2.tick_params(axis='y', colors=p3.get_color(), **tkw)
    host1.tick_params(axis='x', **tkw)
          
    host1.set_xlim(datetime.fromtimestamp(tmin, tz=pytz.utc),datetime.fromtimestamp(tmax, tz=pytz.utc))
    host1.legend(lines, [l.get_label() for l in lines],fontsize=13)
    
    if cmd_status ==0:
        host1.text(0., 1., 'ACTION FINISHED',verticalalignment='bottom', horizontalalignment='left',transform=host1.transAxes,color='green', fontsize=15)
    if cmd_status ==2:
        host1.text(0., 1., 'ACTION STOPPED BY USER',verticalalignment='bottom', horizontalalignment='left',transform=host1.transAxes,color='orange', fontsize=15)
    if cmd_status ==1:
        host1.text(0., 1., 'ACTION STOPPED BY ERROR',verticalalignment='bottom', horizontalalignment='left',transform=host1.transAxes,color='red', fontsize=15)
    if cmd_status ==3:
        host1.text(0., 1., 'CUSTOM PERIOD',verticalalignment='bottom', horizontalalignment='left',transform=host1.transAxes,color='blue', fontsize=15)
    
    host2 = fig.add_subplot(spec[1])
    plt.gca().xaxis.set_major_formatter(formatter)
    
    if dftorque is not None:        
        p13b, =host2.plot(dftorque['T'],dftorque['El1_mean'], color='chocolate',label="El S")
        p23b, =host2.plot(dftorque['T'],dftorque['El2_mean'], color='red',label="El N")
        p12b, =host2.plot(dftorque['T'],dftorque['Az1_mean'], color='lime',label="Az SE")
        p22b, =host2.plot(dftorque['T'],dftorque['Az2_mean'], color='forestgreen',label="Az NE")
        p32b, =host2.plot(dftorque['T'],dftorque['Az3_mean'], color='cyan',label="Az NW")
        p42b, =host2.plot(dftorque['T'],dftorque['Az4_mean'], color='dodgerblue',label="Az SW")
        host2.set_xlabel('Time', fontsize=15)
        host2.set_ylabel('Torque [N.m]', fontsize=15)
        host2.set_xlim(datetime.fromtimestamp(tmin, tz=pytz.utc),datetime.fromtimestamp(tmax, tz=pytz.utc))

        lines = [p13b,p23b,p12b,p22b,p32b,p42b]
        host2.legend(lines, [l.get_label() for l in lines],fontsize=13,title=addtext)
    
    addhtmlfile(fichierhtml,figname)

    plt.savefig(figname, bbox_inches='tight')
    #plt.show()
    plt.close()

    
def logtolin(x):
    return np.exp(x * np.log(10.))
def lintolog(x):
    return np.log10(x)
 
def FigRADec(figname,fichierhtml,dfpos,dfbm,ra,dec,dfacc,dftrack):  
    print("FigRADec %s %s"%(ra,dec))

    dfpos['AzSky'] = dfpos['Az']+dfbm['AzC']
    dfpos['ZASky'] = dfpos['ZA']+dfbm['ZAC']
    dfpos['AltSky'] = 90.- dfpos['ZASky']

    #print(dftrack)
    #print(dfbm)
    
    tracksky = None
    if dftrack is not None:
        tracksky = pd.merge(dftrack, dfbm, on="T",how='inner')
        tracksky['AzSky'] = tracksky['Azth']+tracksky['AzC']
        tracksky['ZASky'] = tracksky['ZAth']+tracksky['ZAC']
        tracksky['AltSky'] = 90.- tracksky['ZASky']
    #print(tracksky)

    lst=EarthLocation.from_geodetic(-17.8915 * u.deg, 28.7615 * u.deg, 2202* u.m)
    #print(dfpos)
    direc_lst = AltAz(location=lst, obstime=dfpos['T'],az=dfpos['AzSky'] * u.deg, alt=dfpos['AltSky'] * u.deg,obswl=0.35*u.micron,relative_humidity=0.5,temperature=10*u.deg_C,pressure=790*u.hPa)
    sky_lst = SkyCoord(direc_lst.transform_to(ICRS()))
    target = SkyCoord(ra=ra*u.deg,dec=dec*u.deg, frame='icrs')
    distsky = target.separation(sky_lst)

    if tracksky is not None:
        direc_lst_track = AltAz(location=lst, obstime=tracksky['Tth'],az=tracksky['AzSky'] * u.deg, alt=tracksky['AltSky'] * u.deg,obswl=0.35*u.micron,relative_humidity=0.5,temperature=10*u.deg_C,pressure=790*u.hPa)
        sky_lst_track = SkyCoord(direc_lst_track.transform_to(ICRS()))
        target_track = SkyCoord(ra=ra*u.deg,dec=dec*u.deg, frame='icrs')
        distsky_track = target.separation(sky_lst_track)

    figname3 = figname.replace(".png","_SkyCoord.png")
    addhtmlfile(fichierhtml,figname3)
    fig = plt.figure(figsize = (20, 12))
    plt.gcf().subplots_adjust(left = 0.1, bottom = 0.15,right = 0.9, top = 0.85, wspace = 0.1, hspace = 0.1)

    spec = gridspec.GridSpec(ncols=2, nrows=2)

    hostd = fig.add_subplot(spec[0])
    if tracksky is not None:
        plt.hist(sky_lst_track.ra.deg,bins=30,histtype='step',density=False,label='Drive target', alpha=0.7,linewidth=3)
    plt.hist(sky_lst.ra.deg,bins=30,histtype='step',density=False,label='Telescope pointing', alpha=0.7,linewidth=3)
    plt.axvline(x=ra, color='k', linestyle='--',label="Target")
    hostd.set_xlabel("RA[deg]", fontsize=15)
    #hostd.set_title(figname3, fontsize=15)    
    hostd.legend()

    hostd = fig.add_subplot(spec[1])
    if tracksky is not None:
        plt.hist(sky_lst_track.dec.deg,bins=30,histtype='step',density=False,label='Drive target', alpha=0.7,linewidth=3)
    plt.hist(sky_lst.dec.deg,bins=30,histtype='step',density=False,label='Telescope pointing', alpha=0.7,linewidth=3)
    plt.axvline(x=dec, color='k', linestyle='--',label="Target")
    hostd.set_xlabel("Declination[deg]", fontsize=15)
    #hostd.set_title(figname3, fontsize=15)    
    hostd.legend()

    hostd = fig.add_subplot(spec[2])
    if tracksky is not None:
        log10distsky_track = np.log10(distsky_track.deg*3600.)
        plt.hist(log10distsky_track,histtype='step',density=False,label='Drive target', alpha=0.7,linewidth=3)
    log10distsky = np.log10(distsky.deg*3600.)
    plt.hist(log10distsky,histtype='step',density=False,label='Telescope pointing', alpha=0.7,linewidth=3)
    for i in range(1,10):
        plt.axvline(x=math.log10(i), color='red', linestyle='--')
        plt.axvline(x=math.log10(i*10), color='blue', linestyle='--')
        plt.axvline(x=math.log10(i*100), color='k', linestyle='--')
 
    hostd.set_xlabel("Log10 Angular Distance to Target[arcsec]", fontsize=15)
    #hostd.set_title(figname3, fontsize=15)    
    hostd.legend()

    hostd = fig.add_subplot(spec[3])
    azmaxacc = np.log10(np.maximum(np.abs(dfacc.Azmax),np.abs(dfacc.Azmin)))
    zdmaxacc = np.log10(np.maximum(np.abs(dfacc.Zdmax),np.abs(dfacc.Zdmin)))

    plt.hist(np.log10(np.abs(dfacc.Azmean)),bins=30,histtype='step',density=True,label='Azimuth 2s average', alpha=0.7,linewidth=3)
    plt.hist(azmaxacc,bins=30,histtype='step', density=True,label='Azimuth 2s max', alpha=0.7,linewidth=3)
    plt.hist(np.log10(np.abs(dfacc.Zdmean)),bins=30,histtype='step',density=True,label='Elevation 2s average', alpha=0.7,linewidth=3)
    plt.hist(zdmaxacc,bins=30,histtype='step', density=True,label='Elevation 2s max', alpha=0.7,linewidth=3)
    plt.axvline(x=math.log10(30.), color='k', linestyle='--',label="30\"")
    plt.axvline(x=math.log10(4.e-02), color='red', linestyle='--',label="Encoder resolution")
    hostd.set_xlabel('log$_{10}$(Accuracy [arcsec])',fontsize=15)
    hostd.legend()

    plt.savefig(figname3, bbox_inches='tight')
    #plt.show()
    plt.close()

    

def FigAccuracyTime(figname,addtext,fichierhtml,dfacc):
    print("FigAccuracyTime ")
    figname2 = figname.replace(".png","_Diff.png")
    addhtmlfile(fichierhtml,figname2)
    fig,hostd = plt.subplots(figsize = (25, 12))
    plt.gcf().subplots_adjust(left = 0.1, bottom = 0.1,right = 0.9, top = 0.9, wspace = 0.1, hspace = 0.1)

    #spec = gridspec.GridSpec(ncols=1, nrows=1,width_ratios=[4,1])
    #hostd = fig.add_subplot(0)

    formatter = mdates.DateFormatter("%H:%M:%S") ; 
    formatter.set_tzinfo(pytz.utc); 
    plt.gca().xaxis.set_major_formatter(formatter)

    pfill = plt.fill_between(dfacc['T'],-60.,60., color='palegreen',label="1' req.")
    pfill2 = plt.fill_between(dfacc['T'],-14.,14., color='limegreen',label="14'' req.")

    p1d = hostd.fill_between(dfacc['T'],dfacc['Azmax'],dfacc['Azmin'],facecolor='blue',alpha=0.5,label='Azimuth')
    p1bd = hostd.fill_between(dfacc['T'],dfacc['Zdmax'],dfacc['Zdmin'],facecolor='red',alpha=0.5,label='Zenith angle')

    hostd.set_ylabel("Pointing Error [arcsec]", fontsize=15)

    lines = [p1d,p1bd]

    hostd.axhline(y=0,xmin=0.05,xmax=0.95,linestyle='dotted')
    lines = [p1d,p1bd,pfill,pfill2]
    hostd.legend(lines, [l.get_label() for l in lines],fontsize=13,title=addtext)
    hostd.set_title(figname2, fontsize=15)    
    plt.legend()
    plt.savefig(figname2, bbox_inches='tight')
    plt.close()



def addhtmlfile(fichierhtml,figname):
    fichierhtml.write("<img src=\"%s\" height=\"500px\" /> \n"%(figname.replace("/DriveOutput/","")))
         
def addhtmltitle(fichierhtml,title):
    fichierhtml.write("<h3> %s </h3>\n"%(title))


def checkDate(beg,end,error,stop,track,repos,filename,filename2,filename3,filename4,figname,fichierhtml,zoom=0):
    failed=0
    #print("CheckDate %s %s"%(beg,end))
    #print("len(beg)%s"%(len(beg)))
    #print("len(end)%s"%(len(end)))
    beg_ok=[]
    end_ok=[]

    for j in range(len(end)):
        for k in range(len(beg)):
            if j==0 and k==0:
                if beg[k]<end[0]:
                    end_ok.append(end[0])
                    break
            if j>0:
                if beg[k]<end[j] and beg[k]>end[j-1]:
                    end_ok.append(end[j])
                    break

    for j in range(len(end_ok)):
        for k in reversed(range(len(beg))):
            #print("%s %s %s vs %s"%(j,k,end_ok[j],beg[k]))
            if beg[k]<end_ok[j]:
                beg_ok.append(beg[k])
                #print("ok")
                break

    figpre = figname
        
    trackok=[]
    trackok.clear()
    for i in range(len(beg_ok)):
        #trackok.append(datetime.fromtimestamp(0))
        trackok.append(0)
        if track is not None:
            for j in range(len(track)):
                #print(track[i])
                if track[j]<beg_ok[i] :
 #                   if (beg_ok[i]-track[j])< timedelta(minutes=6):
                    if (beg_ok[i]-track[j])< (6*60):
                        trackok[i] = track[j]
                        #pst = pytz.timezone('UTC')
                        #trackok[i] = pst.localize(trackok[i])
                else :
                    continue

    
    for i in range(len(end_ok)):
        print("%s T=%s => Duration=%s Track Start %s"%(i,beg_ok[i],end_ok[i]-beg_ok[i],trackok[i]))
        begname = datetime.fromtimestamp(beg_ok[i], tz=pytz.utc)
        endname = datetime.fromtimestamp(end_ok[i], tz=pytz.utc)
        sbegname = begname.strftime("%Y%m%d_%Hh%Mm%Ss")
        sendname = endname.strftime("%Y%m%d_%Hh%Mm%Ss")
        figname = "_%s_%s"%(sbegname,sendname) + ".png"
        figname = figpre + figname.replace(":","")
        trackok2 = trackok[i]
        #pst = pytz.timezone('UTC')
        #trackok2 = pst.localize(trackok2)
        GenerateFig(filename,filename2,filename3,filename4,beg_ok[i],end_ok[i],trackok2,figname.replace(" ",""),fichierhtml,zoom)



def checkallactions(logsorted,fichierhtml):
    #0 reset
    #1 cmd sent
    #2 in progress
    sstate = 0

    isinerror=0
    isstopped=0
    isfinished=0

    action=""
    actiondate=0

    fichierhtml.write("<h2>Summary</h2>\n")

    for i in range(0,len(logsorted)):
        #print("%s %s"%(logsorted[i][1],logsorted[i][0]))
        if logsorted[i][1].find("action error")!= -1 :
            isinerror=1
        if logsorted[i][1].find("StopDrive")!= -1 :
            isstopped=1
        if action.find("Park_Out")!= -1 and logsorted[i][1].find("Park_Out Done")!= -1 :
            isfinished=1
        if action.find("Park_In")!= -1 and logsorted[i][1].find("Park_In Done")!= -1 :
            isfinished=1
        if action.find("GoToPosition")!= -1 and logsorted[i][1].find("GoToTelescopePosition Done")!= -1 :
            isfinished=1
        if action.find("Start Tracking")!= -1 and logsorted[i][1].find("Start_Tracking Done received")!= -1 :
            isfinished=1
        if logsorted[i][1].find("Park_Out command sent") != -1 or logsorted[i][1].find("Park_In command sent") != -1 or logsorted[i][1].find("GoToPosition") != -1 or logsorted[i][1].find("Start Tracking") != -1 or i==(len(logsorted)-1):
            if action != "":
                if isinerror==1:
                    fichierhtml.write("<font color=\"red\">%s %s => Error </font><br>"%(actiondate.strftime("%Y-%m-%d_%H:%M:%S"),action.replace(" command sent","")))
                else:
                    if isstopped==1:
                        fichierhtml.write("<font color=\"green\">%s %s => Stopped by user </font><br>"%(actiondate.strftime("%Y-%m-%d_%H:%M:%S"),action.replace(" command sent","")))
                    else:
                        if isfinished==1:
                            fichierhtml.write("<font color=\"lime\">%s %s => Finished </font><br>"%(actiondate.strftime("%Y-%m-%d_%H:%M:%S"),action.replace(" command sent","")))
                        else:
                            fichierhtml.write("<font color=\"black\">%s %s => Unknown status </font><br>"%(actiondate.strftime("%Y-%m-%d_%H:%M:%S"),action.replace(" command sent","")))

            isinerror=0
            isstopped=0
            isfinished=0
            action=logsorted[i][1]
            actiondate=logsorted[i][0]
            
            
    #print("%s %s"%(action,actiondate))


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

         

def checkDatev2(cmd,beg,end,error,stop,track,repos,filename,filename2,filename3,filename4,figname,fichierhtml,zoom=0,action="",lastone=0,azparam=None,azparamline=None,elparam=None,elparamline=None,ra=None,dec=None):
    #print("beg   %s"%(beg))
    #print("end   %s"%(end))
    #print("error %s"%(error))
    #print("stop  %s"%(stop))

    failed=0
    #print("CheckDate %s %s"%(beg,end))
    #print("len(beg)%s"%(len(beg)))
    #print("len(end)%s"%(len(end)))
    beg_ok=[]
    end_ok=[]
    cmd_status=[]





    # Loop over beginning
    for k in range(len(beg)):
        endarray=[9999999999,9999999999,9999999999]
        #get first end
        for j in range(len(end)):
            #print("end   %s"%(end[j]))
            if end[j] > beg[k] :
                endarray[0]=end[j]
                break
        #get first error
        for j in range(len(error)):
            #print("error %s"%(error[j]))
            if error[j] > beg[k] :
                endarray[1]=error[j]-1
                break
        #get first stop
        for j in range(len(stop)):
            #print("stop  %s"%(stop[j]))
            if stop[j] > beg[k] :
                endarray[2]=stop[j]
                break
        #print("endarray [end,error,stop] %s"%(endarray))
        #print("%s => Started %s Ended %s"%(action,beg[k],min(endarray)))
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
    #for i in range(0,1):
        #trackok.append(datetime.fromtimestamp(0))
        trackok.append(0)
        raok.append(0)
        decok.append(0)
        if track is not None:
            for j in range(len(track)):
                #print(track[i])
                if track[j]<beg_ok[i] :
 #                   if (beg_ok[i]-track[j])< timedelta(minutes=6):
                    if (beg_ok[i]-track[j])< (6*60):
                        trackok[i] = track[j]
                        raok[i] = ra[j]
                        decok[i] = dec[j]
                else :
                    continue

    addtext=''     
    if azparamline is not None:
        addtext = "Az " + getRegulParameters(azparam,azparamline,beg_ok[-1])
    if elparamline is not None:
        addtext += "El " + getRegulParameters(elparam,elparamline,beg_ok[-1])
 
    raok2 = None
    decok2 = None

    if lastone == 0:
        #for i in range(0,1):
        for i in range(len(end_ok)):
            #print("%s T=%s => Duration=%s Track Start %s"%(i,beg_ok[i],end_ok[i]-beg_ok[i],trackok[i]))
            begname = datetime.fromtimestamp(beg_ok[i], tz=pytz.utc)
            endname = datetime.fromtimestamp(end_ok[i], tz=pytz.utc)
            sbegname = begname.strftime("%Y%m%d_%Hh%Mm%Ss")
            sendname = endname.strftime("%Y%m%d_%Hh%Mm%Ss")
            figname = "_%s_%s"%(sbegname,sendname) + ".png"
            figname = figpre + figname.replace(":","")
            trackok2 = trackok[i]
            if ra is not None:
                raok2 = raok[i]
                decok2 = decok[i]
            #pst = pytz.timezone('UTC')
            #trackok2 = pst.localize(trackok2)
            
            if figname.find("Track") != -1 and (end_ok[i]-beg_ok[i])<5 :
                ii=0
                #print("Too short")
            else:
                tmin = beg_ok[i]
                tmax = end_ok[i]
                if zoom==2:
                    tmin = tmax-53
                    tmax = tmax-20
                if zoom==1:
                    tmin = tmax-200
                    tmax = tmax
                GenerateFig(filename,filename2,filename3,filename4,tmin,tmax,
                          cmd_status[i],trackok2,figname.replace(" ",""),fichierhtml,addtext,raok2,decok2)
    else:

        #print("%s T=%s => Duration=%s Track Start %s"%(i,beg_ok[i],end_ok[i]-beg_ok[i],trackok[i]))
        begname = datetime.fromtimestamp(beg_ok[-1], tz=pytz.utc)
        endname = datetime.fromtimestamp(end_ok[-1], tz=pytz.utc)
        sbegname = begname.strftime("%Y%m%d_%Hh%Mm%Ss")
        sendname = endname.strftime("%Y%m%d_%Hh%Mm%Ss")
        figname = "_%s_%s"%(sbegname,sendname) + ".png"
        figname = figpre + figname.replace(":","")
        trackok2 = trackok[-1]
        raok2 = raok[i]
        decok2 = decok[i]
        #pst = pytz.timezone('UTC')
        #trackok2 = pst.localize(trackok2)

        if figname.find("Track") != -1 and (end_ok[-1]-beg_ok[-1])<5 :
            ii=0
            #print("Too short")
        else:
            tmin = beg_ok[-1]
            tmax = end_ok[-1]
            if zoom==2:
                tmin = tmax-53
                tmax = tmax-20
            if zoom==1:
                tmin = tmax-200
                tmax = tmax
            GenerateFig(filename,filename2,filename3,filename4,tmin,tmax,cmd_status[-1],trackok2,figname.replace(" ",""),fichierhtml,addtext,raok2,decok2)

def starthtmlfile(fichierhtml,dirname):
    fichierhtml.write("<!DOCTYPE html>\n")
    fichierhtml.write("<html>\n")
    fichierhtml.write("<head>\n")
    fichierhtml.write("<title>Drive output %s</title>\n"%(dirname.replace("cmd.","")))
    fichierhtml.write("</head>\n")
    fichierhtml.write("<body>\n")
    now = datetime.now()
    #print("now =", now)
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    fichierhtml.write("Last update %s"%(dt_string))
    fichierhtml.write("<h1>Drive output %s</h1>\n"%(dirname.replace("cmd.","")))
    
def endhtmlfile(fichierhtml,logsorted):
    
    
    fichierhtml.write("<h2>Log</h2>\n")
    
    for i in range(0,len(logsorted)):
        if logsorted[i][1].find("Park_Out command sent") != -1 or logsorted[i][1].find("Park_In command sent") != -1 or logsorted[i][1].find("GoToPosition") != -1 or logsorted[i][1].find("Start Tracking") != -1:
            fichierhtml.write("----------------------- <br>")
        fichierhtml.write("%s %s <br>"%(logsorted[i][0].strftime("%Y-%m-%d %H:%M:%S"),logsorted[i][1]))


    fichierhtml.write("</body>\n")
    fichierhtml.write("</html>\n")
    fichierhtml.close()
        

def getAllDate(filename,filename2,filename3,filename4,filename5,lastone=0):
    
    dirname = "./DriveOutput/Log_" + filename
    dirnamehtml = dirname

    generallog.clear()

    #Genereal
    generalstop = getDate(filename,"StopDrive command sent")
    trackcmdinitiale = getDate(filename,"Start Tracking")
    gotocmdinitiale = getDate(filename,"GoToPosition")

    #Param regulation
    azparam,azparamline = getDateAndLine(filename,"Drive Regulation Parameters Azimuth")
    elparam,elparamline = getDateAndLine(filename,"Drive Regulation Parameters Elevation")
     
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

    #print(generallog)
    #print("")
    generallogsorted =sorted(generallog, key=itemgetter(0))
    #print(generallogsorted)
        
    #repos = getRepos(filename,"Taking into account displacement")

    if len(parkoutbeg) != 0 or len(parkinbeg) != 0 or len(gotobeg) != 0 or len(trackbeg) != 0:
    	if path.exists(dirname)==False :
             os.mkdir(dirname)
             
    	#print(dirname)
 
    	fichierhtml = open(dirnamehtml+".html", "w")
    	starthtmlfile(fichierhtml,filename)
    	if path.exists(dirname)==False :
    	    os.mkdir(dirname)

    	checkallactions(generallogsorted,fichierhtml)

    
    if len(trackbeg) != 0:
        if path.exists(dirname+"/Track")==False :
                os.mkdir(dirname+"/Track")
        fichierhtml.write("<h2>Track</h2>")
        print("====== Track =======")
        checkDatev2(trackcmd,trackbeg,trackend,trackerror,generalstop,track,None,filename2,filename3,filename4,filename5,dirname+"/Track"+"/Track",fichierhtml,0,"Tracking",lastone,azparam,azparamline,elparam,elparamline,ra,dec)

    if lastone ==0 :
        if len(parkoutbeg) != 0:
            if path.exists(dirname+"/Parkout")==False :
                    os.mkdir(dirname+"/Parkout")
            fichierhtml.write("<h2>Park Out</h2>")
            print("====== Parkout =======")
            checkDatev2(parkoutcmd,parkoutbeg,parkoutend,parkouterror,generalstop,None,None,filename2,filename3,filename4,filename5,dirname+"/Parkout"+"/Parkout",fichierhtml,0,"ParkOut")

        if len(parkinbeg) != 0:
            if path.exists(dirname+"/Parkin")==False :
                    os.mkdir(dirname+"/Parkin")
            fichierhtml.write("<h2>Park In</h2>")
            print("====== Parkin =======")
            checkDatev2(parkincmd,parkinbeg,parkinend,parkinerror,generalstop,None,None,filename2,filename3,filename4,filename5,dirname+"/Parkin"+"/Parkin",fichierhtml,1,"ParkIn")
            checkDatev2(parkincmd,parkinbeg,parkinend,parkinerror,generalstop,None,None,filename2,filename3,filename4,filename5,dirname+"/Parkin"+"/Parkin",fichierhtml,2,"ParkIn")

        if len(gotobeg) != 0:
            if path.exists(dirname+"/GoToPos")==False :
                    os.mkdir(dirname+"/GoToPos")
            fichierhtml.write("<h2>GoToPos</h2>")
            print("====== GoToPos =======")
            checkDatev2(gotocmd,gotobeg,gotoend,gotoerror,generalstop,None,None,filename2,filename3,filename4,filename5,dirname+"/GoToPos"+"/GoToPos",fichierhtml,0,"GoToPsition")
    
        if len(parkoutbeg) != 0 or len(parkinbeg) != 0 or len(gotobeg) != 0 or len(trackbeg) != 0:
            endhtmlfile(fichierhtml,generallogsorted)

    
    
def plottrack(year,month,day):
    shifttemps=0
    datecompact="20"+str(year)+str(month)+str(day)
    datelong="_log_"+str(year)+"_"+str(month)+"_"+str(day)+".txt"
    cmdstr = "cmd."+datecompact
    drivestr = "DrivePosition"+datecompact
    trackstr = "track"+datelong
    torquestr = "torque"+datelong
    loadpinstr = "R_loadpin"+datelong
    print("getAllDate %s %s %s %s %s"%(cmdstr,drivestr,loadpinstr,trackstr,torquestr))
    getAllDate(cmdstr,drivestr,loadpinstr,trackstr,torquestr)

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def plotstrangefeature():
    zd_pbaz.clear()
    az_pbaz.clear()
    T_pbaz.clear()
    zd_pbzd.clear()
    az_pbzd.clear()
    T_pbzd.clear()
    az_ok.clear()
    azrad_ok.clear()
    zd_ok.clear()
    erraz_ok.clear()
    errzd_ok.clear()
    start_date = date(2020,6,1)
#    start_date = date(2019,6,1)
    end_date = date(2020,7,29)
    for single_date in daterange(start_date, end_date):
        print(single_date.strftime("%Y-%m-%d"))
        plottrack(single_date.strftime("%y"),single_date.strftime("%m"),single_date.strftime("%d"))
    fig, host = plt.subplots(figsize=(20,6), dpi=80)
    fig.subplots_adjust(right=0.75)
    pl0 = plt.scatter(az_ok,zd_ok, marker='.',color='gainsboro',label='All point')
    pl1 = plt.scatter(az_pbaz,zd_pbaz, marker='o',color='red',label='Azimuth Error')
    pl2 = plt.scatter(az_pbzd,zd_pbzd, marker='o',color='blue',label='ZD Error')
    lines = [pl1,pl2,pl0]
    host.legend(lines, [l.get_label() for l in lines],fontsize=13)
    plt.show()    

    
    


