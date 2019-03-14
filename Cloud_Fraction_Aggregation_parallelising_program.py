
# coding: utf-8

# In[1]:


from pyhdf.SD import SD, SDC
import pandas as pd
import numpy as np
import dask.array as da
import dask.dataframe as dd
import time
import math
import graphviz
import os,datetime,sys,fnmatch

# Read Function For MODO3 & MODO6 Files.
def read_MODIS_level2_data(MOD06_files,MOD03_files):
    print('Reading The Cloud Mask From MOD06_L2 Product:')
    myd06 = SD(MOD06_files)
    CM=myd06.select('Cloud_Mask_1km').get() # Reading Specific Variable 'Cloud_Mask_1km'.
    CM   = (np.array(CM[:,:,0],dtype='byte') & 0b00000110) >>1
    CM = np.array(CM).byteswap().newbyteorder()
    print('The Level-2 Cloud Mask Array Shape',CM.shape)
    print(' ')
    
    print('reading the lat-lon from MOD03 product')
    Modo3=SD(MOD03_files)
    latitude=Modo3.select('Latitude').get()
    longitude=Modo3.select('Longitude').get()
    latitude = np.array(latitude).byteswap().newbyteorder() # Addressing Byteswap For Big Endian Error.
    longitude = np.array(longitude).byteswap().newbyteorder() # Addressing Byteswap For Big Endian Error.
    print('The Level-2 Latitude-Longitude Array Shape',latitude.shape)
    print(' ')

    return latitude,longitude,CM

#Function for processing the cloud Fraction

def value_locate(refx, x):
    refx = np.array(refx)
    x = np.array(x)
    loc = np.zeros(len(x), dtype='int')

    for i in range(len(x)):
        ix = x[i]
        ind = ((refx - ix) <= 0).nonzero()[0]
        if len(ind) == 0:
            loc[i] = -1
        else: loc[i] = ind[-1]

    return loc

def division(n, d):

    div = np.zeros(len(d))
    for i in range(len(d)):
        if d[i] >0:
          div[i]=n[i]/d[i]
        else: div[i]=None 

    return div

def countzero(x, axis=1):
    #print(x)
    count0 = 0
    count1 = 0
    for i in x:
        if i <= 1:
            count0 +=1
    #print(count0/len(x))
    return count0/len(x)

MOD03_path = 'C:/Users/deepu/Dask Testcases/test/'
print(MOD03_path)

MOD06_path = 'C:/Users/deepu/Dask Testcases/test/'
print(MOD06_path)

satellite = 'Aqua'

yr = [2008]
mn = [1] #np.arange(1,13)  #[1]
dy = [1] #np.arange(1,32) # [1] #np.arange(1,31)
# latitude and longtitude boundaries of level-3 grid
lat_bnd = np.arange(-90,91,1)
lon_bnd = np.arange(-180,180,1)
nlat = 180
nlon = 360

TOT_pix      = np.zeros(nlat*nlon)
CLD_pix      = np.zeros(nlat*nlon)


MOD03_fp = 'MYD03.A*.hdf'
MOD06_fp = 'MYD06_L2.A*.hdf'
MOD03_fn, MOD06_fn =[],[]
MOD03_fn2, MOD06_fn2 =[],[]
for MOD06_flist in  os.listdir(MOD06_path):
    if fnmatch.fnmatch(MOD06_flist, MOD06_fp):
        MOD06_fn = MOD06_flist
        MOD06_fn2.append(MOD06_flist)
for MOD03_flist in  os.listdir(MOD03_path):
    if fnmatch.fnmatch(MOD03_flist, MOD03_fp):
        MOD03_fn = MOD03_flist
        MOD03_fn2.append(MOD03_flist)
if MOD03_fn and MOD06_fn:
    # if both MOD06 and MOD03 products are in the directory
    print('reading level 2 geolocation and cloud data')
    print(MOD06_fn)
    print(MOD03_fn)
    Lat,Lon,CM = read_MODIS_level2_data(MOD06_path+MOD06_fn,MOD03_path+MOD03_fn)
    
MOD03_path = 'C:/Users/deepu/Dask Testcases/test/'
MOD06_path = 'C:/Users/deepu/Dask Testcases/test/'
satellite = 'Aqua'

yr = [2008]
mn = [1] #np.arange(1,13)  #[1]
dy = [1] #np.arange(1,32) # [1] #np.arange(1,31)
lat_bnd = np.arange(-90,91,1)# latitude and longtitude boundaries of level-3 grid
lon_bnd = np.arange(-180,180,1)
nlat = 180
nlon = 360

TOT_pix      = np.zeros(nlat*nlon)
CLD_pix      = np.zeros(nlat*nlon)

MOD03_fp = 'MYD03.A*.hdf'
MOD06_fp = 'MYD06_L2.A*.hdf'
MOD03_fn, MOD06_fn =[],[]
#MOD03_fn2, MOD06_fn2 =[],[]
MOD03_fn2, MOD06_fn2 =[],[]
for MOD06_flist in  os.listdir(MOD06_path):
    if fnmatch.fnmatch(MOD06_flist, MOD06_fp):
        MOD06_fn = MOD06_flist
        MOD06_fn2.append(MOD06_flist)
        #print(MOD06_fn)
for MOD03_flist in  os.listdir(MOD03_path):
    if fnmatch.fnmatch(MOD03_flist, MOD03_fp):
        MOD03_fn = MOD03_flist
        MOD03_fn2.append(MOD03_flist)
        #print(MOD03_fn)
if MOD03_fn and MOD06_fn:
    # if both MOD06 and MOD03 products are in the directory
    print('Reading Level 2 GeoLocation & Cloud Data')
    #print(MOD06_fn)
    #print(MOD03_fn)
    Lat,Lon,CM = read_MODIS_level2_data(MOD06_path+MOD06_fn,MOD03_path+MOD03_fn)
    

Lon.shape #Checking for the correctness of the longitude shape
Lat.shape #Checking for the correctness of the latitude shape
CM.shape #Checking for the correctness of the CM shape


myd06_name = 'C:/Users/deepu/Dask Testcases/test/'
cm = np.array(2)
cm1 = np.array(2)
cmr = np.array(2)
for MOD06_file in MOD06_fn2:
    MOD06_file2 = myd06_name + MOD06_file
    print(MOD06_file2)
    myd06 = SD(MOD06_file2)
    
    CM=myd06.select('Cloud_Mask_1km').get()
    CM=np.array(CM[:,:,:].data,dtype='byte')# Reading Specific Variable 'Cloud_Mask_1km'.
    #CMr = (np.array(CM[:,:,0],dtype='byte'))
    #CM1   = (np.array(CM[:,:,0],dtype='byte') & 0b00000001)
    CM   = (np.array(CM[:,:,0],dtype='byte') & 0b00000110) >>1
    CM = np.array(CM).byteswap().newbyteorder()
    CM = np.ravel(CM) # Changing The Shape Of The Variable 'CloudMask'.
    #ar.append(CM)
    cm = np.append(cm, CM)
    print(cm)
    print('level-2 cloud mask array shape',cm.shape)
    
myd03_name = 'C:/Users/deepu/Dask Testcases/test/'
lat = np.array(2)
lon = np.array(2)
for MOD03_file in MOD03_fn2:
    MOD03_file2 = myd03_name + MOD03_file
    myd03 = SD(MOD03_file2)
    Lat=myd03.select('Latitude').get() # Reading Specific Variable 'Latitude'.
    latitude = np.ravel(Lat) # Changing The Shape Of The Variable 'Latitude'.
    latitude = np.array(latitude).byteswap().newbyteorder() # Addressing Byteswap For Big Endian Error.
    lat = np.append(lat, latitude)
    print('Latitude Shape Is: ',lat.shape)

    Lon=myd03.select('Longitude').get() # Reading Specific Variable 'Longitude'.
    longitude = np.ravel(Lon) # Changing The Shape Of The Variable 'Longitude'.
    longitude = np.array(longitude).byteswap().newbyteorder() # Addressing Byteswap For Big Endian Error.
    lon = np.append(lon, longitude)
    print('Longitude Shape Is: ',lon.shape)
    
#Verfying the aggregation of the files
lat.shape
lon.shape
cm.shape

d = {'Latitude' :  pd.Series(lat), 'Longitude' : pd.Series(lon),'CM':pd.Series(cm)} #Reading the values in a pandas dataframe
df = pd.DataFrame(d,columns=['Latitude', 'Longitude','CM']) #Creating a pandas dataframe

df1=dd.from_pandas(df,npartitions=1) #Creating the Dask Dataframes out of pandas dataframe
df1=df1.astype(np.int8)
df1.compute()

df1.groupby(['Longitude','Latitude']).aggregate('count').compute() #Applying groupby to get the cloud mask values in different latitude and longitude

def countzero(x, axis=1):
    #print(x)
    count0 = 0
    count1 = 0
    for i in x:
        if i <= 1:
            count0 +=1
    #print(count0/len(x))
    return count0/len(x)

df1.groupby(['Longitude','Latitude']).CM.apply(countzero,meta=('CM', 'int8')).compute() #Applying the reduce function to calculate the Cloud Fraction

