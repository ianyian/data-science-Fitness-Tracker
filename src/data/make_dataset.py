import pandas as pd
from glob import glob

# --------------------------------------------------------------
# Read single CSV file
# --------------------------------------------------------------
single_file_acc = pd.read_csv("../../data/raw/MetaMotion/A-bench-heavy2-rpe8_MetaWear_2019-01-11T16.10.08.270_C42732BE255C_Accelerometer_12.500Hz_1.4.4.csv")

single_file_gyr = pd.read_csv("../../data/raw/MetaMotion/A-bench-heavy2-rpe8_MetaWear_2019-01-11T16.10.08.270_C42732BE255C_Gyroscope_25.000Hz_1.4.4.csv")
# --------------------------------------------------------------
# List all data in data/raw/MetaMotion
# --------------------------------------------------------------
files = glob("../../data/raw/MetaMotion/*.csv")
len(files)


# --------------------------------------------------------------
# Extract features from filename
# --------------------------------------------------------------

data_path = "../../data/raw/MetaMotion/"
f = files[0]

participant = f.split("-")[0].replace("\\","/").replace(data_path,"")
label = f.split("-")[1]
category = f.split("-")[2].rstrip("123") # remove value from eight with either 1 or 2 or 3

df = pd.read_csv(f) # df = data frame
df["participant"] = participant # create new column with value
df["label"] = label # create new column with value
df["category"] = category # create new column with value


# --------------------------------------------------------------
# Read all files
# --------------------------------------------------------------

acc_df = pd.DataFrame()
gyr_df = pd.DataFrame()

files = glob("../../data/raw/MetaMotion/*.csv")
data_path = "../../data/raw/MetaMotion/"

acc_set = 1
gyr_set = 1

for f in files:

    participant = f.split("-")[0].replace("\\","/").replace(data_path,"")
    label = f.split("-")[1]
    category = f.split("-")[2].rstrip("123").rstrip("_MetaWear_2019") # remove value from eight with either 1 or 2 or 3
    
    df = pd.read_csv(f)
    
    df["participant"] = participant
    df["label"] = label
    df["category"] = category
    
    if "Accelerometer" in f:
        df["set"] = acc_set
        acc_set += 1
        acc_df = pd.concat([acc_df,df])
        
    if "Gyroscope" in f:
        df["set"] = gyr_set
        gyr_set += 1
        gyr_df = pd.concat([gyr_df,df])
        
## acc_df[acc_df["set"] == 10 ]



# --------------------------------------------------------------
# Working with datetimes
# --------------------------------------------------------------

acc_df.info() # display acc_df table structure

pd.to_datetime(df["epoch (ms)"],unit="ms") # convert datatype to datetime ms
pd.to_datetime(df["time (01:00)"]).dt.month # convert to month

acc_df.index = pd.to_datetime(acc_df["epoch (ms)"],unit="ms")
gyr_df.index = pd.to_datetime(gyr_df["epoch (ms)"],unit="ms")

del acc_df["epoch (ms)"]
del acc_df["time (01:00)"]
del acc_df["elapsed (s)"]

del gyr_df["epoch (ms)"]
del gyr_df["time (01:00)"]
del gyr_df["elapsed (s)"]

# --------------------------------------------------------------
# Turn into function
# --------------------------------------------------------------

files = glob("../../data/raw/MetaMotion/*.csv")
data_path = "../../data/raw/MetaMotion/"

def read_data_from_files(files):
    
    acc_df = pd.DataFrame()
    gyr_df = pd.DataFrame()

    acc_set = 1
    gyr_set = 1

    for f in files:

        participant = f.split("-")[0].replace("\\","/").replace(data_path,"")
        label = f.split("-")[1]
        category = f.split("-")[2].rstrip("123").rstrip("_MetaWear_2019") # remove value from eight with either 1 or 2 or 3
            
        df = pd.read_csv(f)
            
        df["participant"] = participant
        df["label"] = label
        df["category"] = category
            
        if "Accelerometer" in f:
            df["set"] = acc_set
            acc_set += 1
            acc_df = pd.concat([acc_df,df])
                
        if "Gyroscope" in f:
            df["set"] = gyr_set
            gyr_set += 1
            gyr_df = pd.concat([gyr_df,df])
                    
    acc_df.index = pd.to_datetime(acc_df["epoch (ms)"],unit="ms")
    gyr_df.index = pd.to_datetime(gyr_df["epoch (ms)"],unit="ms")
        
    del acc_df["epoch (ms)"]
    del acc_df["time (01:00)"]
    del acc_df["elapsed (s)"]

    del gyr_df["epoch (ms)"]
    del gyr_df["time (01:00)"]
    del gyr_df["elapsed (s)"]

    return acc_df, gyr_df

acc_df , gyr_df = read_data_from_files(files)

# --------------------------------------------------------------
# Merging datasets
# --------------------------------------------------------------

data_merged = pd.concat([acc_df.iloc[:,:3],gyr_df], axis=1)

# data_merged.dropna() # display those merged all data record
# data_merged.head(50) # top 50 records

# rename column
data_merged.columns = [
    "acc_x",
    "acc_y",
    "acc_z",
    "gyr_x",
    "gyr_y",
    "gyr_z",
    "label",
    "category",
    "participant",
    "set",
]


# --------------------------------------------------------------
# Resample data (frequency conversion)
# --------------------------------------------------------------

# Accelerometer:    12.500HZ = 1/12.5 = 0.04 sec per measure = 加速度计
# Gyroscope:        25.000Hz = 1/25 = 0.08 sec per measure = 陀螺仪

## del data_merged["label"]
## del data_merged["category"]
## del data_merged["participant"]
    
# data_merged[:100]

data_merged.iloc[1]
data_merged.loc[:,"acc_y"]
data_merged.iloc[3]

# top 100 record , then within S = second value do the mean 
data_merged[:100].resample(rule="S").mean() 
data_merged[:100].resample(rule="200ms").mean() 

data_merged.columns # display all column in this dataset

sampling = {
    "acc_x":"mean",
    "acc_y":"mean",
    "acc_z":"mean",
    "gyr_x":"mean",
    "gyr_y":"mean",
    "gyr_z":"mean", # 
    "label":"last",
    "category":"last",
    "participant":"last",
    "set":"last"    # within 200ms, get the last timestamp
}

data_merged[:1000].resample(rule="200ms").mean()
data_merged[:1000].resample(rule="200ms").apply(sampling) #this is time series , which going to auto populate gap


# split by day

days = [g for n , g in data_merged.groupby(pd.Grouper(freq="D"))]

# pull 200ms data then chop into "day" as unit
data_resampled = pd.concat([df.resample(rule="200ms").apply(sampling).dropna() for df in days])

data_resampled.info()

# changed column "set" datatype from float64 to int
data_resampled["set"] = data_resampled["set"].astype("int")

data_resampled.info()


# --------------------------------------------------------------
# Export dataset
# -------------------------------------------- ------------------

# pickle file = python pickle module is used for serializing
# and de-serializing a python object structure.
# any object in python can be pickled so that it can be
# saved on disk. abel to convert to (list, dict, etc)

data_resampled.to_pickle("../../data/interim/01_proceed.pkl")
