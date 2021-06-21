import json
import boto3
import os
import glob
import pandas as pd

s3c = boto3.client('s3')
s3r = boto3.resource('s3')

bins = [40, 54, 69, 180, 248, 400] 



def cpToTmpFolder(sourceBucket, fileName):#Copy File to Tmp Folder
    print("copying: ", sourceBucket,"/",fileName,"     to  /tmp/",fileName)
    s3c.download_file(sourceBucket, fileName, '/tmp/' + fileName)
    return True

def cpFrmTmpToS3(Bucket, FileName):#Copy from Tmp To S3
    tmpfiles = glob.glob(os.path.join('/tmp/*'))
    print("all files in tmp: ", tmpfiles)
    print("copying file: ",  FileName, "    to S3: ", Bucket,  "/", FileName)
    s3r.Object(Bucket, FileName).put(Body=open('/tmp/' + FileName, 'rb'))
    return

def rmFileFrmTmp(fileName):
    try:
        os.remove(fileName)
        print("Deleted ", fileName, "  from tmp")
    except Exception as e:
        print("Unable to delete file from Tmp: ", fileName, "  Error: ", e)
    return


def createDFfrmJson(jsnfile):
    print("Working on Json file: ", jsnfile)
    with open(jsnfile, 'r') as f:
        data = json.load(f)
        rmFileFrmTmp(jsnfile)
    df = pd.DataFrame(data)
    df = df.dropna(axis='columns', how ='all') #drop columns if all values are NULL
    #Clean column names if they have brackets or white space (crude way, there are better ways available) 
    df.columns=df.columns.str.replace(r"\(.*\)","")

    df.columns=df.columns.str.replace('/','')
    df.columns=df.columns.str.replace(' ','')
    print("Shape of ", jsnfile, "  ", df.shape)
    return df



def filterOnColVals(df, filterOnCol, filterOnVals):
    print("Will be filtering on column: ", filterOnCol, " for values: ", filterOnVals)
    df[filterOnCol] = df[filterOnCol].astype(str)
    df = df[df[filterOnCol].isin( filterOnVals )]
    df = df.dropna(axis='columns', how ='all') #drop columns if all values are NULL
    return df

def createTimeCol(df, frmTimeStampCol='timestamp'):
    newtimestamp = frmTimeStampCol + 'T'
    df[newtimestamp] = pd.to_datetime(df[frmTimeStampCol]) 
    df['Time'] = df[newtimestamp].dt.time
    df['Time'] = df['Time'].astype(str)
    return df

def createDateCol(df, frmTimeStampCol='timestamp'):
    newtimestamp = frmTimeStampCol + 'D'
    df[newtimestamp] = pd.to_datetime(df[frmTimeStampCol]) 
    df['Date'] = pd.to_datetime( df[newtimestamp].dt.date )
    return df

def keepColsOfDF(df, keepCols):
    df.drop(df.columns.difference(keepCols), 1, inplace=True)
    df = df[keepCols]
    print("Shape after filter and keepCols: ", df.shape)
    return df

def filterOnTimeRange(df, startTime='06:00:00', endTime='11:00:00'):
    df = df[ df['Time'] >= startTime ]
    df = df[ df['Time'] < endTime ]
    return df

def getLables(bins):
    bins[0] = bins[0]-1 
    labels = [ str( str(bins[i]+1) + "-" + str(bins[i+1]) ) for i in range( 0, len(bins)-1 ) ]
    return labels
lables = getLables(bins)
lastlabel = lables[-1]


def getPercentageDF(df, OverAll=""):
    if len(OverAll):
        df['Date'] = min ( df['Date'] )    
        
    df = df.set_index('Date')
    df.index = pd.to_datetime(df.index)
    df = df.pivot_table(values='amount', index=df.index, columns='TIF', aggfunc='size')
    df = df.sort_values(by=['Date'], ascending=False)
    
    df.columns = df.columns.astype(str)
    rCols =  list(df.columns.values) 
    df = df.div(df.sum(1), 0).mul(100).round(0).assign(Sum=lambda df: df.sum(axis=1))
    df = df.reset_index()

    df[lastlabel] = df.apply(lambda x: x[lastlabel]-1 if x['Sum']==101 else x[lastlabel], axis=1)
    df[lastlabel] = df.apply(lambda x: x[lastlabel]+1 if x['Sum']==99 else x[lastlabel], axis=1)
    
    dropCols = ['Sum']
    df.drop(dropCols, axis=1, inplace=True)
    df = df.fillna(0)
    df['Date'] = pd.to_datetime( df['Date'] ).dt.date
    if len(OverAll):
        df['Date'] = OverAll        
    return df
    

def lambda_handler(event, context):
    bucket = 'dt-request-emishra'
    key = '670_Jan1to15.json'
    keycsv = key.replace(".json", ".csv")
    
    if cpToTmpFolder(bucket, key):
        print("Copied file: ", bucket, "/", key, "     to       ", "/tmp/", key)
    else:
        print("Unable to copy: ", bucket, "/", key, "to tmp folder")
    
    jsnfile = "/tmp/" + key
    df = createDFfrmJson(jsnfile)
    
    filterOnCol = "className"
    filterOnVals = ["GlucoseSensorData","GlucoseSensorDataHigh","GlucoseSensorDataLow"]
    df = filterOnColVals(df, filterOnCol, filterOnVals)

    df = createTimeCol(df)
    df = createDateCol(df)    
    keepCols = ['timestamp', 'Date', 'Time', 'amount']
    df = keepColsOfDF(df, keepCols)
    
    #Sensor Glucose Overlays
    print(df.head())
    tmpcsv =  "SensorGlucoseOverlays_" + str(keycsv)
    fileNameCsv = "/tmp/" + tmpcsv
    print("TimeStamp Max/Min  \n"  , df['timestamp'].min(),  df['timestamp'].max() )
    print("Time Max/Min  \n"  , df['Time'].min(),  df['Time'].max() )
    df.to_csv(fileNameCsv, index=False, header=True)
    cpFrmTmpToS3(bucket, tmpcsv)
    rmFileFrmTmp(fileNameCsv)
    
    
    
    #Post Meal OutCome Plots
    segment = ["Breakfast", "Lunch", "Dinner", "Overnight"]
    startTime = ['06:00:00', '11:00:00', '16:00:00', '22:00:00', '00:00:00'] #Will be included 
    endTime =   ['11:00:00', '16:00:00', '22:00:00', '23:59:59', '06:00:00'] #Will not be included
    
    for myseg in range(0, len(segment)):
        tmpcsv =  "PostMealOutcome_" + segment[myseg] + "_" + str(keycsv)
        fileNameCsv = "/tmp/" + tmpcsv
        if myseg==3:
            dfx1 = filterOnTimeRange(df, startTime[myseg], endTime[myseg])
            dfx2 = filterOnTimeRange(df, startTime[myseg+1], endTime[myseg+1])
            dfx = pd.concat([dfx1, dfx2])
        else:
            dfx = filterOnTimeRange(df, startTime[myseg], endTime[myseg])
        dfx.to_csv(fileNameCsv, index=False, header=True)
        cpFrmTmpToS3(bucket, tmpcsv)
        rmFileFrmTmp(fileNameCsv)
    
    #Post Meal OutCome Plots RHS 
    for myseg in range(0, len(segment)):
        tmpcsv = "PostMealOutcome_TIF_" + segment[myseg] + "_" + str(keycsv)
        fileNameCsv = "/tmp/" + tmpcsv
        if myseg==3:
            dfx1 = filterOnTimeRange(df, startTime[myseg], endTime[myseg])
            dfx2 = filterOnTimeRange(df, startTime[myseg+1], endTime[myseg+1])
            dfx = pd.concat([dfx1, dfx2])
        else:
            dfx = filterOnTimeRange(df, startTime[myseg], endTime[myseg])

        keepCols = ['Date', 'amount']
        dfx = keepColsOfDF(dfx, keepCols)
        dfx['TIF'] = pd.cut(dfx['amount'], bins, labels=lables)
        dfx = getPercentageDF(dfx, segment[myseg]).reset_index()
        dfx.drop('index', axis=1, inplace=True)
        dfx.rename(columns = {'Date':'Segment'}, inplace = True)
        dfx.name = None
        dfx.to_csv(fileNameCsv, index=False, header=True)
        cpFrmTmpToS3(bucket, tmpcsv)
        rmFileFrmTmp(fileNameCsv)
    
    
    
    #Time in Hypo
    df['TimeBelow54'] = df['amount'].apply(lambda x: True if x < 54 else False)
    df['TimeBelow70'] = df['amount'].apply(lambda x: True if (x < 70) and (x >= 54) else False)

    keepCols = ['Time', 'amount', 'TimeBelow54', 'TimeBelow70']
    dfg = keepColsOfDF(df, keepCols)
    dfg["Time"] = pd.to_datetime(dfg["Time"])
    dfg.set_index("Time", inplace=True)

    aggdict = {"amount":"count", "TimeBelow54":"sum", "TimeBelow70":"sum"}
    dfg = dfg.groupby(pd.Grouper(freq='5Min',closed='right',label='right')).agg(aggdict).reset_index()
    dfg["TimeBelow54_p"] = round( ( dfg["TimeBelow54"] * 100 / dfg['amount'] ), 2 )
    dfg["TimeBelow70_p"] = round( ( dfg["TimeBelow70"] * 100 / dfg['amount'] ), 2 )
    dfg["Time"] = pd.to_datetime(dfg["Time"])
    dfg = createTimeCol(dfg, "Time")
    keepCols = ['Time','TimeBelow54_p','TimeBelow70_p']
    dfg = keepColsOfDF(dfg, keepCols)
    tmpcsv =  "TimeInHypo_" + str(keycsv)
    fileNameCsv = "/tmp/" + tmpcsv
    dfg.to_csv(fileNameCsv, index=False, header=True)
    cpFrmTmpToS3(bucket, tmpcsv)
    rmFileFrmTmp(fileNameCsv)
    
    
    #Time in Hyper
    df['TimeAbove180'] = df['amount'].apply(lambda x: True if x > 250 else False)
    df['TimeAbove250'] = df['amount'].apply(lambda x: True if (x > 180) and (x <= 250) else False)

    keepCols = ['Time', 'amount', 'TimeAbove180', 'TimeAbove250']
    dfg = keepColsOfDF(df, keepCols)
    dfg["Time"] = pd.to_datetime(dfg["Time"])
    dfg.set_index("Time", inplace=True)

    aggdict = {"amount":"count", "TimeAbove180":"sum", "TimeAbove250":"sum"}
    dfg = dfg.groupby(pd.Grouper(freq='5Min',closed='right',label='right')).agg(aggdict).reset_index()
    dfg["TimeAbove180_p"] = round( ( dfg["TimeAbove180"] * 100 / dfg['amount'] ), 2 )
    dfg["TimeAbove250_p"] = round( ( dfg["TimeAbove250"] * 100 / dfg['amount'] ), 2 )
    dfg["Time"] = pd.to_datetime(dfg["Time"])
    dfg = createTimeCol(dfg, "Time")
    keepCols = ['Time','TimeAbove180_p','TimeAbove250_p']
    dfg = keepColsOfDF(dfg, keepCols)
    tmpcsv =  "TimeInHyper_" + str(keycsv)
    fileNameCsv = "/tmp/" + tmpcsv
    dfg.to_csv(fileNameCsv, index=False, header=True)
    cpFrmTmpToS3(bucket, tmpcsv)
    rmFileFrmTmp(fileNameCsv)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps(key)
    }
