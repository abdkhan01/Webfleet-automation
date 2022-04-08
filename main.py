import requests
import pandas as pd
import io
import os
import numpy as np
from datetime import datetime
import datetime as dt
from sharepoint_util import uploadToSharepoint
import time


VERSIONFILEPATH= "Utility Files/Version data.xlsx"
ACCOUNTSFILEPATH= "Utility Files/API accounts.csv"

outdir = './OutputTables'
if not os.path.exists(outdir):
    os.mkdir(outdir)

def getContracts(account_df, navigation_device, telematic_device ):
    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    print(accounts)
    for i in range(accounts):
        print(i)
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()

        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)
        r = requests.get(f"https://csv.webfleet.com/extern?lang=en&account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&action=showContracts&outputformat=csv&freetext=Leipzig")
        data = io.StringIO(r.text)
        df = pd.read_csv(data, sep=';')
        if df.empty:

            print("test",r.text)
            continue
        else:

            df["pnd_ncversion"] = df["pnd_ncversion"].apply(str)

            df["Telematic device and software version"] = df['obu_type'] + " | " + df["obu_appversion"]
            df["Navigation device and software version and map data version"] = df['pnd_type'] + " | " + df[
                "pnd_ncversion"] + " | " + df["pnd_mapversion"]

            df = df.replace(" | "," ")
            df = df.replace(" | | "," ")
            df = df.replace(np.nan, '', regex=True)

            df['Telematic device and software version'] = df['Telematic device and software version'].astype(str)

            df = df.merge(telematic_device[["Telematic device and software version", "Update version"]],
                          on="Telematic device and software version", how="left")
            df.rename(columns={'Update version': 'Telematic device update status'}, inplace=True)
            df = df.merge(navigation_device[["Navigation device and software version and map data version", "Update version",
                                             "Update pro version"]],
                          on="Navigation device and software version and map data version", how="left")
            df.rename(columns={'Update version': 'Navigation device update status',
                               'Update pro version': 'Navigation device pro update status'}, inplace=True)
            df["AccountId"] = ACCOUNT_ID

            if(flag == True):
                final_df = df
                flag = False
            else:
                final_df = pd.concat([final_df, df])

    return final_df

def getOrderReportExtern(account_df):
    current_date = datetime.today().strftime('%d/%m/%YT%H:%M:%S')
    timeparameter = f'range_pattern=ud&rangefrom_string=01/01/2020T00:00:00&rangeto_string={current_date}'
    print(timeparameter)
    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    for i in range(accounts):
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()

        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)
        r = requests.get(f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=en&action=showOrderReportExtern&{timeparameter}")
        data = io.StringIO(r.text)
        df = pd.read_csv(data,sep=';')
        if df.empty:
            print("test",r.text)
            continue

        else:
            df["AccountId"] = ACCOUNT_ID
            if (flag == True):
                final_df = df
                flag = False
            else:
                final_df = pd.concat([final_df, df])


    return final_df

def getTripReportExtern(account_df):
    current_date = datetime.today().strftime('%d/%m/%YT%H:%M:%S')
    d = dt.timedelta(days=28)
    a = datetime.today() - d
    fromtime = a.strftime('%d/%m/%YT%H:%M:%S')
    timeparameter = f'range_pattern=ud&rangefrom_string={fromtime}&rangeto_string={current_date}'
    # print(timeparameter)

    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    for i in range(accounts):
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()

        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)
        r = requests.get(f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=en&action=showTripReportExtern&{timeparameter}")
        data = io.StringIO(r.text)
        df = pd.read_csv(data,sep=';')
        if df.empty:
            print("test",r.text)
            continue

        else:
            df["AccountId"] = ACCOUNT_ID
            if (flag == True):
                final_df = df
                flag = False
            else:
                final_df = pd.concat([final_df, df])


    return final_df

def getshowMaintenanceTasks(account_df):
    current_date = datetime.today().strftime('%d/%m/%YT%H:%M:%S')
    d = dt.timedelta(days=365)
    a = datetime.today() - d
    fromtime = a.strftime('%d/%m/%YT%H:%M:%S')
    timeparameter = f'range_pattern=ud&rangefrom_string={fromtime}&rangeto_string={current_date}'
    # print(timeparameter)

    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    for i in range(accounts):
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()

        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)
        r = requests.get(f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=en&action=showMaintenanceTasks&{timeparameter}")
        data = io.StringIO(r.text)
        df = pd.read_csv(data,sep=';')
        if df.empty:
            print("test",r.text)
            continue

        else:
            df["AccountId"] = ACCOUNT_ID
            if (flag == True):
                final_df = df
                flag = False
            else:
                final_df = pd.concat([final_df, df])


    return final_df

# objects limit
def getTripSummaryReportExtern(account_df):
    current_date = datetime.today().strftime('%d/%m/%YT%H:%M:%S')
    d = dt.timedelta(days=364)
    a = datetime.today() - d
    fromtime = a.strftime('%d/%m/%YT%H:%M:%S')
    timeparameter = f'range_pattern=ud&rangefrom_string={fromtime}&rangeto_string={current_date}'
    # print(timeparameter)

    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    for i in range(accounts):
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()
        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)

        #Objects
        r = requests.get(f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=de&action=showObjectReportExtern")
        data = io.StringIO(r.text)
        objects = pd.read_csv(data,sep=';')
        if objects.empty:
            print("test", r.text)
            continue

        else:
            objectNos = objects["objectno"].unique()
            print(len(objectNos))
            i = 0
            for objectno in objectNos:
                if(i>=10):
                    break
                else:
                    i=i+1
                    r = requests.get(f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=en&action=showTripSummaryReportExtern&objectno={objectno}&{timeparameter}")
                    data = io.StringIO(r.text)
                    df = pd.read_csv(data,sep=';')
                    if df.empty:
                        print("test", r.text)
                        continue
                    else:
                        df["AccountId"] = ACCOUNT_ID

                        if (flag == True):
                            final_df = df
                            flag = False
                        else:
                            final_df = pd.concat([final_df, df])

    return final_df

def getshowAddressGroupReport(account_df):
    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    print(accounts)
    for i in range(accounts):
        # print(i)
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()

        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)
        r = requests.get(f"https://csv.webfleet.com/extern?lang=en&account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&action=showAddressGroupReportExtern&outputformat=csv&freetext=Leipzig")
        data = io.StringIO(r.text)
        df = pd.read_csv(data, sep=';')
        if df.empty:

            print("test",r.text)
            continue
        else:

            df["AccountId"] = ACCOUNT_ID

            if(flag == True):
                final_df = df
                flag = False
            else:
                final_df = pd.concat([final_df, df])

    return final_df

def getshowAddressReport(account_df):
    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    print(accounts)
    for i in range(accounts):
        print(i)
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()

        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)
        r = requests.get(f"https://csv.webfleet.com/extern?lang=en&account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&action=showAddressReportExtern&outputformat=csv&freetext=Leipzig")
        data = io.StringIO(r.text)
        df = pd.read_csv(data, sep=';')
        if df.empty:

            print("test",r.text)
            continue
        else:

            df["AccountId"] = ACCOUNT_ID

            if(flag == True):
                final_df = df
                flag = False
            else:
                final_df = pd.concat([final_df, df])

    return final_df

def getshowAddressGroupAddressReport(account_df):
    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    print(accounts)
    for i in range(accounts):
        print(i)
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()

        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)
        r = requests.get(f"https://csv.webfleet.com/extern?lang=en&account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&action=showAddressGroupAddressReportExtern&outputformat=csv&freetext=Leipzig")
        data = io.StringIO(r.text)
        df = pd.read_csv(data, sep=';')
        if df.empty:

            print("test",r.text)
            continue
        else:

            df["AccountId"] = ACCOUNT_ID

            if(flag == True):
                final_df = df
                flag = False
            else:
                final_df = pd.concat([final_df, df])

    return final_df

# objects limit
def getshowAccelerationEvents(account_df):
    current_date = datetime.today().strftime('%d/%m/%YT%H:%M:%S')
    d = dt.timedelta(days=28)
    a = datetime.today() - d
    fromtime = a.strftime('%d/%m/%YT%H:%M:%S')
    timeparameter = f'range_pattern=ud&rangefrom_string={fromtime}&rangeto_string={current_date}'
    # print(timeparameter)

    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    for i in range(accounts):
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()
        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)

        #Objects
        r = requests.get(f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=de&action=showObjectReportExtern")
        data = io.StringIO(r.text)
        objects = pd.read_csv(data,sep=';')
        if objects.empty:
            print("test", r.text)
            continue

        else:
            objectNos = objects["objectno"].unique()
            print(len(objectNos))
            i = 0
            for objectno in objectNos:
                if(i>=10):
                    break
                else:
                    i=i+1
                    r = requests.get(f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=en&action=showAccelerationEvents&objectno={objectno}&{timeparameter}")
                    data = io.StringIO(r.text)
                    df = pd.read_csv(data,sep=';')
                    if df.empty:
                        print("test", r.text)
                        continue
                    else:
                        df["AccountId"] = ACCOUNT_ID

                        if (flag == True):
                            final_df = df
                            flag = False
                        else:
                            final_df = pd.concat([final_df, df])

    return final_df

def getshowAccountOrderAutomations(account_df):
    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    print(accounts)
    for i in range(accounts):
        # print(i)
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()

        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)
        r = requests.get(f"https://csv.webfleet.com/extern?lang=en&account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&action=showAccountOrderAutomations&outputformat=csv&freetext=Leipzig")
        data = io.StringIO(r.text)
        df = pd.read_csv(data, sep=';')
        if df.empty:

            print("test",r.text)
            continue
        else:

            df["AccountId"] = ACCOUNT_ID

            if(flag == True):
                final_df = df
                flag = False
            else:
                final_df = pd.concat([final_df, df])

    return final_df

def getshowAccountOrderStates(account_df):
    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    print(accounts)
    for i in range(accounts):
        # print(i)
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()

        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)
        r = requests.get(f"https://csv.webfleet.com/extern?lang=en&account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&action=showAccountOrderStates&outputformat=csv&freetext=Leipzig")
        data = io.StringIO(r.text)
        df = pd.read_csv(data, sep=';')
        if df.empty:

            print("test",r.text)
            continue
        else:

            df["AccountId"] = ACCOUNT_ID

            if(flag == True):
                final_df = df
                flag = False
            else:
                final_df = pd.concat([final_df, df])

    return final_df

# orders limit
def getshowOrderWaypoints(account_df):
    current_date = datetime.today().strftime('%d/%m/%YT%H:%M:%S')
    d = dt.timedelta(days=364)
    a = datetime.today() - d
    fromtime = a.strftime('%d/%m/%YT%H:%M:%S')
    timeparameter = f'range_pattern=ud&rangefrom_string={fromtime}&rangeto_string={current_date}'
    # print(timeparameter)

    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    for i in range(accounts):
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()
        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)

        #Objects
        current_date = datetime.today().strftime('%d/%m/%YT%H:%M:%S')
        timeparameterr = f'range_pattern=ud&rangefrom_string=01/01/2020T00:00:00&rangeto_string={current_date}'

        r = requests.get(f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=en&action=showOrderReportExtern&{timeparameterr}")
        data = io.StringIO(r.text)
        orders = pd.read_csv(data,sep=';')
        if orders.empty:
            print("test", r.text)
            continue

        else:
            orderNos = orders["orderid"].unique()
            print(len(orderNos))
            i = 0
            for orderno in orderNos:
                print(orderno)
                if(i>=10):
                    break
                else:
                    i=i+1
                    r = requests.get(f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=en&action=showOrderWaypoints&orderid={orderno}&{timeparameter}")
                    data = io.StringIO(r.text)
                    df = pd.read_csv(data,sep=';')
                    if df.empty:
                        print("test", r.text)
                        continue
                    else:
                        df["AccountId"] = ACCOUNT_ID

                        if (flag == True):
                            final_df = df
                            flag = False
                        else:
                            final_df = pd.concat([final_df, df])

    return final_df

def getShowMessages(account_df):
    current_date = datetime.today().strftime('%d/%m/%YT%H:%M:%S')
    d = dt.timedelta(days=13)
    a = datetime.today() - d
    fromtime = a.strftime('%d/%m/%YT%H:%M:%S')
    timeparameter = f'range_pattern=ud&rangefrom_string={fromtime}&rangeto_string={current_date}'

    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    print(accounts)
    for i in range(accounts):
        # print(i)
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()

        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)
        r = requests.get(f"https://csv.webfleet.com/extern?lang=en&account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&action=showMessages&outputformat=csv&freetext=Leipzig&{timeparameter}")
        data = io.StringIO(r.text)
        df = pd.read_csv(data, sep=';')
        if df.empty:

            print("test",r.text)
            continue
        else:

            df["AccountId"] = ACCOUNT_ID

            if(flag == True):
                final_df = df
                flag = False
            else:
                final_df = pd.concat([final_df, df])

    return final_df

def getshowDriverReportExtern(account_df):
    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    print(accounts)
    for i in range(accounts):
        # print(i)
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()

        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)
        r = requests.get(f"https://csv.webfleet.com/extern?lang=en&account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&action=showDriverReportExtern&outputformat=csv&freetext=Leipzig")
        data = io.StringIO(r.text)
        df = pd.read_csv(data, sep=';')
        if df.empty:

            print("test",r.text)
            continue
        else:

            df["AccountId"] = ACCOUNT_ID

            if(flag == True):
                final_df = df
                flag = False
            else:
                final_df = pd.concat([final_df, df])

    return final_df

# objects limit
def getshowVehicleReportExtern(account_df):
    current_date = datetime.today().strftime('%d/%m/%YT%H:%M:%S')
    d = dt.timedelta(days=28)
    a = datetime.today() - d
    fromtime = a.strftime('%d/%m/%YT%H:%M:%S')
    timeparameter = f'range_pattern=ud&rangefrom_string={fromtime}&rangeto_string={current_date}'
    # print(timeparameter)

    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    for i in range(accounts):
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()
        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)

        #Objects
        r = requests.get(f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=de&action=showObjectReportExtern")
        data = io.StringIO(r.text)
        objects = pd.read_csv(data,sep=';')
        if objects.empty:
            print("test", r.text)
            continue

        else:
            objectNos = objects["objectno"].unique()
            print(len(objectNos))
            i = 0
            for objectno in objectNos:
                if(i>=10):
                    break
                else:
                    i=i+1
                    r = requests.get(f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=en&action=showVehicleReportExtern&objectno={objectno}&{timeparameter}")
                    data = io.StringIO(r.text)
                    df = pd.read_csv(data,sep=';')
                    if df.empty:
                        print("test", r.text)
                        continue
                    else:
                        df["AccountId"] = ACCOUNT_ID

                        if (flag == True):
                            final_df = df
                            flag = False
                        else:
                            final_df = pd.concat([final_df, df])

    return final_df

def getshowOptiDriveIndicator(account_df):
    current_date = datetime.today().strftime('%d/%m/%YT%H:%M:%S')
    d = dt.timedelta(days=7)
    a = datetime.today() - d
    fromtime = a.strftime('%d/%m/%YT%H:%M:%S')
    timeparameter = f'range_pattern=ud&rangefrom_string={fromtime}&rangeto_string={current_date}'

    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    print(accounts)
    for i in range(accounts):
        # print(i)
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()

        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)
        r = requests.get(f"https://csv.webfleet.com/extern?lang=en&account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&action=showOptiDriveIndicator&outputformat=csv&freetext=Leipzig&{timeparameter}")
        data = io.StringIO(r.text)
        df = pd.read_csv(data, sep=';')
        if df.empty:

            print("test",r.text)
            continue
        else:

            df["AccountId"] = ACCOUNT_ID

            if(flag == True):
                final_df = df
                flag = False
            else:
                final_df = pd.concat([final_df, df])

    return final_df

def getshowDriverGroups(account_df):
    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    print(accounts)
    for i in range(accounts):
        # print(i)
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()

        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)
        r = requests.get(f"https://csv.webfleet.com/extern?lang=en&account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&action=showDriverGroups&outputformat=csv")
        data = io.StringIO(r.text)
        df = pd.read_csv(data, sep=';')
        if df.empty:

            print("test",r.text)
            continue
        else:

            df["AccountId"] = ACCOUNT_ID

            if(flag == True):
                final_df = df
                flag = False
            else:
                final_df = pd.concat([final_df, df])

    return final_df

# driver limit
def getshowDriverGroupDrivers(account_df):
    current_date = datetime.today().strftime('%d/%m/%YT%H:%M:%S')
    d = dt.timedelta(days=7)
    a = datetime.today() - d
    fromtime = a.strftime('%d/%m/%YT%H:%M:%S')
    timeparameter = f'range_pattern=ud&rangefrom_string={fromtime}&rangeto_string={current_date}'
    # print(timeparameter)

    accounts = len(account_df.index)
    flag = True
    final_df = pd.DataFrame()
    for i in range(accounts):
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()
        print(ACCOUNT_NAME,ACCOUNT_USERNAME,ACCOUNT_PASSWORD,API_KEY)

        #Objects
        r = requests.get(f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=de&action=showDriverReportExtern")
        data = io.StringIO(r.text)
        drivers = pd.read_csv(data,sep=';')
        if drivers.empty:
            print("test", r.text)
            continue

        else:
            driverNos = drivers["driverno"].unique()
            print(len(driverNos))
            i = 0
            for driverno in driverNos:
                if(i>=10):
                    break
                else:
                    i=i+1
                    r = requests.get(f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=en&action=showDriverGroupDrivers&objectno={driverno}&{timeparameter}")
                    data = io.StringIO(r.text)
                    df = pd.read_csv(data,sep=';')
                    if df.empty:
                        print("test", r.text)
                        continue
                    else:
                        df["AccountId"] = ACCOUNT_ID

                        if (flag == True):
                            final_df = df
                            flag = False
                        else:
                            final_df = pd.concat([final_df, df])

    return final_df

def objectsmethods(account_df):
    current_date = datetime.today().strftime('%d/%m/%YT%H:%M:%S')
    d28 = dt.timedelta(days=28)
    d364 = dt.timedelta(days=364)
    a28 = datetime.today() - d28
    a364 = datetime.today() - d364
    fromtime28 = a28.strftime('%d/%m/%YT%H:%M:%S')
    fromtime364 = a364.strftime('%d/%m/%YT%H:%M:%S')
    timeparameter28 = f'range_pattern=ud&rangefrom_string={fromtime28}&rangeto_string={current_date}'
    timeparameter364 = f'range_pattern=ud&rangefrom_string={fromtime364}&rangeto_string={current_date}'

    # print(timeparameter)

    accounts = len(account_df.index)
    flag = [True,True,True]

    final_dfAccEvnts = pd.DataFrame()
    final_dfTrpSumm = pd.DataFrame()
    final_dfVclRpt = pd.DataFrame()

    for i in range(accounts):
        ACCOUNT_NAME = account_df["AccountName"][i].strip()
        ACCOUNT_ID = account_df["AccountId"][i]
        ACCOUNT_USERNAME = account_df["User"][i].strip()
        ACCOUNT_PASSWORD = account_df["Password"][i].strip()
        API_KEY = account_df["ApiKey"][i].strip()
        print(ACCOUNT_NAME, ACCOUNT_USERNAME, ACCOUNT_PASSWORD, API_KEY)

        # Objects
        r = requests.get(
            f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=de&action=showObjectReportExtern")
        data = io.StringIO(r.text)
        objects = pd.read_csv(data, sep=';')
        if objects.empty:
            print("test", r.text)
            continue

        else:
            objectNos = objects["objectno"].unique()
            print(len(objectNos))
            i = 0
            j=10
            for objectno in objectNos:
                if (i >= j or (i>=len(objectNos))):
                    time.sleep(64)
                    j=j+10
                    print(j)
                else:
                    print(i)
                    i = i + 1
                    r1 = requests.get(
                        f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=en&action=showAccelerationEvents&objectno={objectno}&{timeparameter28}")
                    data = io.StringIO(r1.text)
                    dfAccEvnts = pd.read_csv(data, sep=';')

                    r2 = requests.get(
                        f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=en&action=showTripSummaryReportExtern&objectno={objectno}&{timeparameter364}")
                    data = io.StringIO(r2.text)
                    dfTrpSumm = pd.read_csv(data, sep=';')

                    r3 = requests.get(
                        f"https://csv.webfleet.com/extern?account={ACCOUNT_NAME}&username={ACCOUNT_USERNAME}&password={ACCOUNT_PASSWORD}&apikey={API_KEY}&lang=en&action=showVehicleReportExtern&objectno={objectno}&{timeparameter28}")
                    data = io.StringIO(r3.text)
                    dfVclRpt = pd.read_csv(data, sep=';')

                    if dfAccEvnts.empty:
                        print("Acc Events", r1.text)
                    else:
                        dfAccEvnts["AccountId"] = ACCOUNT_ID

                        if (flag[0] == True):
                            final_dfAccEvnts = dfAccEvnts
                            flag[0] = False
                        else:
                            final_dfAccEvnts = pd.concat([final_dfAccEvnts, dfAccEvnts])


                    if dfTrpSumm.empty:
                        print("Trp Summ", r2.text)
                    else:
                        dfTrpSumm["AccountId"] = ACCOUNT_ID

                        if (flag[1] == True):
                            final_dfTrpSumm = dfTrpSumm
                            flag[1] = False
                        else:
                            final_dfTrpSumm = pd.concat([final_dfTrpSumm, dfTrpSumm])


                    if dfVclRpt.empty:
                        print("Vcl Report", r3.text)
                    else:
                        dfVclRpt["AccountId"] = ACCOUNT_ID

                        if (flag[2] == True):
                            final_dfVclRpt = dfVclRpt
                            flag[2] = False
                        else:
                            final_dfVclRpt = pd.concat([final_dfVclRpt, dfVclRpt])


    TripSummarypath = os.path.join(outdir,"TripSummaryReportExtern.csv")
    final_dfTrpSumm.to_csv(TripSummarypath)

    showAccelerationEventspath = os.path.join(outdir,"AccelerationEvents.csv")
    final_dfAccEvnts.to_csv(showAccelerationEventspath)

    showVehicleReportExternpath = os.path.join(outdir,"showVehicleReportExtern.csv")
    final_dfVclRpt.to_csv(showVehicleReportExternpath)

    # return final_df


xls_book = pd.ExcelFile(VERSIONFILEPATH,engine='openpyxl')
telematic_device = xls_book.parse("Telematic device")

navigation_device = xls_book.parse("Navigation device")
navigation_device["Software version"]= navigation_device["Software version"].apply(str)

telematic_device["Telematic device and software version"] = telematic_device['Telematic device'] + " | " + telematic_device["Software version"]
navigation_device["Navigation device and software version and map data version"] = navigation_device['Navigation device'] + " | " + navigation_device["Software version"] + " | " + navigation_device["Map data version"]

telematic_device = telematic_device.replace(" | "," ")
navigation_device = navigation_device.replace(" | | "," ")

telematic_device.reset_index(drop=True, inplace=True)
navigation_device.reset_index(drop=True, inplace=True)

account_df = pd.read_csv(ACCOUNTSFILEPATH)

# contracts = getContracts(account_df, navigation_device, telematic_device)
# contractspath = os.path.join(outdir,"Contracts.csv")
# contracts.to_csv(contractspath)
#
# orderReportExtern = getOrderReportExtern(account_df)
# orderReportExternpath = os.path.join(outdir,"OrderReportExtern.csv")
# orderReportExtern.to_csv(orderReportExternpath)
#
# # TripSummary = getTripSummaryReportExtern(account_df)
# # TripSummarypath = os.path.join(outdir,"TripSummaryReportExtern.csv")
# # TripSummary.to_csv(TripSummarypath)
#
# TripReportExtern = getTripReportExtern(account_df)
# TripReportExternpath = os.path.join(outdir,"TripReportExtern.csv")
# TripReportExtern.to_csv(TripReportExternpath)
#
# showMaintenanceTasks = getshowMaintenanceTasks(account_df)
# showMaintenanceTaskspath = os.path.join(outdir,"MaintenanceTasks.csv")
# showMaintenanceTasks.to_csv(showMaintenanceTaskspath)
#
# showAddressGroupReport = getshowAddressGroupReport(account_df)
# showAddressGroupReportpath = os.path.join(outdir,"AddressGroupReportExtern.csv")
# showAddressGroupReport.to_csv(showAddressGroupReportpath)
#
# showAddressReport = getshowAddressReport(account_df)
# showAddressReportpath = os.path.join(outdir,"AddressReportExtern.csv")
# showAddressReport.to_csv(showAddressReportpath)
#
# showAddressGroupAddressReport = getshowAddressGroupAddressReport(account_df)
# showAddressGroupAddressReportpath = os.path.join(outdir,"AddressGroupAddressReportExtern.csv")
# showAddressGroupAddressReport.to_csv(showAddressGroupAddressReportpath)
#
# # showAccelerationEvents = getshowAccelerationEvents(account_df)
# # showAccelerationEventspath = os.path.join(outdir,"AccelerationEvents.csv")
# # showAccelerationEvents.to_csv(showAccelerationEventspath)
#
# showAccountOrderAutomations = getshowAccountOrderAutomations(account_df)
# showAccountOrderAutomationspath = os.path.join(outdir,"AccountOrderAutomations.csv")
# showAccountOrderAutomations.to_csv(showAccountOrderAutomationspath)
#
# showAccountOrderStates = getshowAccountOrderStates(account_df)
# showAccountOrderStatespath = os.path.join(outdir,"AccountOrderStates.csv")
# showAccountOrderStates.to_csv(showAccountOrderStatespath)
#
# # consult with client (Not working)
# # showOrderWaypoints = getshowOrderWaypoints(account_df)
# # showOrderWaypointspath = os.path.join(outdir,"showOrderWaypoints.csv")
# # showOrderWaypoints.to_csv(showOrderWaypointspath)
#
# ShowMessages = getShowMessages(account_df)
# ShowMessagespath = os.path.join(outdir,"ShowMessages.csv")
# ShowMessages.to_csv(ShowMessagespath)
#
# showDriverReportExtern = getshowDriverReportExtern(account_df)
# showDriverReportExternpath = os.path.join(outdir,"showDriverReportExtern.csv")
# showDriverReportExtern.to_csv(showDriverReportExternpath)
#
# # showVehicleReportExtern = getshowVehicleReportExtern(account_df)
# # showVehicleReportExternpath = os.path.join(outdir,"showVehicleReportExtern.csv")
# # showVehicleReportExtern.to_csv(showVehicleReportExternpath)
#
# showOptiDriveIndicator = getshowOptiDriveIndicator(account_df)
# showOptiDriveIndicatorpath = os.path.join(outdir,"showOptiDriveIndicator.csv")
# showOptiDriveIndicator.to_csv(showOptiDriveIndicatorpath)
#
# showDriverGroups = getshowDriverGroups(account_df)
# showDriverGroupspath = os.path.join(outdir,"showDriverGroups.csv")
# showDriverGroups.to_csv(showDriverGroupspath)
#
showDriverGroupDrivers = getshowDriverGroupDrivers(account_df)
showDriverGroupDriverspath = os.path.join(outdir,"showDriverGroupDrivers.csv")
showDriverGroupDrivers.to_csv(showDriverGroupDriverspath)
# #
#
# objectsmethods(account_df)
#
# uploadToSharepoint()



# https://csv.webfleet.com/extern?lang=en&account=kuehne&username=arc2&password=yzjz1p5ryp7WNHF&apikey=6932c3b5-627b-4e6b-9dde-7be0e197b40c&action=showOrderWaypoints&orderid=LDC213491695
# https://csv.webfleet.com/extern?account=transport-222&username=arc2&password=yzjz1p5ryp7WNHF&apikey=6932c3b5-627b-4e6b-9dde-7be0e197b40c&lang=de&action=showTripReportExtern&range_pattern=m0