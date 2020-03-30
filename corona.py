import pandas as pd
import numpy as np
from client import *


big_data = pd.read_csv("data.csv", index_col='Date', parse_dates=True)

'''
tagname = CountryName.CountryCode.RegionCode.RegionName.Confirmed

'''
print(big_data.columns)

myDynamicType = [{
                    "id": "CovidData",
                    "version": "1.0.0.0",
                    "type": "object",
                    "classification": "dynamic",
                    "properties": {
                        "Time": {
                            "type": "string",
                            "format": "date-time",
                            "isindex": True
                            },
                        "Confirmed": {
                            "type": "integer",
                            "format": "int32"
                        },
                        "Deaths": {
                            "type": "integer",
                            "format": "int32"
                        },
                        "Latitude": {
                            "type": "number",
                            "format": "float32"
                        },
                        "Longitude": {
                            "type": "number",
                            "format": "float32"
                        },
                        "Population": {
                            "type": "integer",
                            "format": "int32"
                        },
                    }
                }]

send_omf_message_to_endpoint("type", myDynamicType, "create")

#CountryName.CountryCode.RegionCode.RegionName.Confirmed


sub_df = big_data.copy()[['CountryName', 'CountryCode', 'RegionName', 'RegionCode']]
sub_df.fillna('', inplace=True)

concatenated_df = sub_df['CountryName'] + sub_df['CountryCode'] + sub_df['RegionName'] \
                    + sub_df['RegionCode']

spec_chars = ["!",'"',"#","%","&","'","(",")",
              "*","+",",","-",".","/",":",";","<",
              "=",">","?","@","[","\\","]","^","_",
              "`","{","|","}","~","–"]

for char in spec_chars:
    concatenated_df = concatenated_df.str.replace(char, ' ')

uniqueArray = np.unique(concatenated_df)
concatenated_df = concatenated_df.to_frame(name='container')
dynamicTypeContatinerArray = []

for i in range(0, uniqueArray.shape[0]):
    dynamicTypeContatine = {
                    "id": uniqueArray[i],
                    "typeid": "CovidData",
                    "typeversion": "1.0.0.0",
                    "name": uniqueArray[i]
                    }
    dynamicTypeContatinerArray.append(dynamicTypeContatine)


send_omf_message_to_endpoint("container", dynamicTypeContatinerArray, "create")

final_df = pd.concat([big_data, concatenated_df], axis=1)
final_df.fillna('0', inplace=True)
myDataArray = []

for index, row in final_df.iterrows():

    myData = {
                "containerid": row['container'],
                "values": [{
                    "Time": str(index.isoformat()),
                    "Confirmed": int(row['Confirmed']),
                    "Deaths": int(row['Deaths']),
                    "Latitude": row['Latitude'],
                    "Longitude": row['Longitude'],
                    "Population": int(row['Population']),
                }]
            }
    myDataArray.append(myData)


send_omf_message_to_endpoint("data", myDataArray, "create")

countryTemplate = [{
    "id": "Country",
    "classification": "static",
    "type": "object",
    "name": "Country",
    "properties": {
        "CountryId": {
            "type": "string",
            "name": "CountryId",
            "isIndex": True
        },
        "CountryName": {
            "type": "string",
            "name": "CountryName",
            "isIndex": False
        }
    }
}]

regionTemplate = [{
    "id": "Region",
    "classification": "static",
    "type": "object",
    "name": "Region",
    "properties": {
        "RegionId": {
            "type": "string",
            "name": "RegionId",
            "isIndex": True
        },
        "RegionName": {
            "type": "string",
            "name": "RegionName",
            "isIndex": False
        },
        "BaseTagName": {
            "type": "string",
            "name": "BaseTagName",
            "isIndex": False
        }

    }
}]

# send_omf_message_to_endpoint("type", countryTemplate, "create")
send_omf_message_to_endpoint("type", regionTemplate, "create")
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

print(final_df.tail())
final_df.replace(to_replace=0, value='')
countryNames = final_df['CountryName']
uniqueCountries = np.unique(countryNames)
print(uniqueCountries)


regionNames = final_df['RegionName']
uniqueRegions = np.unique(regionNames)
print(uniqueRegions)



countries = []
for index, row in final_df.iterrows():


    country = [{
        "typeId": "Region",
        "values": [
            {
                "RegionId": str(row['CountryName']).lstrip().rstrip() + '.' + str(row['RegionName']).lstrip().rstrip(),
                "RegionName": str(row['CountryName']).lstrip().rstrip() + '.' + str(row['RegionName']).lstrip().rstrip(),
                "BaseTagName": row['container']
            }
        ]
    }]
    send_omf_message_to_endpoint("data", country, "create")




