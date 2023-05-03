
#!/usr/bin/env python3
# coding: utf-8

# In[69]:
import boto3
import pytz
import time
import json
import inspect
import requests
from datetime import datetime, timedelta


# In[11]:


# Lambda1 that calls the Cirium API, retreives all the flight arriving in the next n hours and populates a DDB

# Cirium app credentials
appId = 'c10fb336'
appKey = '4715b048952d5135286e7b243ae5ff96'
baseURL = 'https://api.flightstats.com/flex'


def putItemDynamoDB(flightData, DynamoDBTableName):

    # Create the DynamoDB instance and upload the data
    dynamodb = boto3.client('dynamodb', region_name='us-east-2')
    table_name = DynamoDBTableName
    response = dynamodb.put_item(
        TableName=table_name,
        Item=flightData
    )
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        print(f'{response}. Function name: {inspect.currentframe().f_code.co_name}()')


def getLocalTime(airportFsCode):

    url = f"{baseURL}/airports/rest/v1/json/iata/{airportFsCode}?appId={appId}&appKey={appKey}"
    response = requests.get(url)
    offset_hours = json.loads(response.content)['airports'][0]['utcOffsetHours']
    localTime = datetime.now(tz=pytz.utc) + timedelta(hours=offset_hours)
    return localTime


def checkArrivingFlights(airportFsCode):
    # Find the timezone of the airport. Cirium API works on local timezone
    localTime = getLocalTime(airportFsCode)
    localTime += timedelta(hours=1) # Get next hours flights

    # Current date.
    year = str(localTime.year)
    month = str(localTime.month)
    day = str(localTime.day)
    hour = str(localTime.hour)

    # Cirium FlightStats API to retrieve arriving flights at IAH in the next 1 hour
    url = f"{baseURL}/flightstatus/rest/v2/json/airport/status/{airportFsCode}/arr/{year}/{month}/{day}/{hour}?appId={appId}&appKey={appKey}"

    # Make the GET request and store the response
    try:
        response = requests.get(url)
        data = json.loads(response.content)
        if data['flightStatuses']:
            for i in range(len(data['flightStatuses'])):

                # Check if it is a United flight
                if data['flightStatuses'][i]['carrierFsCode'] == "UA":
                    flightData = {
                        'utcEstimatedGateArrival': {'S': data['flightStatuses'][i]['operationalTimes']['estimatedGateArrival']['dateUtc']},
                        'localEstimatedGateArrival': {'S': data['flightStatuses'][i]['operationalTimes']['estimatedGateArrival']['dateLocal']},
                        'airportFsCode': {'S': airportFsCode},
                        'carrierFsCode': {'S': data['flightStatuses'][i]['carrierFsCode']},
                        'flightNumber': {'S': data['flightStatuses'][i]['flightNumber']}
                    }
                    DynamoDBTableName = 'imminent_flight_arrivals'
                    putItemDynamoDB(flightData, DynamoDBTableName)

        else:
            print(
                f"Cirium API's response for the '{airportFsCode}' is erroneous. Check the 'Cirium's Flight by airport arrivals' for more details. Log Function name: {inspect.currentframe().f_code.co_name}")

    except Exception as error:
        print(f"Error in fetching flight details from Cirium API. {error}. Function name: {inspect.currentframe().f_code.co_name}()")
        
checkArrivingFlights("IAH")