#!/usr/bin/env python3
# coding: utf-8

# In[69]:

import re
import boto3
import pytz
import json
import requests
import inspect
from datetime import datetime, timedelta



# This lambda maps the transfer-ready beacons to their arriving and departing flights, 
# along with their estimated arrival/departure resp.
# Cirium app credentials
appId = 'c10fb336'
appKey = '4715b048952d5135286e7b243ae5ff96'
baseURL = 'https://api.flightstats.com/flex'

def regexFormat(my_string):
    # Extract the alphabetical and numerical parts using regular expressions
    alphabet = re.findall('[a-zA-Z]+', my_string)[0]
    numeric = re.findall('\d+', my_string)[0]

    return alphabet, numeric

def getLocalTime(airportFsCode):

    url = f"{baseURL}/airports/rest/v1/json/iata/{airportFsCode}?appId={appId}&appKey={appKey}"
    response = requests.get(url)
    offset_hours = json.loads(response.content)['airports'][0]['utcOffsetHours']
    localTime = datetime.now(tz=pytz.utc) + timedelta(hours=offset_hours)
    return localTime


def putItemDDB(DynamoDBTableName, data, region_name='us-east-2'):
    dynamodb = boto3.client('dynamodb', region_name=region_name)
    table_name = DynamoDBTableName
    response = dynamodb.put_item(
        TableName=table_name,
        Item=data
    )
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        print(f'{response}')


def getImminentFlightArrivalInfo(DynamoDBTableName):
    # Set up a DynamoDB client
    dynamodb = boto3.client('dynamodb', region_name='us-east-2')

    # Name of the DynamoDB table to query
    table_name = 'imminent_flight_arrivals'

    # Get a reference to the DynamoDB table
    table = boto3.resource('dynamodb').Table(table_name)

    listFlight = []
    # Use the scan method to retrieve all items from the table
    response = table.scan()
    for item in response['Items']:
        listFlight.append(item['flightNumber'])

    return listFlight[:50] # Change this


#  This function determines the arrival or departure times of a flight
def checkFlightInfo(airportFsCode, carrierFsCode, flightNumber, ArrOrDep):

    # Find the timezone of the airport. Cirium API works on local timezone
    localTime = getLocalTime(airportFsCode)
    
    # Current date.
    year = str(localTime.year)
    month = str(localTime.month)
    day = str(localTime.day)
    utc_timezone = pytz.timezone('UTC')

    # Cirium FlightStats API to retrieve arriving flight's info
    url = f"{baseURL}/flightstatus/rest/v2/json/flight/status/{carrierFsCode}/{flightNumber}/{ArrOrDep}/{year}/{month}/{day}?appId={appId}&appKey={appKey}"

    response = requests.get(url)
    data = json.loads(response.content)
    if ArrOrDep == 'arr':
        if data['flightStatuses']:
            try:
                arrivalAirportFsCode = data['flightStatuses'][0]['arrivalAirportFsCode']
            except Exception as e:
                arrivalAirportFsCode = 'unknown'
            try:
                legSeq1ArrFlight_estimatedArrival = data['flightStatuses'][0]['operationalTimes']['estimatedGateArrival']['dateUtc']
            except Exception as e:
                try:
                    legSeq1ArrFlight_estimatedArrival = data['flightStatuses'][0]['operationalTimes']['scheduledGateArrival']['dateUtc']
                except Exception as e:
                    legSeq1ArrFlight_estimatedArrival = 'unknown'
            try:
                arrivalGate = data['flightStatuses'][0]['airportResources']['arrivalGate']
            except Exception as e:
                arrivalGate = 'unknown'
            try:
                arrivalTerminal = data['flightStatuses'][0]['airportResources']['arrivalTerminal']
            except Exception as e:
                try:
                    # Sometimes the API returns no terminal but an erroneous gate number as 'C34'. In that case, C is the terminal, and 34 is the gate number. 
                    # We use regex to handle this exception
                    arrivalTerminal, arrivalGate = regexFormat(data['flightStatuses'][0]['airportResources']['arrivalGate'])
                    print('Regex Formatted!')
                except Exception as e:
                    arrivalTerminal = 'unknown'
                    print('Regex Failed!!')
            
        else:
            legSeq1ArrFlight_estimatedArrival = None
            arrivalTerminal = None
            arrivalGate = None
            arrivalAirportFsCode = None

        return (legSeq1ArrFlight_estimatedArrival, arrivalTerminal, arrivalGate, arrivalAirportFsCode)

    else:
        if data['flightStatuses']:
            print(data['request']['flight']['requested'])
            try:
                departureAirportFsCode = data['flightStatuses'][0]['departureAirportFsCode']
            except Exception as e:
                departureAirportFsCode = 'unknown'
            try:
                legSeq2DepFlight_estimatedDeparture = data['flightStatuses'][0]['operationalTimes']['estimatedGateDeparture']['dateUtc']
            except Exception as e:
                try:
                    legSeq2DepFlight_estimatedDeparture = data['flightStatuses'][0]['operationalTimes']['scheduledGateDeparture']['dateUtc']
                except Exception as e:
                    legSeq2DepFlight_estimatedDeparture = 'unknown'
            try:
                departureGate = data['flightStatuses'][0]['airportResources']['departureGate']
            except Exception as e:
                departureGate = 'unknown'
            try:
                departureTerminal = data['flightStatuses'][0]['airportResources']['departureTerminal']
            except Exception as e:
                try:
                    # Sometimes the API returns no terminal but gate number as 'C34'. In that case C is the terminal, and 34 is the gate number. 
                    # We use regex to handle this exception
                    departureTerminal, departureGate = regexFormat(data['flightStatuses'][0]['airportResources']['departureGate'])
                    print('Regex Formatted!')
                except Exception as e:
                    departureTerminal = 'unknown'
                    print('Regex Failed!!')
            
        else:
            legSeq2DepFlight_estimatedDeparture = None
            departureTerminal = None
            departureGate = None
            departureAirportFsCode = None

        return (legSeq2DepFlight_estimatedDeparture, departureTerminal, departureGate, departureAirportFsCode)



def getBeaconsOnFlightInfo(listFlight):
    # Set up a DynamoDB client
    dynamodb = boto3.client('dynamodb', region_name='us-east-2')

    # Set the table name
    table_name = 'beacon_on_which_flight'

    # Split the list of items into batches of 100. DynamoDB can only allows 100 expressions per query
    item_batches = [listFlight[i:i+100] for i in range(0, len(listFlight), 100)]  

    for item_batch in item_batches:

        # Set the query parameters
        query_params = {
            'TableName': table_name,
            'FilterExpression': 'legSeq1ArrFlightCode IN (' + ','.join([':f{}'.format(i) for i in range(len(item_batch))]) + ')',
            'ExpressionAttributeValues': {(':f{}'.format(i)): {'S': item_batch[i]} for i in range(len(item_batch))},
        }

        # Execute the query
        response = dynamodb.scan(**query_params)

        # Print the results
        items = response.get('Items', [])

        for item in items:
            carrierFsCode = item['carrierFsCode']['S']

            # Call Cirium API to fetch Seq1 arrival time (A0)
            legSeq1ArrFlight_estimatedArrival, arrivalTerminal, arrivalGate, arrivalAirportFsCode = \
            checkFlightInfo('IAH', carrierFsCode, item['legSeq1ArrFlightCode']['S'], 'dep')

            # # Call Cirium API to fetch Seq2 departure time (D0)
            legSeq2DepFlight_estimatedDeparture, departureTerminal, departureGate, departureAirportFsCode = \
            checkFlightInfo('IAH', carrierFsCode, item['legSeq2DepFlightCode']['S'], 'dep')

            # If all items returned above are None, then the record is erroneous, hence we skip that record
            # TODO: figure out a way to hadnle this excpetion
            if legSeq1ArrFlight_estimatedArrival is None and legSeq2DepFlight_estimatedDeparture is None:
                continue

            # Add arrival/departure times to the dict
            item['legSeq1ArrFlight_estimatedArrival'] = {'S': str(legSeq1ArrFlight_estimatedArrival)}
            item['legSeq2DepFlight_estimatedDeparture'] = {'S': str(legSeq2DepFlight_estimatedDeparture)}
            item['departureTerminal'] = {'S': str(departureTerminal)}
            item['departureGate'] = {'S': str(departureGate)}
            item['arrivalTerminal'] = {'S': str(arrivalTerminal)}
            item['arrivalGate'] = {'S': str(arrivalGate)}
            item['arrivalAirportFsCode'] = {'S': str(arrivalAirportFsCode)}
            item['departureAirportFsCode'] = {'S': str(departureAirportFsCode)}
            
            # Upload the records to DDB
            putItemDDB('transfer_baggage_info', item)

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            print(f'{response}')

# Main file
def mapArrivalFlightToBeacons():
    # get flights for the next hour
    listFlight = getImminentFlightArrivalInfo(DynamoDBTableName='imminent_flight_arrivals')

    # Find all the beacons on those specific flights
    getBeaconsOnFlightInfo(listFlight)

mapArrivalFlightToBeacons()
