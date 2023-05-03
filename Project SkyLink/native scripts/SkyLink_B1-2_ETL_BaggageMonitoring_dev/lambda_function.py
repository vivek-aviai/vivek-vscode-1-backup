#!/usr/bin/env python3
# coding: utf-8

# ! Lambda Name: SkyLink_B1-2_ETL_BaggageMonitoring_dev

# In[69]:

import re
import boto3
import json
import requests
import inspect
from datetime import datetime, timezone, timedelta

# In[47]:


#? This lambda extracts, transforms and loads the data required for real time baggage monitoring.
#? We consolidate data from DDB tables, query different APIs to get flight status, and load
#? the aggregrated/transformed data onto another DDB table {realTime_baggage_monitoring} that is 
#? being constantly monitored by another lambda.

# Cirium app credentials
appId = 'c10fb336'
appKey = '4715b048952d5135286e7b243ae5ff96'
baseURL = 'https://api.flightstats.com/flex'


def fetchDataFromDynamoDB(tableName=None, GSIndex=None, attributeName=None, attributeValue=None, ScanIndexForward=False, scan=False):

    # Use the scan method to retrieve all items from the table
    # Since the table has the TTL value stale records are purged automatically by DDB, so we dont handle the time range management here
    if scan:

        # Get a reference to the DynamoDB table
        table = boto3.resource('dynamodb').Table(tableName)
        response = table.scan()
        return response['Items']

    else:

        # Create a specific query to retrieve data from the table
        dynamodb = boto3.client('dynamodb', region_name='us-east-2')
        query = {
            'TableName': tableName,
            # 'IndexName': 'beacon_id-index',
            'IndexName': GSIndex,  # name of the secondary index to use
            'KeyConditionExpression': '#attr = :val',
            'ExpressionAttributeNames': {'#attr': attributeName},
            'ExpressionAttributeValues': {':val': {'S': attributeValue}},

            # Sort the results in descending order (latest first)
            'ScanIndexForward': ScanIndexForward
        }

        # Search for the item using the query
        try:
            response = dynamodb.query(**query)
            # [0] indicates we want the latest record, typically applicable for time series records
            return response['Items'][0]
        except Exception as e:
            print(f"{attributeValue}: Error: {e} (Note: if the error is 'list index out of range, ignore that. \
                  It just means that the beaconID's GPS data isn't avialable currently, possibly because the aircraft just landed, \
                  and the beacon has not have been detected by an anchor just yet.) Log Function name: {inspect.currentframe().f_code.co_name}")
            return None


def putItemDDB(DynamoDBTableName, data):

    # Create a DDB client
    dynamodb = boto3.client('dynamodb', region_name='us-east-2')
    table_name = DynamoDBTableName

    # Upload the record to the DDB table
    try:
        response = dynamodb.put_item(
            TableName=table_name,
            Item=data
        )
    except Exception as error:
        print(f'Error: {error}')

    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        print(f'{response}')


def arrivalFlightDetails(carrierFsCode, flightNumber, year, month, day):

    # We fetch flight arrivals details from Cirium API
    url = f"{baseURL}/flightstatus/rest/v2/json/flight/status/{carrierFsCode}/{flightNumber}/arr/{year}/{month}/{day}?appId={appId}&appKey={appKey}"

    # Make the GET request and store the response
    try:
        response = requests.get(url)
        data = json.loads(response.content)
        if data['flightStatuses']:
            flightArrival = {}
            try:
                flightArrival['arrivalEstimatedGateArrival'] = {
                    'S': data['flightStatuses'][0]['operationalTimes']['estimatedGateArrival']['dateUtc']}
            except Exception as e:
                flightArrival['arrivalEstimatedGateArrival'] = {'S': 'unknown'}
            try:
                flightArrival['arrivalActualEquipmentIataCode'] = {
                    'S': data['flightStatuses'][0]['flightEquipment']['actualEquipmentIataCode']}
            except Exception as e:
                flightArrival['arrivalActualEquipmentIataCode'] = {
                    'S': 'unknown'}
            try:
                flightArrival['arrivalFlightTailNumber'] = {
                    'S': data['flightStatuses'][0]['flightEquipment']['tailNumber']}
            except Exception as e:
                flightArrival['arrivalFlightTailNumber'] = {'S': 'unknown'}
            try:
                flightArrival['arrivalTerminal'] = {
                    'S': data['flightStatuses'][0]['airportResources']['arrivalTerminal']}
            except Exception as e:
                flightArrival['arrivalTerminal'] = {'S': 'unknown'}
            try:
                flightArrival['arrivalGate'] = {
                    'S': data['flightStatuses'][0]['airportResources']['arrivalGate']}
            except Exception as e:
                flightArrival['arrivalGate'] = {'S': 'unknown'}
            try:
                flightArrival['arrivalAirportFsCode'] = {
                    'S': data['flightStatuses'][0]['arrivalAirportFsCode']}
            except Exception as e:
                flightArrival['arrivalAirportFsCode'] = {'S': 'unknown'}
            try:
                flightArrival['arrivalActualEquipmentName'] = {
                    'S': data['appendix']['equipments'][0]['name']}
            except Exception as e:
                flightArrival['arrivalActualEquipmentName'] = {'S': 'unknown'}
            try:
                flightArrival['arrivalActualEquipmentWidebody'] = {
                    'S': data['appendix']['equipments'][0]['widebody']}
            except Exception as e:
                flightArrival['arrivalActualEquipmentWidebody'] = {
                    'S': 'unknown'}
            try:
                flightArrival['arrivalFlightId'] = {
                    'S': data['flightStatuses'][0]['flightId']}
            except Exception as e:
                flightArrival['arrivalFlightId'] = {'S': 'unknown'}

            return flightArrival
        else:
            print(
                f"Cirium API's response for the '{carrierFsCode}{flightNumber}' on {day}-{month}-{year} is erroneous. Check the 'Cirium's Flight by airport arrivals' for more details. Log Function name: {inspect.currentframe().f_code.co_name}")
            return None
    except Exception as error:
        print(error)


def departureFlightDetails(carrierFsCode, flightNumber, year, month, day):

    # We fetch flight arrivals details from Cirium API
    url = f"{baseURL}/flightstatus/rest/v2/json/flight/status/{carrierFsCode}/{flightNumber}/dep/{year}/{month}/{day}?appId={appId}&appKey={appKey}"

    # Make the GET request and store the response
    try:
        response = requests.get(url)
        data = json.loads(response.content)
        if data['flightStatuses']:
            flightDeparture = {}
            try:
                flightDeparture['departureEstimatedGateDeparture'] = {
                    'S': data['flightStatuses'][0]['operationalTimes']['estimatedGateDeparture']['dateUtc']}
            except Exception as e:
                flightDeparture['departureEstimatedGateDeparture'] = {
                    'S': 'unknown'}
            try:
                flightDeparture['departureActualEquipmentIataCode'] = {
                    'S': data['flightStatuses'][0]['flightEquipment']['actualEquipmentIataCode']}
            except Exception as e:
                flightDeparture['departureActualEquipmentIataCode'] = {
                    'S': 'unknown'}
            try:
                flightDeparture['departureFlightTailNumber'] = {
                    'S': data['flightStatuses'][0]['flightEquipment']['tailNumber']}
            except Exception as e:
                flightDeparture['departureFlightTailNumber'] = {'S': 'unknown'}
            try:
                flightDeparture['departureTerminal'] = {
                    'S': data['flightStatuses'][0]['airportResources']['departureTerminal']}
            except Exception as e:
                flightDeparture['departureTerminal'] = {'S': 'unknown'}
            try:
                flightDeparture['departureGate'] = {
                    'S': data['flightStatuses'][0]['airportResources']['departureGate']}
            except Exception as e:
                flightDeparture['departureGate'] = {'S': 'unknown'}
            try:
                flightDeparture['departureAirportFsCode'] = {
                    'S': data['flightStatuses'][0]['departureAirportFsCode']}
            except Exception as e:
                flightDeparture['departureAirportFsCode'] = {'S': 'unknown'}
            try:
                flightDeparture['departureActualEquipmentName'] = {
                    'S': data['appendix']['equipments'][0]['name']}
            except Exception as e:
                flightDeparture['departureActualEquipmentName'] = {
                    'S': 'unknown'}
            try:
                flightDeparture['departureActualEquipmentWidebody'] = {
                    'S': data['appendix']['equipments'][0]['widebody']}
            except Exception as e:
                flightDeparture['departureActualEquipmentWidebody'] = {
                    'S': 'unknown'}
            try:
                flightDeparture['departureFlightId'] = {
                    'S': data['flightStatuses'][0]['flightId']}
            except Exception as e:
                flightDeparture['departureFlightId'] = {'S': 'unknown'}

            return flightDeparture
        else:
            print(
                f"Cirium API's response for the '{carrierFsCode}{flightNumber}' on {day}-{month}-{year} is erroneous. Check the 'Cirium's Flight by airport arrivals' for more details. Log Function name: {inspect.currentframe().f_code.co_name}")
            return None
    except Exception as error:
        print(error)


def getBoundingRadius(search_key):
    # Initialize the DynamoDB client
    dynamodb = boto3.client('dynamodb')

    # Define the table name and the search key
    table_name = 'boundingRadius_equipment'

    # Define the query parameters
    query_params = {
        'TableName': table_name,
        'KeyConditionExpression': 'equipment = :equipment',
        'ExpressionAttributeValues': {
            ':equipment': {'S': search_key}
        }
    }

    # Execute the query and get the results
    response = dynamodb.query(**query_params)

    # Print the rows that match the search key
    return response['Items'][0]['boundingRadius']


def regexFormat(my_string):
    # Extract the alphabetical and numerical parts using regular expressions
    # alphabet = re.findall('[a-zA-Z]+', my_string)[0]
    numeric = re.findall('\d+', my_string)[0]

    return numeric


# TODO: We fetch all the gate details and filter locally. Can be optimized to run the complex query directly on DDB
def fetchGateCoordinates(terminalNumber, gateNumber):
    # Set the name of the DynamoDB table
    tableName = 'IAH_gate_coordinates'

    # Fetch all the elements in the table.
    table = boto3.resource('dynamodb').Table(tableName)
    response = table.scan()

    for item in response['Items']:

        # From Cirium's API, we notice that even though airport might designate a gate as '20a', the API response contains only '20'.
        # Conceptually, the coordinates are the same, so we clean the string to reflect only the numeric part of the gateNumber
        gateNumber_clean = regexFormat(item['gateNumber'])

        if terminalNumber.lower() == item['terminal'].lower() and gateNumber == gateNumber_clean:
            return item['latitude'], item['longitude']


def formatDate(dateString):

    # Convert the date string to a datetime object
    date_obj = datetime.strptime(dateString, "%Y-%m-%d")
    data = {
        'year': str(date_obj.year),
        'month': str(date_obj.month).zfill(2),
        'day': str(date_obj.day).zfill(2)
    }
    return data



def main():

    # DynamoDB table name and attributes
    tableName = 'beacon_on_which_flight'

    # Get the resource data from the table
    transferBaggage = fetchDataFromDynamoDB(tableName=tableName, scan=True)

    for item in transferBaggage:
        tableName = 'uwb-data-from-kinesis'
        GSIndex = 'beacon_id-index'
        attributeName = 'beacon_id'
        attributeValue = item['beaconID']

        # Get the resource from DDB
        beaconPosition = fetchDataFromDynamoDB(tableName=tableName, GSIndex=GSIndex, attributeName=attributeName,
                                               attributeValue=attributeValue, ScanIndexForward=False, scan=False)

        tmp_ts = datetime.now(timezone.utc)
        ts = tmp_ts.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        # Get the arrival/departure information
        carrierFsCode = item['carrierFsCode']
        arrivingFlight = item['legSeq1ArrFlightCode']
        departingFlight = item['legSeq2DepFlightCode']
        arrivalDate = formatDate(
            item['legSeq1ArrFlightCode_scheduledBoardDate'])
        departureDate = formatDate(
            item['legSeq2DepFlightCode_scheduledBoardDate'])
        flightArrivals = arrivalFlightDetails(
            carrierFsCode, arrivingFlight, arrivalDate['year'], arrivalDate['month'], arrivalDate['day'])
        flightDepartures = departureFlightDetails(
            carrierFsCode, arrivingFlight, departureDate['year'], departureDate['month'], departureDate['day'])
        if flightDepartures is None:
            continue

        # Bounding radius information from a DDB table
        # Can also be fetched directly from "https://api.flightstats.com/flex/equipment/rest/v1/json/all"
        try:
            boundingRadius = getBoundingRadius(
                flightDepartures['departureActualEquipmentIataCode']['S'])
        except Exception as e:
            boundingRadius = 'unknown'

        # Compute gate location (outbound flights from IAH only)
        try:
            if flightDepartures['departureAirportFsCode']['S'] == 'IAH':
                terminalNumber = flightDepartures['departureTerminal']['S']
                gateNumber = flightDepartures['departureGate']['S']
                gateLat, gateLng = fetchGateCoordinates(
                    terminalNumber, gateNumber)
            else:
                gateLat = 'unknown'
                gateLng = 'unknown'
        except Exception as e:
            print(e)

        print(beaconPosition['longitude'])

        if beaconPosition:  # Check if beacon real time positioning is available

            data = {
                # Baggage and flight sequences from 'item'
                'ts': {'S': str(ts)},
                'beaconID': {'S': item['beaconID']},
                'baggageID': {'S': item['baggageID']},
                'legSeq1ArrFlightCode': {'S': item['legSeq1ArrFlightCode']},
                'legSeq2DepFlightCode': {'S': item['legSeq2DepFlightCode']},

                # beacon location from 'uwb-data-from-kinesis'
                'status': {'S': 'tracking'},
                'beaconLatitude': beaconPosition['latitude'],
                'beaconLongitude': beaconPosition['longitude'],

                # from flightArrivals
                'arrivalEstimatedGateArrival': flightArrivals['arrivalEstimatedGateArrival'],
                'arrivalActualEquipmentIataCode': flightArrivals['arrivalActualEquipmentIataCode'],
                'arrivalFlightTailNumber': flightArrivals['arrivalFlightTailNumber'],
                'arrivalTerminal': flightArrivals['arrivalTerminal'],
                'arrivalGate': flightArrivals['arrivalGate'],
                'arrivalAirportFsCode': flightArrivals['arrivalAirportFsCode'],
                'arrivalActualEquipmentName': flightArrivals['arrivalActualEquipmentName'],
                'arrivalActualEquipmentWidebody': {'S': str(flightArrivals['arrivalActualEquipmentWidebody']['S'])},
                'arrivalFlightId': {'S': str(flightArrivals['arrivalFlightId']['S'])},

                # from flightDepartures
                'departureEstimatedGateDeparture': flightDepartures['departureEstimatedGateDeparture'],
                'departureActualEquipmentIataCode': flightDepartures['departureActualEquipmentIataCode'],
                'departureFlightTailNumber': flightDepartures['departureFlightTailNumber'],
                'departureTerminal': flightDepartures['departureTerminal'],
                'departureAirportFsCode': flightDepartures['departureAirportFsCode'],
                'departureActualEquipmentName': flightDepartures['departureActualEquipmentName'],
                'departureEstimatedGateDeparture': flightDepartures['departureEstimatedGateDeparture'],
                'departureActualEquipmentWidebody': {'S': str(flightDepartures['departureActualEquipmentWidebody']['S'])},
                'departureFlightId': {'S': str(flightDepartures['departureFlightId']['S'])},

                # gate location and bounding radius
                'boundingRadius': boundingRadius,
                'gateLatitude': {'S': gateLat},
                'gateLongitude': {'S': gateLng}
            }
        else:
            data = {
                # Baggage and flight sequences from 'item'
                'ts': {'S': str(ts)},
                'beaconID': {'S': item['beaconID']},
                'baggageID': {'S': item['baggageID']},
                'legSeq1ArrFlightCode': {'S': item['legSeq1ArrFlightCode']},
                'legSeq2DepFlightCode': {'S': item['legSeq2DepFlightCode']},

                # beacon location from 'uwb-data-from-kinesis'
                'status': {'S': 'waiting'},
                'beaconLatitude': {'S': 'unknown'},
                'beaconLongitude': {'S': 'unknown'},

                # from flightArrivals
                'arrivalEstimatedGateArrival': flightArrivals['arrivalEstimatedGateArrival'],
                'arrivalActualEquipmentIataCode': flightArrivals['arrivalActualEquipmentIataCode'],
                'arrivalFlightTailNumber': flightArrivals['arrivalFlightTailNumber'],
                'arrivalTerminal': flightArrivals['arrivalTerminal'],
                'arrivalGate': flightArrivals['arrivalGate'],
                'arrivalAirportFsCode': flightArrivals['arrivalAirportFsCode'],
                'arrivalActualEquipmentName': flightArrivals['arrivalActualEquipmentName'],
                'arrivalActualEquipmentWidebody': {'S': str(flightArrivals['arrivalActualEquipmentWidebody']['S'])},
                'arrivalFlightId': {'S': str(flightArrivals['arrivalFlightId']['S'])},

                # from flightDepartures
                'departureEstimatedGateDeparture': flightDepartures['departureEstimatedGateDeparture'],
                'departureActualEquipmentIataCode': flightDepartures['departureActualEquipmentIataCode'],
                'departureFlightTailNumber': flightDepartures['departureFlightTailNumber'],
                'departureTerminal': flightDepartures['departureTerminal'],
                'departureAirportFsCode': flightDepartures['departureAirportFsCode'],
                'departureActualEquipmentName': flightDepartures['departureActualEquipmentName'],
                'departureEstimatedGateDeparture': flightDepartures['departureEstimatedGateDeparture'],
                'departureActualEquipmentWidebody': {'S': str(flightDepartures['departureActualEquipmentWidebody']['S'])},
                'departureFlightId': {'S': str(flightDepartures['departureFlightId']['S'])},

                # gate location and bounding radius
                'boundingRadius': boundingRadius,
                'gateLatitude': {'S': str(gateLat)},
                'gateLongitude': {'S': str(gateLng)}
            }

        print(data)

        # Upload the record
        putItemDDB('realTime_baggage_monitoring', data)



def lambda_handler(event, context):
    main()

