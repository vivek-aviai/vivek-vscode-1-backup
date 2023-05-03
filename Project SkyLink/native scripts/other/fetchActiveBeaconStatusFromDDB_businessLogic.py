#!/usr/bin/env python3
# coding: utf-8

# In[69]:

import re
import boto3
from datetime import datetime, timedelta, timezone

# BUSINESS LOGI - 1A
# Lambda4 that monitors the active_beacon_status and triggers notification if the logic is satisfied.
# This business logic triggers a warning if the bag/beacon is found outside the departing gate's periphery within 30mins of departure
# Ultimately, this creates another app with an REST endpoint + uuid for each case. That endpoint opens a web app with a functionality to
# locate the beacon inside the airport.

#  This is time in minutes before gate departure that the applications triggers a warning
triggerTimeDelta = 30  # in minutes
airportFocus = 'IAH'


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


# %%
# This "main" function consolidates different data points related to the beacons of interest,
# and triggers notification for potential mishandlings by updating a DDB

def fetchActiveBeaconStatusFromDDB():

    # Set the name of the DynamoDB table
    tableName = 'active_beacon_status'

    # Fetch all the elements in the table
    # TODO: Can be optimized to run the query directly on DDB
    table = boto3.resource('dynamodb').Table(tableName)
    response = table.scan()
    for item in response['Items']:
        if item['departureAirportFsCode'] == airportFocus and item['status'] == 'tracking':
            terminalNumber = item['departureTerminal']
            gateNumber = item['departureGate']
            lat, lng = fetchGateCoordinates(terminalNumber, gateNumber)

            # Prepare the object to be uploaded
            tmp_ts = datetime.now(timezone.utc)
            ts = tmp_ts.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

            # Compute the actual trigger time, if the triggering mechanism needs to kick in 30mins before scheduled departure
            format_str = '%Y-%m-%dT%H:%M:%S.%fZ'
            tmp_time = datetime.strptime(
                item['legSeq2DepFlight_estimatedDeparture'], format_str)
            actualTriggerTime_str = tmp_time - \
                timedelta(minutes=triggerTimeDelta)
            actualTriggerTime = actualTriggerTime_str.replace(
                tzinfo=timezone.utc) + 'Z'
            print(actualTriggerTime)

            data = {
                'ts': {'S': str(ts)},
                'beaconID': {'S': item['beaconID']},
                'baggageID': {'S': item['baggageID']},
                'legSeq1ArrFlightCode': {'S': item['legSeq1ArrFlightCode']},
                'departureGate': {'S': item['departureGate']},
                'arrivalAirportFsCode': {'S': item['arrivalAirportFsCode']},
                'arrivalTerminal': {'S': item['arrivalTerminal']},
                'departureAirportFsCode': {'S': item['departureAirportFsCode']},
                'legSeq2DepFlight_estimatedDeparture': {'S': item['legSeq2DepFlight_estimatedDeparture']},
                'carrierFsCode': {'S': item['carrierFsCode']},
                'departureTerminal': {'S': item['departureTerminal']},
                'legSeq1ArrFlight_estimatedArrival': {'S': item['legSeq1ArrFlight_estimatedArrival']},
                'arrivalGate': {'S': item['arrivalGate']},
                'legSeq2DepFlightCode': {'S': item['legSeq2DepFlightCode']},
                'status': {'S': 'tracking'},
                'beaconLatitude': {'S': item['beaconLatitude']},
                'beaconLongitude': {'S': item['beaconLongitude']},

                'gateLatitude': {'S': lat},
                'gateLongitude': {'S': lng}

            }
            # print(data)


fetchActiveBeaconStatusFromDDB()
