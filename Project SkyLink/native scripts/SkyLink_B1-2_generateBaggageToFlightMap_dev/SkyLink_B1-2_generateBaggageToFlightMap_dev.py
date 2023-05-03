#!/usr/bin/env python3
# coding: utf-8


# %%

import boto3
import random
from datetime import date


# Create a DynamoDB client
dynamodb = boto3.client('dynamodb', region_name='us-east-2')

# Name of the DynamoDB table to query
table_name = 'imminent_flight_arrivals'

# Get a reference to the DynamoDB table
table = boto3.resource('dynamodb').Table(table_name)

# Use the scan method to retrieve all items from the table
response = table.scan()
listTmp = []
# Print the items returned by the scan operation
for item in response['Items']:
    listTmp.append(item['flightNumber'])

for i in range(1, 1000):
    my_dict = {}
    randomStr = str(i).zfill(4)
    beaconID = f"AviAI_v1_{randomStr}"
    legSeq1ArrFlightCode = random.choice(listTmp)
    carrierFsCode = "UA"
    baggageID = str(random.randint(1000000000, 9999999999))
    legSeq1ArrFlightCode_scheduledBoardDate = date.today()
    legSeq2DepFlightCode_scheduledBoardDate = date.today()
    legSeq2DepFlightCode = random.choice(listTmp)
    if legSeq2DepFlightCode == legSeq1ArrFlightCode:
        legSeq2DepFlightCode = random.choice(listTmp)

    my_dict = {
        'beaconID': {'S': beaconID},
        'baggageID': {'S': baggageID},
        'legSeq1ArrFlightCode': {'S': legSeq1ArrFlightCode},
        'carrierFsCode': {'S': carrierFsCode},
        'legSeq2DepFlightCode': {'S': legSeq2DepFlightCode},
        'legSeq1ArrFlightCode_scheduledBoardDate': {'S': str(legSeq1ArrFlightCode_scheduledBoardDate)},
        'legSeq2DepFlightCode_scheduledBoardDate': {'S': str(legSeq2DepFlightCode_scheduledBoardDate)}

    }

    dynamodb = boto3.client('dynamodb', region_name='us-east-2')
    table_name = 'beacon_on_which_flight'
    response = dynamodb.put_item(
        TableName=table_name,
        Item=my_dict
    )
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        print(f'{response}')

