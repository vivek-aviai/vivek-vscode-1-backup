#!/usr/bin/env python3
# coding: utf-8

# ! Lambda Name: SkyLink_B1-2_generateSyntheticBaggageCoordinates_dev

#%%

import boto3
import pytz
import random
import uuid
from datetime import datetime

# %%

def getBboxRandomGPS():
        
    # Define the bounding box vertices as latitude-longitude pairs
    # These are the boundaries for IAH
    A = (29.987245, -95.350591)
    B = (29.986137, -95.348969)
    C = (29.985750, -95.332333)
    D = (29.987958, -95.332366)

    # Define the latitude and longitude ranges for the bounding box
    min_lat = min(A[0], B[0], C[0], D[0])
    max_lat = max(A[0], B[0], C[0], D[0])
    min_lon = min(A[1], B[1], C[1], D[1])
    max_lon = max(A[1], B[1], C[1], D[1])

    # Generate random points within the bounding box
    lat = random.uniform(min_lat, max_lat)
    lng = random.uniform(min_lon, max_lon)

    return lat, lng
# %%
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

# %%

def main():

    # Populate synthetic beacon coordintes within IAH in the 'synthetic_baggage_coordinates_IAH' table
    # duration = 20*60
    # start = time.monotonic()

    # while time.monotonic() - start < duration:
    for i in range(1, 1000):
        ts = str(datetime.now(tz=pytz.utc))
        anchor_id = str(uuid.uuid4()).upper()
        angle = str(random.uniform(-180, 180))
        beaconID = str(f"AviAI_v1_{str(i).zfill(4)}")
        lat, lng = getBboxRandomGPS()
        rel_dist = str(random.uniform(0, 10))
        station = 'IAH'
        x_dist = str(random.uniform(-1, 1))
        y_dist = str(random.uniform(-1, 1))
        z_dist = str(random.uniform(-1, 1))

        # Create the record object for DDB table
        data = {
            'ts': {'S': ts},
            'anchor_id': {'S': anchor_id},
            'angle': {'S': angle},
            'beacon_id': {'S': beaconID},
            'latitude': {'S': str(lat)},
            'longitude': {'S': str(lng)},
            'rel_dist': {'S': rel_dist},
            'station': {'S': station},
            'x_dist': {'S': x_dist},
            'y_dist': {'S': y_dist},
            'z_dist': {'S': z_dist}
        }
        putItemDDB('uwb-data-from-kinesis', data)

# Call the main function when the script is run

def lambda_handler(event, context):
    
    # Run the main function
    main()
    