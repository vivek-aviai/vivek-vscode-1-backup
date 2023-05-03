#!/usr/bin/env python3
# coding: utf-8
# %%
import uuid
import boto3
import random
import requests
import pandas as pd
# %%

# Google Maps API Key (can change it to another, if you have else use this)
MapsAPIKey = 'AIzaSyD8S-qN2aHoNaE3zykKr4iZd6T3_tdwqRQ'


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
        print(f'Error: {error}. Response: {response}')

    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        print(f'{response}')


def getPlaceID(gateAddress):
    # Define the Google Maps Geocoding API endpoint and parameters
    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    params = {
        'address': f'IAH Gate {gateAddress}',
        'key': MapsAPIKey
    }

    # Make a request to the Geocoding API endpoint with the specified parameters
    response = requests.get(url, params=params)

    # Parse the JSON response and extract the place ID
    data = response.json()
    place_id = data['results'][0]['place_id']

    return place_id


def getGateCoordinates(place_id, terminal, gateNumber):
    # Define the Google Maps Places API endpoint and parameters
    url = 'https://maps.googleapis.com/maps/api/place/details/json'
    params = {
        'place_id': place_id,
        'fields': 'name,formatted_address,geometry',
        'key': MapsAPIKey
    }

    # Make a request to the Places API endpoint with the specified parameters
    response = requests.get(url, params=params)

    # Parse the JSON response and extract the place name and address
    data = response.json()
    name = data['result']['name']
    address = data['result']['formatted_address']
    lat = data['result']['geometry']['location']['lat']
    lng = data['result']['geometry']['location']['lng']

    # print(name)
    print(address)
    print(lat, lng)

    # Create an object to upload a single record to DDB
    data = {
        'gateNumber': {'S': gateNumber},
        'terminal': {'S': terminal},
        'latitude': {'S': str(lat)},
        'longitude': {'S': str(lng)},
        'address': {'S': address},
        'GMaps_placeID': {'S': place_id}
    }
    putItemDDB('IAH_gate_coordinates', data)


def main():

    # Load the gateNumber from IAH_Gates.csv
    df = pd.read_csv(
        '/home/ubuntu/vivek/Project SkyLink/native scripts/IAH_Gates.csv')

    # Populate REAL data for IAH gates in the 'IAH_gate_coordinates' table
    for index, row in df.iterrows():
        gateAddress = row['terminal'] + row['gateNumber']
        print(gateAddress)
        place_id = getPlaceID(gateAddress)
        getGateCoordinates(place_id, row['terminal'], row['gateNumber'])

    # Populate synthetic beacon coordintes within IAH in the 'synthetic_baggage_coordinates_IAH' table
    for i in range(1, 100):
        randomStr = str(i).zfill(4)
        beaconID = f"AviAI_v1_{randomStr}"
        lat, lng = getBboxRandomGPS()

        # Create the record object for DDB table
        data = {
            'beaconID': {'S': beaconID},
            'latitude': {'S': str(lat)},
            'longitude': {'S': str(lng)}
        }
        putItemDDB('synthetic_baggage_coordinates_IAH', data)


# Call the main function when the script is run
if __name__ == '__main__':
    main()
# %%
