
import boto3
import random
import time


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


def fetchDataFromDynamoDB(tableName):

    # Get a reference to the DynamoDB table
    table = boto3.client('dynamodb', region_name='us-east-2')
    response = table.scan(TableName=tableName)
    return response['Items']


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


data = fetchDataFromDynamoDB('Ariella_Dev_realTime_Baggage_Monitoring')
count = 1
while (1):
    print(f'Count: {count}')

    start = time.time()
    for item in data:
        # Generate random lat long for teh beacon
        lat, lng = getBboxRandomGPS()

        # Update the new coordinates
        item['beaconLatitude']['S'] = str(lat)
        item['beaconLongitude']['S'] = str(lng)

        # Upload to DDB
        putItemDDB('Ariella_Dev_realTime_Baggage_Monitoring', item)

    end = time.time()
    print(f'Runtime: {end - start} \n\n')

    count += 1

    time.sleep(5)
