

import requests
import json
import boto3

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


url = f"https://api.flightstats.com/flex/equipment/rest/v1/json/all?appId={appId}&appKey={appKey}"

# response = requests.get(url)
# equipments = json.loads(response.content)['equipment']
# for item in equipments:

#     if item['widebody']:
#         boundingRadius = str(75)
#     else:
#         boundingRadius = str(45)
#     data = {
#         'equipment': {'S': item['iata']},
#         'name': {'S': item['name']},
#         'turboProp': {'BOOL': item['turboProp']},
#         'jet': {'BOOL': item['jet']},
#         'widebody': {'BOOL': item['widebody']},
#         'regional': {'BOOL': item['regional']},
#         'boundingRadius': {'S': boundingRadius}
#     }
#     # print(data)

#     putItemDynamoDB(data, 'boundingRadius_equipment')


