
# This script fetches historical flight schedule data from Cirium API and uploads it to S3
import requests
import boto3
import json

APP_ID='77a3541b'
APP_KEY='21a4d3071fb40a7e9d76f3159add19b4'

def uploadToS3(file_attributes, json_data):
    # Set the S3 bucket name and region
    bucket_name = 'cirium-flight-schedule-data'
    region_name = 'us-east-2'
    
    year = file_attributes['year']
    month = file_attributes['month']
    day = file_attributes['day']
    hourOfTheDay = file_attributes['hourOfTheDay']
    airport = file_attributes['airport']
    type = 'ByArrival' if file_attributes['type'] == 'arr' else 'ByDeparture'
    
    # Create an S3 client
    s3_client = boto3.client('s3', region_name=region_name)
    file_path = f'{airport}/{year}/{month}/{day}/'
    file_name = f'historicalSchedule_{type}_{airport}_{year}-{month}-{day}-{hourOfTheDay}00H.json'
    
    # Upload the file to S3
    s3_client.put_object(Bucket=bucket_name, Key=f'{file_path}{file_name}', Body=json_data, ContentType='application/json')
    print(f"File uploaded to S3: {file_path}{file_name}")

# Python function to systematically fetch every hour's data for a year, and upload it to S3
def fetchCirium_historicalFlightStatus_byAirportArrivals(type):
    # Set the year for which you want to fetch data
    year = 2022
    airport = 'IAH'
    # type = 'arr'

    # Define a for loop to iterate over all the days of the year
    for month in range(1, 13):   # 1 to 12
        for day in range(1, 32):   # 1 to 31
            # Check if the month and day combination is valid for the given year
            if month == 2 and day > 28:
                continue   # Skip invalid day in February for non-leap year
            elif month in [4, 6, 9, 11] and day > 30:
                continue   # Skip invalid day for 30-day months
            elif day > 31:
                continue   # Skip invalid day for 31-day months

            # Define a for loop to iterate over all the hours of the day
            for hour in range(0, 24):   # 0 to 23
                # Define the path variables for the REST API request

                file_attributes = {
                            "year": str(year).zfill(4),
                            "month": str(month).zfill(2),
                            "day": str(day).zfill(2),
                            "hourOfTheDay": str(hour).zfill(2),
                            "airport": airport,
                            "type": type
                            }
                path = f'{file_attributes["year"]}/{file_attributes["month"]}/{file_attributes["day"]}/{file_attributes["hourOfTheDay"]}00H'
                print(path)
                # Make the REST API request
                url = f"https://api.flightstats.com/flex/flightstatus/historical/rest/v3/json/airport/status/{file_attributes['airport']}/{file_attributes['type']}/{file_attributes['year']}/{file_attributes['month']}/{file_attributes['day']}/{file_attributes['hourOfTheDay']}?appId={APP_ID}&appKey={APP_KEY}"
                response = requests.get(url)

                # Check for error in response
                if response.status_code != 200:
                    print(f"Error fetching data for {file_attributes['year']}-{file_attributes['month']}-{file_attributes['day']} {file_attributes['hourOfTheDay']}:00H. Status code: {response.status_code}. Content: {response.text}")
                    continue
                else:
                    #  Upload to S3
                    json_data = json.dumps(response.json()).encode('utf-8')
                    uploadToS3(file_attributes, json_data)
                    
if __name__ == '__main__':
    for item in ['arr', 'dep']:
        fetchCirium_historicalFlightStatus_byAirportArrivals(item)
