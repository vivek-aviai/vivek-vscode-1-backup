#%%
from dotenv import load_dotenv
import pickle as pkl
import pandas as pd
import boto3
import json
import time
import io


#%%
def filterFlightScheduleData(data, file_attributes):
   
    # Create a dataframe to store the flight schedule data
    df = pd.DataFrame(columns=['flightId', 
                            'operatingCarrierFsCode', 
                            'primaryCarrierFsCode', 
                            'flightNumber', 
                            'departureAirportFsCode', 
                            'arrivalAirportFsCode', 
                            'flightEquipment', 
                            'arrivalDateUTC', 
                            'arrivalDateLocal', 
                            'scheduledGateArrivalUTC', 
                            'scheduledGateArrivalLocal', 
                            'actualGateArrivalUTC', 
                            'actualGateArrivalLocal', 
                            'arrival_terminal', 
                            'arrival_gate', 
                            'baggage', 
                            'codeShareFsCode', 
                            'codeShareFlightNumber'])

    # Manage edge cases for the 'arrivalTerminal', 'arrivalGate', and 'baggage' keys
    
    for items in data["flightStatuses"]:
        
        # Flight details
        flightId = items["flightId"]
        operatingCarrierFsCode = items["operatingCarrierFsCode"]
        primaryCarrierFsCode = items["primaryCarrierFsCode"]
        flightNumber  = items["flightNumber"]
        departureAirportFsCode = items["departureAirportFsCode"]
        arrivalAirportFsCode = items["arrivalAirportFsCode"]
        
        # Aircraft equipment details
        if 'scheduledEquipmentIataCode' in items["flightEquipment"]:
            flightEquipment= items["flightEquipment"]["scheduledEquipmentIataCode"]
        else:
            flightEquipment = None
        
        # Departure and arrival details
        arrivalDateUTC = items["arrivalDate"]["dateUtc"]
        arrivalDateLocal= items["arrivalDate"]["dateLocal"]
        
        scheduledGateArrivalUTC = items["arrivalDate"]["dateUtc"]
        scheduledGateArrivalLocal = items["arrivalDate"]["dateLocal"]
        
        actualGateArrivalUTC = items["arrivalDate"]["dateUtc"]
        actualGateArrivalLocal = items["arrivalDate"]["dateLocal"]
        
        # Check if the 'arrivalTerminal' key exists in the items
        if 'arrivalTerminal' in items['airportResources']:
            arrival_terminal = items['airportResources']['arrivalTerminal']
        else:
            arrival_terminal = None

        # Check if the 'arrivalGate' key exists in the items
        if 'arrivalGate' in items['airportResources']:
            arrival_gate = items['airportResources']['arrivalGate']
        else:
            arrival_gate = None

        # Check if the 'baggage' key exists in the items
        if 'baggage' in items['airportResources']:
            baggage = items['airportResources']['baggage']
        else:
            baggage = None
            
        # Check for codeshare flights
        # if 'fsCode' in items['codeshares'][0]:
        if items['codeshares']:
            codeShareFsCode = items["codeshares"][0]["fsCode"]
            codeShareFlightNumber = items["codeshares"][0]["flightNumber"]
        else:
            codeShareFsCode = None
            codeShareFlightNumber = None
        
        # Create a temporary row to append to the dataframe
        tmp_row = [flightId, 
                   operatingCarrierFsCode, 
                   primaryCarrierFsCode, 
                   flightNumber, 
                   departureAirportFsCode, 
                   arrivalAirportFsCode, 
                   flightEquipment, 
                   arrivalDateUTC, 
                   arrivalDateLocal, 
                   scheduledGateArrivalUTC, 
                   scheduledGateArrivalLocal, 
                   actualGateArrivalUTC, 
                   actualGateArrivalLocal, 
                   arrival_terminal, 
                   arrival_gate, 
                   baggage, 
                   codeShareFsCode, 
                   codeShareFlightNumber]
        
        
        tmp_df = pd.DataFrame([tmp_row], columns=df.columns)
        df = pd.concat([df, tmp_df], ignore_index=True)
        
        # Write the DataFrame to an in-memory buffer as a pickle file
        buffer = io.BytesIO()
        df.to_pickle(buffer)
        
        # Upload the contents of the buffer to S3
        s3 = boto3.client('s3')
        bucket_name = 'cirium-flight-schedule-data'
        file_path = f'{file_attributes["airport"]}/processed_data/{file_attributes["year"]}/{file_attributes["month"]}/{file_attributes["day"]}/'
        file_name = f'processed_historicalSchedule_{file_attributes["type"]}_{file_attributes["airport"]}_{file_attributes["year"]}-{file_attributes["month"]}-{file_attributes["day"]}-{file_attributes["hourOfTheDay"]}00H.pkl'
        s3.put_object(Bucket=bucket_name, Key=f'{file_path}{file_name}', Body=buffer.getvalue())



    
def fetch_and_process_historicalSchedule_data(type):
    # Set the year for which you want to fetch data
    year = 2022
    airport = 'IAH'
    # type = 'arr'
    list_1 = []
    # Define a for loop to iterate over all the days of the year
    for month in range(4, 13):   # 1 to 12
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
                startTime = time.time()
                file_attributes = {
                            "year": str(year).zfill(4),
                            "month": str(month).zfill(2),
                            "day": str(day).zfill(2),
                            "hourOfTheDay": str(hour).zfill(2),
                            "airport": airport,
                            "type": type
                            }

                file_attributes["type"] = 'ByArrival' if file_attributes['type'] == 'arr' else 'ByDeparture'
                # Set up S3 client
                s3 = boto3.client('s3')

                # Fetch file from S3 bucket
                file_path = f'{file_attributes["airport"]}/raw_data/{file_attributes["year"]}/{file_attributes["month"]}/{file_attributes["day"]}/'
                file_name = f'historicalSchedule_{file_attributes["type"]}_{file_attributes["airport"]}_{file_attributes["year"]}-{file_attributes["month"]}-{file_attributes["day"]}-{file_attributes["hourOfTheDay"]}00H.json'
                bucket_name = 'cirium-flight-schedule-data'
                file_key = file_path + file_name  
                
                # Fetch from to S3
                s3 = boto3.client('s3')
                try:     
                    file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
                    data = json.loads(file_obj['Body'].read())                    
                    filterFlightScheduleData(data, file_attributes)

                except Exception as e:
                    print(f'Error: {e} \n File: {file_attributes["year"]}/{file_attributes["month"]}/{file_attributes["day"]}/{file_attributes["hourOfTheDay"]}00H, Type: {file_attributes["type"]}')
                    continue
                
                endTime = time.time()
                print(f'Processing Time for {file_attributes["year"]}/{file_attributes["month"]}/{file_attributes["day"]}/{file_attributes["hourOfTheDay"]}00H:  {endTime - startTime} seconds')
    
    
if __name__ == '__main__':
    for items in ['dep']:
        fetch_and_process_historicalSchedule_data(items)    


