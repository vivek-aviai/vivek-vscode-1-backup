
#! This script only works on a live machine with MacOS Ventura.
#! Need to change chromedriver to make it work on Linux.

import pandas as pd
import boto3
import time
import csv
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

timeout = 120

def get_inter_gate_distance(from_gate, to_gate):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--incognito")
    browser = webdriver.Chrome(options=chrome_options)
    # Navigate to the IAH maps website
    url = "https://iahmaps.fly2houston.com/"
    browser.get(url)
    # time.sleep(2)
    wait = WebDriverWait(browser, timeout)

    try:
        wait.until(EC.presence_of_element_located((
            By.XPATH, '//*[@id="mapRenderDiv"]/div/div/div[2]/div/div[2]/div[2]/div[1]/div[1]/div/div/div/div[4]/div/button'))).click()
        # time.sleep(1)
        
        input_fromGate = browser.find_element(
            By.CSS_SELECTOR, "div[data-cy='SearchInputFrom'] input").send_keys(from_gate)

        dropdown_menu = WebDriverWait(browser, timeout).until(EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "div[class='SearchResultsList__Wrapper-sc-vg4obg-0 lOXno']")))
        # options = dropdown_menu.find_elements(
        #     By.XPATH, '//*[@id="mapRenderDiv"]/div/div/div[2]/div/div[2]/div[2]/div[1]/div[2]/div/div/div').click()
        options = dropdown_menu.find_elements(
            By.CSS_SELECTOR, "li[class='SearchResultsList__SearchResultsElem-sc-vg4obg-1 lgDAcU']")
        
        
        for option in options:
            # option.click()
            # break
            # print(option.text)
            label_element = option.find_element(
                By.CSS_SELECTOR, "span[class='label']")
            if label_element.text == from_gate:
                # print("found the gate")
                option.click()
                break
        

        input_toGate = browser.find_element(
            By.CSS_SELECTOR, "div[data-cy='SearchInputTo'] input").send_keys(to_gate)

        dropdown_menu = WebDriverWait(browser, timeout).until(EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "div[class='SearchResultsList__Wrapper-sc-vg4obg-0 lOXno']")))
        options = dropdown_menu.find_elements(
            By.CSS_SELECTOR, "li[class='SearchResultsList__SearchResultsElem-sc-vg4obg-1 lgDAcU']")
        for option in options:
            # option.click()
            # break
            label_element = option.find_element(
                By.CSS_SELECTOR, "span[class='label']")
            if label_element.text == to_gate:
                option.click()
                break

        # Check the security pop-up
        # click_routeButton = browser.find_element(By.XPATH, '//*[@id="mapRenderDiv"]/div/div/div[2]/div/div[4]/div/div[2]/div/div/button[1]').click()
        route_duration = browser.find_element(
            By.XPATH, '//*[@id="mapRenderDiv"]/div/div/div[2]/div/div[2]/div[2]/div[1]/div[2]/div/div[1]/div[2]/span[2]').text.split()[0]
        tmp = browser.find_element(
            By.XPATH, '//*[@id="mapRenderDiv"]/div/div/div[2]/div/div[2]/div[2]/div[1]/div[2]/div/div[1]/div[2]/span[3]')
        route_distance = tmp.text.split(" ")[0]

        label = f'\n \n The distance between {from_gate} and {to_gate} is {route_distance} meters.\nIt will take {route_duration} minutes to transfer. \n \n'
        print(label)
        
        with open('output.txt', 'a') as f:
            f.write(label)            
        return route_distance, route_duration
        
    except NoSuchElementException as e:
        print(f'Error: {e} \n \n')        
        return None, None

    # Close the browser
    browser.quit()


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
        
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            print(f'Response Error: {response}')
            
            
    except Exception as error:
        print(f'Error: {error}. Response: {response}')

def append_to_csv_file(filename, data):
    # Open the CSV file in append mode
    with open(filename, 'a', newline='') as file:
        # Create a CSV writer object
        writer = csv.writer(file)
        # Append the new data to the CSV file
        writer.writerow(data)
        
list = ['A3H', 'A3G', 'A3F', 'A3E', 'A3D', 'A3C', 'A3B', 'A3A', 'A15', 'A14', 'A12', 'A11', 'A10', 'A9', 'A8', 'A7', 'A2', 'A1', 'A17', 'A18', 'A19', 'A20', 'A24', 'A25', 'A26', 'A27', 'A29', 'A30', 'B76', 'B77', 'B79', 'B80', 'B81', 'B83', 'B85', 'B86', 'B86A', 'B87', 'B88', 'B11', 'B1', 'B10', 'B2', 'B9', 'B3', 'B8', 'B4', 'B7', 'B6', 'B5', 'B20', 'B12', 'B19', 'B14', 'B18', 'B15', 'B17', 'B16', 'B21', 'B31', 'B30', 'B22', 'B29', 'B23', 'B28', 'B24', 'B27', 'B26', 'B25', 'B85A', 'B83A ', 'B81A', 'B77A', 'B76A', 'B79A', 'C1', 'C2', 'C29', 'C30', 'C31', 'C32', 'C33', 'C34', 'C35', 'C36', 'C37', 'C39', 'C40', 'C41', 'C42', 'C43', 'C44', 'C45', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10', 'C11', 'C12', 'C14', 'C15', 'D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8', 'D9', 'D10', 'D11', 'D12', 'D9A', 'E1', 'E2', 'E3', 'E4', 'E5', 'E6', 'E7', 'E8', 'E9', 'E10', 'E11', 'E12', 'E14', 'E24', 'E23', 'E15', 'E16', 'E17', 'E18', 'E19', 'E20', 'E21', 'E22']     

def compute_distance():
    # get_inter_gate_distance('United Check-In', 'Gate B24')

    df1 = pd.read_csv('./IAH_gates_short.csv')
    gates1 = df1.gateNumber.values.tolist()

    df2 = pd.read_csv('./IAHgates.csv')
    gates2 = df2.gateNumber.values.tolist()


    # gates1 = ['A30']
    # gates2 = ['D12', 'D9A', 'E1', 'E2', 'E3', 'E4', 'E5', 'E6', 'E7', 'E8', 'E9', 'E10', 'E11', 'E12', 'E14', 'E24', 'E23', 'E15', 'E16', 'E17', 'E18', 'E19', 'E20', 'E21', 'E22'] 
    for gate1 in gates1:
        for gate2 in gates2:
            if gate1 != gate2:
                # print(f'Gate {gate1} to Gate {gate2}')
                dist, duration = get_inter_gate_distance(
                    f'Gate {gate1}', f'Gate {gate2}')
                if dist is not None and duration is not None:
                    data = [gate1, gate2, dist, duration]
                else:
                    data = [gate1, gate2, 'unknown', 'unknown']
                append_to_csv_file('gateDistance_04142023.csv', data)
    
    
    

    # for gate1 in gates1:
    #     for gate2 in gates2:
    #         if gate1 != gate2:
    #             dist, duration = get_inter_gate_distance(
    #                 f'Gate {gate1}', f'Gate {gate2}')
    #             if dist is not None and duration is not None:
    #                 data = {
    #                     'fromGate': {'S': gate1},
    #                     'toGate': {'S': gate2},
    #                     'distance': {'S': dist},
    #                     'duration': {'S': duration}
    #                 }
    #             else:
    #                 data = {
    #                     'fromGate': {'S': gate1},
    #                     'toGate': {'S': gate2},
    #                     'distance': {'S': 'unknown'},
    #                     'duration': {'S': 'unknown'}
    #                 }
    #             putItemDDB('IAH_interGate_Distance', data)

def uploadData():
    # Read the data from the CSV file
    df = pd.read_csv('/home/ubuntu/vivek/Project Aether/native scripts/data/gateDistance.csv')

    # Filter out the records with unknown distance & time
    df['distance'] = df['distance'].replace(['<1'], '1')
    df['time'] = df['time'].replace(['<1'], '1')
    df = df[df['distance'] != 'unknown']
    
    # Create dict records from the dataframe
    data = df.to_dict('records')
    print(data)
    
    # Initialize the DynamoDB client
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table('IAH_interGate_distance')
    
    # Batch write items to the DynamoDB table
    with table.batch_writer() as batch:
        for item in data:
            batch.put_item(Item=item)
    
    

if __name__ == '__main__':
    # compute_distance()
    uploadData()

    


