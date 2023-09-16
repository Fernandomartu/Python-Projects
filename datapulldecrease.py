import requests
import json
import csv
import threading
import time
import pandas as pd
import os

inpApi = input("enter your API key: ")
print("enter the root url for your bubble application. For example: https://appname.bubbleapps.io/api/1.1/obj/<data table>")
inpurl = input("root url: ")
print("Would you like to fetch the data in ascending or descending order? enter 0 for ascending or 1 for descending")
inporder = input("")
orderval = ""
inpamount = input("input quantity of rows you would like to retrieve, type \"all\" to retrieve all rows: ")


if int(inporder) == 1:
    url = inpurl + "?sort_field=createddate&&descending=true&"
    orderval = "less than"
else:
    url = inpurl + "?"
    orderval = "greater than"


headers = {
    "Authorization": "Bearer " + inpApi,
    "Content-Type": "application/json"
}
row = 0
cursor = 0
lock = threading.Lock()
threads = []

# List of predefined keys

print("enter the data table headers you would like exported to csv, each separated by a comma and a space. For example: <country, city>. Make sure the headers match the field keys in bubble exactly.")
inpfields = input("")

inpfieldslist = inpfields.split(', ')

print(inpfieldslist)
header_keys = inpfieldslist
# header_keys = ["Domain", "_id"]

response = requests.get(url, headers=headers, timeout=30)
jsondata = json.loads(response.content)
with open("output.csv", "w", newline='') as f:
    writer = csv.writer(f)
    writer.writerow(header_keys)  # Write predefined header to CSV
totalcount = int(jsondata['response']['remaining'])
if inpamount == "all":
    pagecount = totalcount // 100
else: 
    pagecount = int(inpamount) // 100
print(pagecount)
def fetch_and_write_data(cursor, row):
    print("total rows written = ", row)
    try:
        if row < 50000:
            response = requests.get(url + "cursor=" + str(cursor), headers=headers, timeout=30)
            print(url + "cursor=" + str(cursor))
        else:
            response = requests.get(url + "cursor=" + str(cursor) + "&constraints=[{\"key\":\"createddate\",\"constraint_type\":\"" + orderval + "\",\"value\":\"" + createddate + "\"}]", headers=headers, timeout=30)
            print(url + "cursor=" + str(cursor) + "&constraints=[{\"key\":\"createddate\",\"constraint_type\":\"" + orderval + "\",\"value\":\"" + createddate + "\"}]")
        if response.status_code == 200:
            data = json.loads(response.content)
            results = data["response"]["results"]
            print("page ", str(row//100), "returned ", len(data["response"]["results"]), " results")
            with lock:
                with open("output.csv", "a", newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    for result in results:
                        row_values = []
                        for key in header_keys:
                            if key in result:
                                row_values.append(result[key])
                            else:
                                row_values.append("N/A")
                        writer.writerow(row_values)  # Write values to CSV
            print("Data has been converted to CSV and appended to output.csv file.", cursor)
        else:
            print("Failed to fetch data from Bubble.io database. Status code:", response.status_code, response.content)
            fetch_and_write_data(cursor, row)
    except Exception as e:
        print("Error occurred:", str(e))
        print("Rescheduling thread.", cursor)
        time.sleep(30)
        fetch_and_write_data(cursor, row)
for i in range(pagecount+2):
    if len(threads) >= 20:
        for thread in threads:
            thread.join()
        threads = []
    thread = threading.Thread(target=fetch_and_write_data, args=(cursor, row))
    thread.start()
    threads.append(thread)
    cursor += 100
    row += 100
    if cursor == 50000 and row == 50000:
        print("RUNNING FIRST OPTION")
        if orderval == "greater than":
            response = requests.get(url + "cursor=49999", headers=headers, timeout=30)
        elif orderval == "less than":
            response = requests.get(inpurl + "?cursor=49999&sort_field=createddate&&descending=true", headers=headers, timeout=30)
        data = json.loads(response.content)
        print(data)
        teststr = data['response']['results'][0]['Created Date']
        print(teststr)
        teststr = teststr.split(".")
        teststrOne = teststr[0]
        num = int(teststr[1].split("Z")[0])
        if orderval == "greater than":
            num = num-1
        elif orderval == "less than":
            num = num+1
        print(num)
        teststrfinal = teststrOne + "." +  str(num) + "Z"
        createddate = teststrfinal
        print(createddate)
        cursor = 0
    elif cursor == 50000 and row > 50000:
        print("RUNNING SECOND OPTION")
        response = requests.get(url + "cursor=49999" + "&constraints=[{\"key\":\"createddate\",\"constraint_type\":\"" + orderval + "\",\"value\":\"" + createddate + "\"}]", headers=headers, timeout=30)
        data = json.loads(response.content)
        print(data)
        teststr = data['response']['results'][0]['Created Date']
        print(teststr)
        teststr = teststr.split(".")
        teststrOne = teststr[0]
        num = int(teststr[1].split("Z")[0])
        if orderval == "greater than":
            num = num-1
        elif orderval == "less than":
            num = num+1
        print(num)
        teststrfinal = teststrOne + "." +  str(num) + "Z"
        createddate = teststrfinal
        print(createddate)
        cursor = 0
    
    
for thread in threads:
    thread.join()
print("Data extraction and conversion to CSV completed successfully.")

def cleandata():
    print("Removing Duplicates and Validating Data...")
    # Read CSV file
    df = pd.read_csv('output.csv')

    # Drop duplicates
    df.drop_duplicates(inplace=True)

    # Remove whitespace
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Write cleaned data to CSV file
    df.to_csv('bubbledata.csv', index=False)

    os.remove('output.csv')

cleandata()