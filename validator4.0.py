import csv
from pandas import *
import os
import time

_, _, files = next(os.walk("./"))
file_count = len(files)

print(file_count)

domain_lists = []

file_name = './SCAN_'

files = []


for x in range(1, file_count):
    files.append(file_name + str(x) + ".csv")
    
    print(files[0])


for x in range(0, file_count-1):
    data = read_csv(files[x])

# converting column data to list
    domains = data['site'].tolist()

    domain_lists.append(domains)



from urllib.request import urlopen
from urllib.error import URLError
from urllib.error import HTTPError
from http import HTTPStatus
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed


def test_http(url, validated, invalid):
    try:
        # open a connection to the server with a timeout
        with urlopen("http://www." + url, timeout=3) as connection:
            # get the response code, e.g. 200
            code = connection.getcode()
            if code == 200:
                print(connection.status, url, "200")
                validated.append([url, 'no'])
            return code, validated
    except HTTPError as e:
        if e.code == 403:
            invalid.append([url, str(e.code)])
        elif e.code == 301:
            invalid.append([url, str(e.code)])
        print(e.code, url)
        return invalid
    except URLError as e:
        print(e, url)
    except Exception as e:
        print(e, url)
 
# get the status of a website
def get_website_status(url, validated, invalid):
    # handle connection errors
    try:
        # open a connection to the server with a timeout
        with urlopen("https://www." + url, timeout=3) as connection:
            # get the response code, e.g. 200
            code = connection.getcode()
            if code == 200:
                print(connection.status, url, "200")
                validated.append([url, "yes"])
            return code, validated
    except HTTPError as e:
        if e.code == 403:
            invalid.append([url, str(e.code)])
        elif e.code == 301:
            invalid.append([url, str(e.code)])
        else:
            test_http(url, validated, invalid)
            print(e.code, url)
        return invalid
    except URLError as e:
        print(e, url)
        test_http(url, validated, invalid)
    except Exception as e:
        print(e, url)
        test_http(url, validated, invalid)
 
# interpret an HTTP response code into a status
def get_status(code):
    if code == HTTPStatus.OK:
        return 'OK'
    return 'ERROR'
 
# check status of a list of websites
def check_status_urls(urls):
    
    invalid.append(['site', 'error code'])
    validated.append(['site', 'secure'])
    # create the thread pool
    with ThreadPoolExecutor(100) as executor:
        # submit each task, create a mapping of futures to urls
        future_to_url = {executor.submit(get_website_status, url, validated, invalid):url for url in urls}
        # get results as they are available
        for future in as_completed(future_to_url):
            # get the url for the future
            url = future_to_url[future]
            # get the status for the website
            code = future.result()
            # interpret the status
            status = get_status(code)
            # report status
           # print(f'{url:20s}\t{status:5s}\t{code}')
            return validated, invalid
            
 

# check all urls
for x in range(file_count-1):
    validated = []
    invalid = []
    check_status_urls(domain_lists[x])
    with open("SCAN_" + str(x+1) + "_GOODOUT.csv", "w", newline ='') as f:
                writer = csv.writer(f)
                writer.writerows(validated)
            
    with open("SCAN_" + str(x+1) + "_BADOUT.csv", "w", newline ='') as f:
        writer = csv.writer(f)
        writer.writerows(invalid)
    print("sleeeeping zzzzz")
    time.sleep(600)




