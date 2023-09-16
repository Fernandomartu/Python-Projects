import concurrent.futures
import math
import requests
import json



def main(cursor):
    url = 'https://mysafedomain.com/api/1.1/obj/Websites-Live-Listed'
    headers = {
        "Content-Type": "application/json"
    }

    bubresponse = requests.get(url + "?cursor=" + str(cursor), headers=headers, timeout=30)
    bubdata = json.loads(bubresponse.content)
    remaining = bubdata["response"]["remaining"]
    print('remaining', remaining)

    if remaining <= 0:
        return

    if remaining > 5000:
        threadPoolCount = 50
    else:
        threadPoolCount = math.ceil(remaining/100)

    print('threadpoolcount', threadPoolCount)

    with concurrent.futures.ThreadPoolExecutor(max_workers=threadPoolCount) as threadPool:
        futures = []

        for i in range(threadPoolCount):
            futures.append(threadPool.submit(fetch_data, cursor))
            cursor += 100

        concurrent.futures.wait(futures)
    
        main(cursor)

def fetch_data(cursor):
    url = 'https://mysafedomain.com/api/1.1/obj/Websites-Live-Listed'
    headers = {
        "Content-Type": "application/json"
    }


    response = requests.get(url + "?cursor=" + str(cursor), headers=headers, timeout=30)
    jsondata = json.loads(response.content)

    for i, obj in enumerate(jsondata["response"]["results"]):
        try:
            ipqsurl = 'https://ipqualityscore.com/api/json/url/enter api key here/' + obj["Domain"]
            headers = {
                "Content-Type": "application/json"
            }
            
            BID = obj["BID"]

            ipqs_response = requests.get(ipqsurl, headers=headers, timeout=30)
            ipqs_data = json.loads(ipqs_response.content)

            new_obj = {
                "Domain": ipqs_data.get("domain", ""),
                "ip-address": ipqs_data.get("ip_address", ""),
                "server": ipqs_data.get("server", ""),
                "Content Type": ipqs_data.get("content_type", ""),
                "HTTP Status Code": str(ipqs_data.get("status_code", "")),
                "Page Size": str(ipqs_data.get("page_size", "")),
                "Country Code": ipqs_data.get("country_code", ""),
                "domain-category": ipqs_data.get("category", ""),
                "domain age": ipqs_data.get("domain_age", {}).get("human", ""),
                "Parking" : "",
                "spamming" : "",
                "malware" : "",
                "Suspicious" : "",
                "Phishing" : ""
            }

            parked = ipqs_data.get("parking", "")
            spamming = ipqs_data.get("spamming", "")
            malware = ipqs_data.get("malware", "")
            suspicious = ipqs_data.get("suspicious", "")
            phishing = ipqs_data.get("phishing", "")

            if parked:
                new_obj["Parking"] = "Parked"
            else:
                new_obj["Parking"] = "Not Parked"

            if spamming:
                new_obj["spamming"] = "Spamming Activity Detected"
            else:
                new_obj["spamming"] = "Clean"

            if malware:
                new_obj["malware"] = "Malware Detected"
            else:
                new_obj["malware"] = "Clean"

            if suspicious:
                new_obj["Suspicious"] = "Suspicious"
            else:
                new_obj["Suspicious"] = "Clean"

            if phishing:
                new_obj["Phishing"] = "Potential Phishing Activity Detected"
            else:
                new_obj["Phishing"] = "Clean"

            print(new_obj)
            patch_bubble(BID, new_obj)
                
        except requests.exceptions.RequestException as e:
            print("Request Exception:", e)
            return
    
def patch_bubble(BID, processed_data):


    bub_url = 'https://mysafedomain.com/api/1.1/obj/websites/' + BID
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer enter api key here"
    }

    data = processed_data

    response = requests.patch(bub_url, headers=headers, json=data, timeout=30)

    # Check the response
    if response.status_code == 200 or response.status_code == 204:
        print("PATCH request was successful")
    else:
        print(f"PATCH request failed with status code {response.status_code}")
        print(response.text)

cursor = 0

main(cursor)


        

