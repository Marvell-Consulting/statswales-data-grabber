#!/usr/bin/python3
import json
import csv 
import requests

#dataset = "educ0107"
dataset = "POPU0003".lower()
initalRequestUrl = f"http://open.statswales.gov.wales/en-gb/dataset/{dataset}"

def get_next_link(url):
    print(f"Getting data from '{url}'")
    try:
        r = requests.get(url)
    except RuntimeError as error:
        print(error)
        print(f"Unable to make connection to petitions url '{url}'. Exiting...")
        quit()
    list_data = r.json()
    return list_data

values = []
list_data = get_next_link(initalRequestUrl)
while 'odata.nextLink' in list_data:
    for item in list_data['value']:
        values.append(item)
    nextURL = list_data['odata.nextLink']
    list_data = get_next_link(nextURL)

with open(f'{dataset}.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
    topRow = []
    for key in values[0].keys():
        topRow.append(key)
    spamwriter.writerow(topRow)

    for item in values:
        row = []
        for key in topRow:
            row.append(item[key])
        spamwriter.writerow(row)
    csvfile.close()
