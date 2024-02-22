#!/usr/bin/python3
import json
import csv 
import requests
import sys, getopt
import argparse

welsh = False
initalRequestUrl = "http://open.statswales.gov.wales/en-gb/dataset/XXXX0000"
englistBaseRequestUrl = "http://open.statswales.gov.wales"
welshBaseRequestUrl = "http://agored.statscymru.llyw.cymru"
outputfile = "out.csv"
datasetID = "XXXX0000".lower()

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

def createdatafile(outfile, data):
    with open(outfile, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        topRow = []
        for key in data[0].keys():
            topRow.append(key)
        csvwriter.writerow(topRow)
        csvfile.close()
        return topRow

def writedata(outfile, toprow, data):
         with open(outfile, 'a', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for item in data:
                row = []
                for key in toprow:
                    row.append(item[key])
                csvwriter.writerow(row)
            csvfile.close()

def main(argv):
    global welshBaseRequestUrl, englistBaseRequestUrl
    parser=argparse.ArgumentParser(description="Get data from statswales.gov.wales and save to a csv file.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-u", "--url", help="a StatsWales open data url", type=str)
    group.add_argument("-d", "--dataset", help="A StatsWales opendata name (You can find this on the Metadata tab)", type=str)
    parser.add_argument("-w", "--welsh", help="Force getting data from the welsh languague version", action="store_true")
    parser.add_argument("-e", "--english", help="Force getting data from the english languague version", action="store_true")
    parser.add_argument("-o", "--out", help="Override the name of the output file (default: the name of the dataset)", type=str)
    args = parser.parse_args()

    if args.dataset:
        datasetID = args.dataset.lower()
        englistInitalRequestUrl = f"{englistBaseRequestUrl}/en-gb/dataset/{datasetID}"
        welshInitalRequestUrl = f"{welshBaseRequestUrl}/dataset/{datasetID}"
        if args.welsh:
            initalRequestUrl = welshInitalRequestUrl
        else:
            initalRequestUrl = englistInitalRequestUrl
    elif args.url:
        if welshBaseRequestUrl in args.url:
            welshInitalRequestUrl = args.url
            welsh = True
        elif englistBaseRequestUrl in args.url:
            englistInitalRequestUrl = args.url
            welsh = False
        else:
            print("Invalid URL. You need to supply a URL from StatsWales. Exiting...")
            exit(1)
        initalRequestUrl = args.url
        datasetID = initalRequestUrl.split("/")[-1].lower()
        if welsh:
            englistInitalRequestUrl = f"{englistBaseRequestUrl}/en-gb/dataset/{datasetID}"
        else:
            welshInitalRequestUrl = f"{welshBaseRequestUrl}/dataset/{datasetID}"
    else:
        parser.print_help()
        sys.exit(2)
    
    if args.welsh:
        initalRequestUrl = welshInitalRequestUrl
    
    if args.english: 
        initalRequestUrl = englistInitalRequestUrl

    if args.out:
        outputfile = args.out
    else:
        outputfile = f"{datasetID}.csv"
    
    opendata = get_next_link(initalRequestUrl)
    toprow = createdatafile(outputfile, opendata['value'])
    while 'odata.nextLink' in opendata:
        writedata(outputfile, toprow, opendata['value'])
        nextURL = opendata['odata.nextLink']
        opendata = get_next_link(nextURL)

if __name__ == "__main__":
   main(sys.argv[1:])
