# statswales-data-grabber

A small tool to get data from the existing StatsWales system

## Running

You need to install the request module before running

```
pip3 install requests
```

Once you've installed requests you can run the script:

```
./data-grabber.py
usage: data-grabber.py [-h] [-u URL | -d DATASET] [-w] [-e] [-o OUT]

Get data from statswales.gov.wales and save to a csv file.

optional arguments:
  -h, --help            show this help message and exit
  -u URL, --url URL     a StatsWales open data url
  -d DATASET, --dataset DATASET
                        A StatsWales opendata name (You can find this on the Metadata tab)
  -w, --welsh           Force getting data from the welsh languague version
  -e, --english         Force getting data from the english languague version
  -o OUT, --out OUT     Override the name of the output file (default: the name of the dataset)
```

Typical usage is:

```
./data-grabber.py -d EDUC0107
```
