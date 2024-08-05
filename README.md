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

# spider and statswales hypercube (metadata)

Code to extract all the publically available StatsWales2 metadata and populate an sqlite database (spider.py)
Also includes the schema in sql form, along with the E-R diagaram as an svg file and also in dbml format, can also be viewed here:
[External link to dbdiagram.io](https://dbdiagram.io/d/StatsWales-E-R-diagram-6508558f02bd1c4a5ec93987)

## Running spider.py

Input following commands to python to output an sqlite database:

        import spider
        spider.initialise()
        spider.purge_database()
        spider.initialise()
        spider.load_all()

## Additional requirements

The item_id_mismatch.csv, item_id_mismatch_new.csv and the extra.datasetdimensionitems_fixed.json sould be put in the same directory.

item_id_mismatch is a list of item ids that need to be cnaged as they are incorrect in the metadata, and the json file contains the corrected versions.
item_id_mismatch_new is an empty csv with just headers where any additinal item codes that do not match will be recorded.
