# statswales-data-grabber

A small tool to get data from the existing StatsWales system

## Dependencies

```
$ pip3 install requests
$ pip3 install zstandard
```

You need to install the request module before running


## data-grabber

You can run the script to fetch a single datacube fact table from StatsWales2:

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

## spider.py

spider.py contains code to extract all the publically available StatsWales2
metadata and populate an sqlite database (spider.py).
It also includes the schema in SQL form, along with the E-R diagaram as an svg
file and also in dbml format.
The .dbml file can also be viewed here: [External link to
dbdiagram.io](https://dbdiagram.io/d/StatsWales-E-R-diagram-6508558f02bd1c4a5ec93987)

See the block comment at the top of spider.py for more information about how to
use it.

## compress-ugc

Once a snapshot has been created you can compress the downloaded files to
approximately 3.3GiB with the compress-ugc utility.
Run it in the snapshot/XXXX-XX-XX/ directory:

```
$ cd snapshots/2024-08-06/
$ ../../compress-ugc
```

After compression, spider.py will still be able to automatically use the files
rather than redownloading them.

