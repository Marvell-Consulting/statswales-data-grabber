#!/usr/bin/python3

################################################################################
#
# spider.py
#
# Spider the Open Data URL hierarchy of StatsWales2 so that we can build some
# inspectors and other tools to explore what's there.
#
# Finds metadata about the different StatsWales2 datacubes and loads it into an
# sqlite3 database.
#
#
#    To load the database initially:
#
#    -----
#    $ python3
#    >>> import spider
#    >>> spider.initialise()
#    >>> spider.load_all()
#    -----
#
#    To force a reload from cached downloads:
#
#    -----
#    $ python3
#    >>> import spider
#    >>> spider.initialise()
#    >>> spider.purge_database()
#    >>> spider.initialise()
#    >>> spider.load_all()
#    -----
#
#    To reload entirely from scratch:
#
#    -----
#    $ rm -fr ugc/ tmp/ statswales2.hypercube.sqlite
#    $ git checkout ugc/extra.dataset_property_dimension.xml
#    $ git checkout ugc/extra.odata_dataset_dimensions.json
#    $ python3
#    >>> import spider
#    >>> spider.initialise()
#    >>> spider.load_all()
#    -----
#
#
# Andy Bennett <andyjpb@register-dynamics.co.uk>, 2024/02/26 12:26.
#
################################################################################


import os
import sys
import pathlib
import hashlib
import sqlite3
import codecs
import urllib
import requests
import time
import re
import collections

# Parsing Libraries
# We assume that StatsWales2 is trustworthy so do not currently defend against
# erroneous or maliciously constructed data. For more information see the
# security notes at
#   https://docs.python.org/3/library/xml.html
#   and
#   https://docs.python.org/3/library/json.html
import xml.etree.ElementTree
import json


################################################################################
### Program State.

db        = None
http      = None

# Longest interval to wait between HTTP Requests that time out.
retry_max = 512



################################################################################
### Helpers.

def warn(*args, **kwargs):
    r = print(*args, file=sys.stderr, end="", **kwargs)
    sys.stderr.flush()



################################################################################
### Database schema.

# These statements will be executed in order to incrementally build the
# database schema. Each time the schema needs to be upgraded, the newly added
# statements will be executed.
# Never remove or change anything in this table: only append things to the end.
# The first entry is always one that creates the db_meta table.
db_schema = [
        # Metadata for the sqlite database itself.
        "CREATE TABLE `db_meta` (\n"
        "`key`	TEXT NOT NULL,\n"
        "`value`,\n"
        "PRIMARY KEY(`key`)\n"
        ") WITHOUT ROWID;\n",
        #
        #
        #
        # Local cache of the files we download so that we can easily reprocess
        # them.
        # Use IF NOT EXISTS so the schema upgrade process still works if we
        # wind back to version 0 by DROPping all the other tables.
        "CREATE TABLE IF NOT EXISTS `spider_uri_cache` (\n"
        "`uri`	TEXT NOT NULL,\n"
        "`timestamp`	TEXT NOT NULL,\n"
        "`status`	INTEGER NOT NULL,\n"
        "`content-type`	TEXT,\n"
        "`content-length`	INTEGER,\n"
        "`cache-control`	TEXT,\n"
        "`pragma`	TEXT,\n"
        "`expires`	TEXT,\n"
        "`date`	TEXT,\n"
        "`filename`	TEXT NOT NULL,\n"
        "PRIMARY KEY(`uri`,`timestamp`)\n"
        ") WITHOUT ROWID;\n",
        #
        #
        #
        # Tables for OData metadata definitions
        #   from
        #     http://open.statswales.gov.wales/en-gb/discover/$metadata    (English)
        #     and
        #     http://agored.statscymru.llyw.cymru/cy-gb/discover/$metadata (Welsh)
        #
        # StatsWales.ODataProxy.Web.Models
        #
        #  SwMetadataEnglish           and SwMetadataWelsh
        #    The list of things at
        #      http://open.statswales.gov.wales/en-gb/discover/metadata
        #      and
        #      http://agored.statscymru.llyw.cymru/cy-gb/discover/metadata
        #    Metadata is the information that appears in the tabs below the
        #    datacube, for example "High level information", "Title".
        "CREATE TABLE `odata_metadata_tag` (\n"
        "`dataset`	TEXT NOT NULL,\n"
        "`partition_key`	TEXT NOT NULL,\n"
        "`row_key`	TEXT NOT NULL,\n"
        "`lang`	TEXT NOT NULL NOT NULL,\n"
        "`tag_type`	TEXT NOT NULL,\n"
        "`tag`	TEXT NOT NULL,\n"
        "`description`	TEXT NOT NULL,\n"
        "`timestamp`	TEXT,\n"
        "`etag`	TEXT,\n"
        "PRIMARY KEY(`dataset`,`partition_key`,`row_key`,`lang`),\n"
        "FOREIGN KEY(`dataset`,`lang`) REFERENCES `dataset_collection_info`(`dataset`,`lang`)\n"
        ") WITHOUT ROWID;\n",
        #
        # This index doesn't work because there are 792 occurances of a duplicate tag for a dataset.
        # This is always "Ffynhonnell 1" ("Source 1") and it appears to be
        # incorrectly translated from the English tag "Source 2" or "Source 3".
        # The view `odata_metadata_tag_duplicates` shows the erroneous rows.
        # This view returns rows so we don't bless it with the "check_" prefix
        # that denotes a passing test.
        #"CREATE UNIQUE INDEX `odata_metadata_tag_unique_tags` ON `odata_metadata_tag` (\n"
        #"`dataset`	ASC,\n"
        #"`lang`	ASC,\n"
        #"`tag_type`	ASC,\n"
        #"`tag`	ASC\n"
        #");\n",
        "CREATE VIEW `odata_metadata_tag_duplicates`\n"
        "AS\n"
        "SELECT\n"
        "	`dataset`,\n"
        "	`lang`,\n"
        "	`tag_type`,\n"
        "	`tag`,\n"
        "	COUNT(*) AS `count`\n"
        "FROM `odata_metadata_tag`\n"
        "GROUP BY\n"
        "	`dataset`,\n"
        "	`lang`,\n"
        "	`tag_type`,\n"
        "	`tag`\n"
        "HAVING `count` > 1\n"
        "ORDER BY `count` DESC\n"
        ";\n",
        #
        #
        #
        # Tables for OData reference data definitions
        #   from
        #      http://open.statswales.gov.wales/en-gb/discover/dimension...    (English)
        #      and
        #      http://agored.statscymru.llyw.cymru/cy-gb/discover/dimension... (Welsh)
        #
        # These are the "dimension" / "reference data" / "code lists" that are
        # well defined and can be shared between a number of datacubes. They
        # are generally identifiable when they're used in other places because
        # the "SemanticKey" value is populated with something other than the
        # empty string.
        #
        #  DimensionTypeEnglish        and DimensionTypeWelsh
        #    The list of things at
        #      http://open.statswales.gov.wales/en-gb/discover/dimensiontypes
        #      and
        #      http://agored.statscymru.llyw.cymru/cy-gb/discover/dimensiontypes
        #    Well specified code lists contain an entry in this table and can
        #    be reused across dimensions in datacubes. Not every dimension
        #    refers to a code list in this table. There do not appear to be any
        #    Welsh translations for the user visible parts of this data.
        "CREATE TABLE `odata_dimension_type` (\n"
        "`semantic_key`	TEXT NOT NULL,\n"
        "`type`	TEXT NOT NULL,\n"
        "`subtype`	TEXT NOT NULL,\n"
        "PRIMARY KEY(`semantic_key`)\n"
        ") WITHOUT ROWID;\n",
        #
        "CREATE TABLE `odata_dimension_type_info` (\n"
        "`semantic_key`	TEXT NOT NULL,\n"
        "`lang`	TEXT NOT NULL,\n"
        "`type_description`	TEXT,\n"
        "`subtype_description`	TEXT,\n"
        "`external_uri`	TEXT,\n"
        "PRIMARY KEY(`semantic_key`,`lang`),\n"
        "FOREIGN KEY(`semantic_key`) REFERENCES `odata_dimension_type`(`semantic_key`)\n"
        ") WITHOUT ROWID;\n",
        #
        #  DimensionItemEnglish        and DimensionItemWelsh
        #    The list of things at
        #      http://open.statswales.gov.wales/en-gb/discover/dimensionitems
        #      and
        #      http://agored.statscymru.llyw.cymru/cy-gb/discover/dimensionitems
        #    The items / codes that make up the entries in the code lists.
        "CREATE TABLE `odata_dimension_item` (\n"
        "`semantic_key`	TEXT NOT NULL,\n"
        "`item`	TEXT NOT NULL,\n"
        "`hierarchy`	TEXT,\n"
        "`partition_key`	TEXT,\n"
        "`row_key`	TEXT,\n"
        "`etag`	TEXT,\n"
        "PRIMARY KEY(`semantic_key`,`item`),\n"
        "FOREIGN KEY(`semantic_key`) REFERENCES `odata_dimension_type`(`semantic_key`)\n"
        ") WITHOUT ROWID;\n",
        #
        "CREATE TABLE `odata_dimension_item_info` (\n"
        "`semantic_key`	TEXT NOT NULL,\n"
        "`item`	TEXT NOT NULL,\n"
        "`lang`	TEXT NOT NULL,\n"
        "`description`	TEXT NOT NULL,\n"
        "PRIMARY KEY(`semantic_key`,`item`,`lang`),\n"
        "FOREIGN KEY(`semantic_key`,`item`) REFERENCES `odata_dimension_item`(`semantic_key`,`item`)\n"
        ") WITHOUT ROWID;\n",
        #
        "CREATE TABLE `odata_dimension_item_alternative` (\n"
        "`semantic_key` TEXT NOT NULL,\n"
        "`item` TEXT NOT NULL,\n"
        "`alternative_index`	INTEGER NOT NULL,\n"
        "`alternative_item`	TEXT NOT NULL,\n"
        "PRIMARY KEY(`semantic_key`,`item`,`alternative_index`),\n"
        "FOREIGN KEY(`semantic_key`,`item`) REFERENCES `odata_dimension_item`(`semantic_key`,`item`)\n"
        ") WITHOUT ROWID;\n",
        #
        #
        #
        #  DatasetDimensionEnglish     and DatasetDimensionWelsh
        #    The list of things at
        #      http://open.statswales.gov.wales/en-gb/discover/datasetdimensions
        #      and
        #      http://agored.statscymru.llyw.cymru/cy-gb/discover/datasetdimensions
        #    These tables contain information about the actual dimensions that
        #    each dataset has.
        #    Each StatsWales2 Datacube consists of a "Measure" which is the
        #    core data value and some "Dimensions". This table lists the
        #    dimensions for each cube. There doesn't appear to be any metadata
        #    available to map this to the JSON Keys in .../dataset/<dataset> so
        #    we try to synthesise the dimension column ourselves:
        #    .../dataset/$metadata only gives us the primitive types for each
        #    Value.
        #    The OData feed does not contain a proper key for the identity of
        #    the dimension: we just get DimensionName_ENG and DimensionNameWEL so
        #    we have to rely on the apparent fact that the dimensions appear in the
        #    same order for both the English and Welsh versions of the service.
        #    Therefore we introduce "index" to keep track of each dimension and
        #    make it a UNIQUE field so that we can efficiently use it as a
        #    secondary key to the odata_set_dimension table during the part of
        #    the load where we correlate the responses from each version of the
        #    OData API and as part of the PRIMARY KEY to
        #    odata_dataset_dimension_info because we don't always have the
        #    value of dimension when loading that table.
        "CREATE TABLE `odata_dataset_dimension` (\n"
        "`dataset`	TEXT NOT NULL,\n"
        "`dimension`	TEXT NOT NULL,\n"
        "`dimension_index`	INTEGER NOT NULL UNIQUE,\n"
        "`semantic_key`	TEXT,\n"
        "PRIMARY KEY(`dataset`,`dimension`),\n"
        "FOREIGN KEY(`dataset`) REFERENCES `dataset_collection`(`dataset`),\n"
        "FOREIGN KEY(`dataset`,`dimension`) REFERENCES `dataset_property_dimension`(`dataset`,`dimension`)\n"
        ") WITHOUT ROWID;\n",
        #
        "CREATE UNIQUE INDEX `odata_dataset_dimension_fk_dataset_dimension_index` ON `odata_dataset_dimension` (\n"
        "`dataset`,\n"
        "`dimension_index`\n"
        ");\n",
        #
        "CREATE TABLE `odata_dataset_dimension_info` (\n"
        "`dataset`	TEXT NOT NULL,\n"
        "`dimension_index`	INTEGER NOT NULL,\n"
        "`lang`	TEXT NOT NULL,\n"
        "`dimension_localised`	TEXT,\n"
        "`dimension_name`	TEXT,\n"
        "`description`	TEXT,\n"
        "`dataset_uri`	TEXT,\n"
        "`dataset_dimension_uri`	TEXT,\n"
        "`notes`	TEXT,\n"
        "`external_uri`	TEXT,\n"
        "PRIMARY KEY(`dataset`,`dimension_index`,`lang`),\n"
        "FOREIGN KEY(`dataset`,`dimension_index`) REFERENCES `odata_dataset_dimension`(`dataset`,`dimension_index`),\n"
        "FOREIGN KEY(`dataset`,`lang`) REFERENCES `dataset_collection_info`(`dataset`,`lang`)\n"
        ") WITHOUT ROWID;\n",
        #
        #  DatasetDimensionItemEnglish and DatasetDimensionItemWelsh
        #    The list of things at
        #      http://open.statswales.gov.wales/en-gb/discover/datasetdimensionitems
        #      and
        #      http://agored.statscymru.llyw.cymru/cy-gb/discover/datasetdimensionitems
        #    The lookup tables for each dimension for each dataset. This
        #    provides the values from each dimension that each dataset uses. If
        #    the dimension has a semantic_key then the canonical list of values
        #    are provided by odata_dimension_items as well, which hopefully are
        #    a superset of the values used. If there is no semantic_key then
        #    this is the only source of valid values and they are not
        #    guaranteed to match the values used by other cubes that claim a
        #    dimension with a similar domain type (i.e. that should match).
        "CREATE TABLE `odata_dataset_dimension_item` (\n"
        "`dataset`	TEXT NOT NULL,\n"
        "`dimension`	TEXT NOT NULL,\n"
        "`item`	TEXT NOT NULL,\n"
        "`item_index`	INTEGER NOT NULL UNIQUE,\n"  # Must be part of PK because hous0403/Area (and other dimensions in other datasets) have duplicate codes with different descriptions.
        "`hierarchy`	TEXT,\n"
        "`sort_order`	INTEGER,\n"
        "`semantic_key`	TEXT,\n"
        "PRIMARY KEY(`dataset`,`dimension`,`item`,`item_index`),\n"
        "FOREIGN KEY(`dataset`,`dimension`) REFERENCES `odata_dataset_dimension`(`dataset`,`dimension`)\n"
        ") WITHOUT ROWID;\n",
        #
        "CREATE UNIQUE INDEX `odata_dataset_dimension_item_fk_dataset_item_index` ON `odata_dataset_dimension_item` (\n"
        "`dataset`,\n"
        "`item_index`\n"
        ");\n",
        #
        "CREATE TABLE `odata_dataset_dimension_item_info` (\n"
        "`dataset`	TEXT NOT NULL,\n"
        "`item_index`	INTEGER NOT NULL,\n"
        "`lang`	TEXT NOT NULL,\n"
        "`dimension_localised`	TEXT NOT NULL,\n"
        "`description`	TEXT,\n"
        "`notes`	TEXT,\n"
        "PRIMARY KEY(`dataset`,`item_index`,`lang`),\n"
        "FOREIGN KEY(`dataset`,`item_index`) REFERENCES `odata_dataset_dimension_item`(`dataset`,`item_index`),\n"
        "FOREIGN KEY(`dataset`,`lang`) REFERENCES `dataset_collection_info`(`dataset`,`lang`)\n"
        ") WITHOUT ROWID;\n",
        #
        "CREATE TABLE `odata_dataset_dimension_item_alternative` (\n"
        "`dataset`	TEXT NOT NULL,\n"
        "`item_index`	INTEGER NOT NULL,\n"
        "`alternative_index`	INTEGER NOT NULL,\n"
        "`alternative_item`	TEXT NOT NULL,\n"
        "PRIMARY KEY(`dataset`,`item_index`,`alternative_index`),\n"
        "FOREIGN KEY(`dataset`,`item_index`) REFERENCES `odata_dataset_dimension_item`(`dataset`,`item_index`)\n"
        ") WITHOUT ROWID;\n",
        #
        #
        #
        # Default Namespace
        #  CatalogueEnglish and CatalogueWelsh
        #    The list of things at
        #      http://open.statswales.gov.wales/en-gb/discover/catalogue
        #      and
        #      http://agored.statscymru.llyw.cymru/cy-gb/discover/catalogue
        #    These are the individual datacubes themselves. They are views on
        #    the collections listed in the dataset_collections table.
        "CREATE TABLE `odata_catalogue` (\n"
        "`dataset` TEXT NOT NULL,\n"
        "`partition_key` TEXT NOT NULL,\n"
        "`row_key` TEXT NOT NULL,\n"
        "`folder_path` TEXT NOT NULL,\n"
        "PRIMARY KEY(`dataset`,`partition_key`,`row_key`),\n"
        "FOREIGN KEY(`dataset`) REFERENCES `dataset_collection`(`dataset`)\n"
        ") WITHOUT ROWID;\n",
        #
        "CREATE TABLE `odata_catalogue_info` (\n"
        "`dataset`	TEXT NOT NULL,\n"
        "`partition_key`	TEXT NOT NULL,\n"
        "`row_key`	TEXT NOT NULL,\n"
        "`lang`	TEXT NOT NULL,\n"
        "`dataset_uri`	TEXT NOT NULL,\n"
        "`hierarchy_path`	TEXT NOT NULL,\n"
        "`view_name`	TEXT NOT NULL,\n"
        "PRIMARY KEY(`dataset`,`partition_key`,`row_key`,`lang`),\n"
        "FOREIGN KEY(`dataset`,`partition_key`,`row_key`) REFERENCES `odata_catalogue`(`dataset`,`partition_key`,`row_key`),\n"
        "FOREIGN KEY(`dataset`,`lang`) REFERENCES `dataset_collection_info`(`dataset`,`lang`)\n"
        ") WITHOUT ROWID;\n",
        #
        #
        #
        # Tables for OData data itself
        #   from
        #     http://open.statswales.gov.wales/en-gb/dataset...    (English)
        #     and
        #     http://agored.statscymru.llyw.cymru/cy-gb/dataset... (Welsh)
        #
        # Collections
        #   The list of things at
        #     http://open.statswales.gov.wales/en-gb/dataset
        #     and
        #     http://agored.statscymru.llyw.cymru/cy-gb/dataset
        #   There is one entry for each collection. Each collection may be used
        #   by more than one view.
        "CREATE TABLE `dataset_collection` (\n"
        "`dataset`	TEXT NOT NULL,\n"
        "`href`	TEXT,\n"  # FIXME: Add " NOT NULL,\n" when missing datasets have been resolved.
        "PRIMARY KEY(`dataset`)\n"
        ") WITHOUT ROWID;\n",
        #
        "CREATE TABLE `dataset_collection_info` (\n"
        "`dataset`	TEXT NOT NULL,\n"
        "`lang`	TEXT NOT NULL,\n"
        "PRIMARY KEY(`dataset`,`lang`),\n"
        "FOREIGN KEY(`dataset`) REFERENCES `dataset_collection`(`dataset`)\n"
        ") WITHOUT ROWID;\n",
        #
        # Primitive types
        #   The data types at
        #     http://open.statswales.gov.wales/en-gb/dataset/$metadata
        #     and
        #     http://agored.statscymru.llyw.cymru/cy-gb/dataset/$metadata
        #   These are just the primitive EDM types (i.e. C# Primitive Types).
        #   It seems to be possible to work out which properties refer to the
        #   measure and which refer to the dimensions based on the number of
        #   "_" characters in the property name. From this we infer the schema
        #   of each datacube and record it in the three dataset_property_*
        #   tables.
        #   The data should match between the English and Welsh versions except
        #   for the _ENG and _WEL suffixes.
        "CREATE TABLE `dataset_property_measure` (\n"
        "`dataset`	TEXT NOT NULL,\n"
        "`measure_type`	TEXT NOT NULL,\n"
        "`measure_nullable`	INTEGER NOT NULL,\n"
        "`row_key_type`	TEXT NOT NULL,\n"
        "`row_key_nullable`	INTEGER NOT NULL,\n"
        "`partition_key_type`	TEXT NOT NULL,\n"
        "`partition_key_nullable`	INTEGER NOT NULL,\n"
        "PRIMARY KEY(`dataset`),\n"
        "FOREIGN KEY(`dataset`) REFERENCES `dataset_collection`(`dataset`)\n"
        ") WITHOUT ROWID;\n",
        #
        "CREATE TABLE `dataset_property_dimension` (\n"
        "`dataset`	TEXT NOT NULL,\n"
        "`dimension`	TEXT NOT NULL,\n"
        "`item_type`	TEXT NOT NULL,\n"
        "`item_nullable`	INTEGER NOT NULL,\n"
        "`item_name_type`	TEXT NOT NULL,\n"
        "`item_name_nullable`	INTEGER NOT NULL,\n"
        "`sort_order_type`	TEXT,\n"
        "`sort_order_nullable`	INTEGER,\n"
        "`hierarchy_type`	TEXT,\n"
        "`hierarchy_nullable`	INTEGER,\n"
        "`item_notes_type`	TEXT,\n"
        "`item_notes_nullable`	INTEGER,\n"
        "PRIMARY KEY(`dataset`,`dimension`),\n"
        "FOREIGN KEY(`dataset`) REFERENCES `dataset_collection`(`dataset`)\n"
        ") WITHOUT ROWID;\n",
        #
        "CREATE TABLE `dataset_property_dimension_alternative` (\n"
        "`dataset`	TEXT NOT NULL,\n"
        "`dimension`	TEXT NOT NULL,\n"
        "`alternative_index`	INTEGER NOT NULL,\n"
        "`alternative_type`	TEXT NOT NULL,\n"
        "`alternative_nullable`	INTEGER NOT NULL,\n"
        "PRIMARY KEY(`dataset`,`dimension`,`alternative_index`),\n"
        "FOREIGN KEY(`dataset`,`dimension`) REFERENCES `dataset_property_dimension`(`dataset`,`dimension`)\n"
        ") WITHOUT ROWID;\n",
        #
        #
        #
        # Views to check (where possible) that the data loads are correct.
        #
        #  Returns the datasets that are present in one language but not the other.
        #  If no rows are returned then each dataset that is present in
        #  dataset_collection has entries for both English and Welsh.
        "CREATE VIEW `check_dataset_collection`\n"
        "(`dataset`, `lang`, `error`)\n"
        "AS\n"
        "SELECT\n"
        " a.`dataset`,\n"
        " a.`lang`,\n"
        " 'Missing from cy-gb' AS `error`\n"
        "FROM\n"
        "	`dataset_collection` AS a\n"
        "LEFT JOIN\n"
        "	`dataset_collection` AS b\n"
        "ON\n"
        "	a.`dataset` = b.`dataset` AND\n"
        "	b.lang = 'cy-gb'\n"
        "WHERE\n"
        "	a.lang = 'en-gb' AND\n"
        "	b.dataset IS NULL\n"
        "\n"
        "UNION ALL\n"
        "\n"
        "SELECT\n"
        " a.`dataset`,\n"
        " a.`lang`,\n"
        " 'Missing from en-gb' AS `error`\n"
        "FROM\n"
        "	`dataset_collection` AS a\n"
        "LEFT JOIN\n"
        "	`dataset_collection` AS b\n"
        "ON\n"
        "	a.`dataset` = b.`dataset` AND\n"
        "	b.lang = 'en-gb'\n"
        "WHERE\n"
        "	a.lang = 'cy-gb' AND\n"
        "	b.dataset IS NULL\n"
        ";\n",
        #
        #  Returns the datasets that are present in odata_catalogue don't have
        #  exactly two entries in odata_catalogue_info. i.e. one for English
        #  and one for Welsh.
        #  If no rows are returned then all datasets in odata_catalogue have
        #  the correct number of entries in odata_catalogue_info. However,this
        #  view does not detect entries in odata_catalouge_info that have no
        #  entry in odata_catalogue. This will be enforced with a FOREIGN KEY
        #  constraint.
        "CREATE VIEW `check_odata_catalogue`\n"
        "(`dataset`, `partition_key`, `row_key`, `odata_catalogue_info_entries`)\n"
        "AS\n"
        "SELECT\n"
        "	a.`dataset`,\n"
        "	a.`partition_key`,\n"
        "	a.`row_key`,\n"
        "	COUNT(b.`dataset`) as `odata_catalogue_info_entries`\n"
        "FROM\n"
        "	`odata_catalogue` AS a\n"
        "LEFT JOIN\n"
        "	`odata_catalogue_info` AS b\n"
        "ON\n"
        "	a.`dataset`       = b.`dataset` AND\n"
        "	a.`partition_key` = b.`partition_key` AND\n"
        "	a.`row_key`       = b.`row_key`\n"
        "GROUP BY\n"
        "	a.`dataset`,\n"
        "	a.`partition_key`,\n"
        "	a.`row_key`\n"
        "HAVING `odata_catalogue_info_entries` != 2\n"
        ";\n",
        #
        #  Returns the reference data dimensions that are present in one
        #  language but not the other.
        #  If no rows are returned then each dataset that is present in
        #  odata_dimension_type_info has entries for both English and Welsh.
        #  However, we don't check that the contents of the Welsh entries
        #  because we know that they are all empty strings.
        "CREATE VIEW `check_odata_dimension_type_info`\n"
        "(`semantic_key`, `lang`, `error`)\n"
        "AS\n"
        "SELECT\n"
        " a.`semantic_key`,\n"
        " a.`lang`,\n"
        " 'Missing from cy-gb' AS `error`\n"
        "FROM\n"
        "	`odata_dimension_type_info` AS a\n"
        "LEFT JOIN\n"
        "	`odata_dimension_type_info` AS b\n"
        "ON\n"
        "	a.`semantic_key` = b.`semantic_key` AND\n"
        "	b.lang = 'cy-gb'\n"
        "WHERE\n"
        "	a.lang = 'en-gb' AND\n"
        "	b.semantic_key IS NULL\n"
        "\n"
        "UNION ALL\n"
        "\n"
        "SELECT\n"
        " a.`semantic_key`,\n"
        " a.`lang`,\n"
        " 'Missing from en-gb' AS `error`\n"
        "FROM\n"
        "	`odata_dimension_type_info` AS a\n"
        "LEFT JOIN\n"
        "	`odata_dimension_type_info` AS b\n"
        "ON\n"
        "	a.`semantic_key` = b.`semantic_key` AND\n"
        "	b.lang = 'en-gb'\n"
        "WHERE\n"
        "	a.lang = 'cy-gb' AND\n"
        "	b.semantic_key IS NULL\n"
        ";\n",
        #
        # Similar to check_odata_dimension_type_info but highlights rows where
        # the contents of one of the translations has at least one blank field.
        # This view returns rows so we don't bless it with the "check_" prefix
        # that denotes a passing test.
        "CREATE VIEW `odata_dimension_type_info_empty_translation`\n"
        "(`semantic_key`, `lang`, `error`)\n"
        "AS\n"
        "SELECT\n"
        " a.`semantic_key`,\n"
        " a.`lang`,\n"
        " 'Translation is blank for cy-gb' AS `error`\n"
        "FROM\n"
        "	`odata_dimension_type_info` AS a\n"
        "LEFT JOIN\n"
        "	`odata_dimension_type_info` AS b\n"
        "ON\n"
        "	a.`semantic_key` = b.`semantic_key` AND\n"
        "	b.`lang` = 'cy-gb'\n"
        "WHERE\n"
        "	a.`lang` = 'en-gb' AND\n"
        "	(b.`semantic_key` IS NULL OR\n"
        "	b.`type_description` == \"\" OR\n"
        "	b.`subtype_description` == \"\")\n"
        "\n"
        "UNION ALL\n"
        "\n"
        "SELECT\n"
        " a.`semantic_key`,\n"
        " a.`lang`,\n"
        " 'Translation is blank for en-gb' AS `error`\n"
        "FROM\n"
        "	`odata_dimension_type_info` AS a\n"
        "LEFT JOIN\n"
        "	`odata_dimension_type_info` AS b\n"
        "ON\n"
        "	a.`semantic_key` = b.`semantic_key` AND\n"
        "	b.`lang` = 'en-gb'\n"
        "WHERE\n"
        "	a.`lang` = 'cy-gb' AND\n"
        "	(b.`semantic_key` IS NULL OR\n"
        "	b.`type_description` == \"\" OR\n"
        "	b.`subtype_description` == \"\")\n"
        ";\n",
        #
        #  Returns the reference data dimension items / codes that are present
        #  in one language but not the other.
        #  If no rows are returned then each dataset that is present in
        #  odata_dimension_item_info has entries for both English and Welsh.
        #  The description is not always populated for one or both languages
        #  however we don't check for that.
        "CREATE VIEW `check_odata_dimension_item_info`\n"
        "(`semantic_key`, `item`, `lang`, `error`)\n"
        "AS\n"
        "SELECT\n"
        " a.`semantic_key`,\n"
        " a.`item`,\n"
        " a.`lang`,\n"
        " 'Missing from cy-gb' AS `error`\n"
        "FROM\n"
        "	`odata_dimension_item_info` AS a\n"
        "LEFT JOIN\n"
        "	`odata_dimension_item_info` AS b\n"
        "ON\n"
        "	a.`semantic_key` = b.`semantic_key` AND\n"
        "	a.`item` = b.`item` AND\n"
        "	b.lang = 'cy-gb'\n"
        "WHERE\n"
        "	a.lang = 'en-gb' AND\n"
        "	b.semantic_key IS NULL\n"
        "\n"
        "UNION ALL\n"
        "\n"
        "SELECT\n"
        " a.`semantic_key`,\n"
        " a.`item`,\n"
        " a.`lang`,\n"
        " 'Missing from en-gb' AS `error`\n"
        "FROM\n"
        "	`odata_dimension_item_info` AS a\n"
        "LEFT JOIN\n"
        "	`odata_dimension_item_info` AS b\n"
        "ON\n"
        "	a.`semantic_key` = b.`semantic_key` AND\n"
        "	a.`item` = b.`item` AND\n"
        "	b.lang = 'en-gb'\n"
        "WHERE\n"
        "	a.lang = 'cy-gb' AND\n"
        "	b.semantic_key IS NULL\n"
        ";\n",
        #
        #  Returns the dataset dimensions that are present in one language but
        #  not the other.
        #  If no rows are returned then each dataset that is present in
        #  odata_dataset_dimension_info has entries for both English and Welsh.
        "CREATE VIEW `check_odata_dataset_dimension_info`\n"
        "(`dataset`, `dimension_index`, `lang`, `error`)\n"
        "AS\n"
        "SELECT\n"
        "	a.`dataset`,\n"
        "	a.`dimension_index`,\n"
        "	a.`lang`,\n"
        "	'Missing from cy-gb' AS `error`\n"
        "FROM\n"
        "	`odata_dataset_dimension_info` AS a\n"
        "LEFT JOIN\n"
        "	`odata_dataset_dimension_info` AS b\n"
        "ON\n"
        "	a.`dataset` = b.`dataset` AND\n"
        "	a.`dimension_index` = b.`dimension_index` AND\n"
        "	b.`lang` = 'cy-gb'\n"
        "WHERE\n"
        "	a.`lang` = 'en-gb' AND\n"
        "	b.`dataset` IS NULL\n"
        "\n"
        "UNION ALL\n"
        "\n"
        "SELECT\n"
        "	a.`dataset`,\n"
        "	a.`dimension_index`,\n"
        "	a.`lang`,\n"
        "	'Missing from en-gb' AS `error`\n"
        "FROM\n"
        "	`odata_dataset_dimension_info` AS a\n"
        "LEFT JOIN\n"
        "	`odata_dataset_dimension_info` AS b\n"
        "ON\n"
        "	a.`dataset` = b.`dataset` AND\n"
        "	a.`dimension_index` = b.`dimension_index` AND\n"
        "	b.`lang` = 'en-gb'\n"
        "WHERE\n"
        "	a.`lang` = 'cy-gb' AND\n"
        "	b.`dataset` IS NULL\n"
        ";\n",
        ]



################################################################################
### Database helpers.

# Gets the value of the key from the db_meta table.
def db_meta_get(key):

    c = db.cursor()
    r = c.execute("SELECT `value` FROM `db_meta` WHERE `key` = ?;", (key,))
    r = r.fetchone()

    if (r is None):
        return None
    else:
        return r[0]


# Sets a new value for the key in the db_meta table, overwriting or creating it
# where necessary.
def db_meta_set(key, value):
    c = db.cursor()
    c.execute("SAVEPOINT db_meta_set");
    r = c.execute("DELETE FROM `db_meta` WHERE `key` = ?;", (key,))
    r = c.execute("INSERT INTO `db_meta` (`key`, `value`) VALUES (?, ?);", (key, value,))
    c.execute("RELEASE db_meta_set")


# Returns the current version of the on disk database schema which is the
# number of statements in db_schema that have been successfully applied to it.
def database_version():
    try:
        v = db_meta_get("schema_version")
        if (v is None):
            return 1  # The first entry in db_schema is always the one that creates the db_meta table.
        else:
            return v
    except sqlite3.OperationalError:
        return 0  # Perhaps the db_meta table doesn't exist yet.


def upgrade_database():

    old_version = database_version()
    new_version = len(db_schema)
    c           = db.cursor()

    if (old_version > new_version):
        raise AssertionError("upgrade_database: Database is at schema_version %d but we only understand up to %d" % (old_version, new_version))

    if (old_version < new_version):
        warn("upgrade_database: ")

        c.execute("SAVEPOINT upgrade_database");

        # Upgrade the schema itself.
        for s in db_schema[old_version:]:
            r = c.execute(s)

        # Record the current version number.
        db_meta_set("schema_version", new_version)

        c.execute("RELEASE upgrade_database")

        warn("successfully upgraded from version %d to %d.\n" % (old_version, new_version))


# Remove all database objects except spider_uri_cache.
def purge_database():

    warn("purge_database()\n")

    # VACUUM needs the connection to not be inside a transaction, so refuse to
    # even start purging if it is.
    if (db.in_transaction):
        raise AssertionError("purge_database: Refusing to purge the database whilst it's already in a transaction!")

    c = db.cursor()

    c.execute("PRAGMA foreign_keys = OFF;")
    c.execute("SAVEPOINT purge_database");

    # DROP all the tables except spider_uri_cache.

    r = c.execute(SELECT("sqlite_master", ("name",),
        "WHERE `name` != 'spider_uri_cache' AND `type` = 'table';"))

    # Read all the data at once from sqlite_master because DROP... will need to
    # get a write lock on it.
    for r in  r.fetchall():
        c.execute("DROP TABLE %s;" % sqlite3_quote_identifier(r[0]))


    # DROP all the views.

    r = c.execute(SELECT("sqlite_master", ("name",),
        "WHERE `type` = 'view';"))

    # Read all the data at once from sqlite_master because DROP... will need to
    # get a write lock on it.
    for r in  r.fetchall():
        c.execute("DROP VIEW %s;" % sqlite3_quote_identifier(r[0]))


    c.execute("RELEASE purge_database")
    c.execute("PRAGMA foreign_keys = ON;")

    db.commit()
    c.execute("VACUUM")


# Adapted from https://gist.github.com/jeremyBanks/1083518/584008c38a363c45acb84e4067b5188bb36e20f4
def sqlite3_quote_identifier(s, errors="strict"):
    encodable = s.encode("utf-8", errors).decode("utf-8")

    nul_index = encodable.find("\x00")

    if nul_index >= 0:
        error = UnicodeEncodeError("utf-8", encodable, nul_index, nul_index + 1, "NUL not allowed")
        error_handler = codecs.lookup_error(errors)
        replacement, _ = error_handler(error)
        encodable = encodable.replace("\x00", replacement)

    return "`" + encodable.replace("`", "``") + "`"


# converter procedures used to prepare the bindings in INSERT, CHECK_ROW, etc
# can raise this exception to indicate that no database operation (e.g. INSERT)
# should be done for this row afterall.
# It is not an error, rather it signals that the converter knows a reason why
# this data does not result in a relevant or correct database entry.
class SkipRow(Exception):
    pass


# Generates a SELECT query string.
# Quotes all the identifiers supplied in table and columns.
# Any remaining arguments should be generated with WHERE or similar procedures
# as they will be joined with whitespace and included literally.
def SELECT(table, columns, *args):

    table   = sqlite3_quote_identifier(table)
    columns = [sqlite3_quote_identifier(x) for x in columns]
    columns = ", ".join(columns)

    q = "SELECT %s FROM %s" % (columns, table)
    if args:
        q = "%s %s" % (q, " ".join(args))

    return q

# Generates a procedure that runs an INSERT query based on the provided map
# data structure.
# the_map is an array of specifications for each column to be inserted. Each
# specification is a tuple containing 3 elements:
#   + The column name in the database table.
#   + A procedure that is called to provide column values that can be passed to
#     the sqlite3 library.
#   + Data that is passed as the first argument of the procedure.
# thus:
#   [(column_name, converter, state), ...]
# Quotes all of the identifiers and generates unnamed, positional SQL
# parameters for all of the columns.
# Returns a procedure that takes a cursor and a dictionary and performs the
# INSERT.
# If we were using a database over a socket rather than in core then it would
# be nice to refactor this to be able to use a batching interface.
def INSERT(table, the_map):

    # Build the query string.
    table      = sqlite3_quote_identifier(table)
    columns    = [sqlite3_quote_identifier(x[0]) for x in the_map]
    columns    = ",".join(columns)
    parameters = ["?" for _ in the_map]
    parameters = ",".join(parameters)
    query      = "INSERT INTO %s (%s) VALUES (%s);" % (table, columns, parameters)

    # Prepare the value that will be written into a column.
    def bind(spec, dictionary):
        (column, converter, state) = spec
        return converter(state, dictionary)

    # Perform the INSERT given the data in dictionary.
    def closure(cursor, dictionary):
        # INSERT the row unless one of the converters tells us otherwise.
        # We go to some lengths to ensure that every converter for every
        # binding runs because some (for example autoincrements) have side
        # effects and we want the side effects to have a chance to happen even
        # if the row is not INSERTed.
        bindings = []
        failed   = 0

        for x in the_map:
            try:
                bindings.append(bind(x, dictionary))
            except SkipRow:
                failed += 1

        if (failed == 0):
            try:
                cursor.execute(query, bindings)
            except Exception as e:
                warn("INSERT: Error during query:\n")
                warn("\t%s\n" % query)
                warn("\t%s\n" % bindings)
                warn("\n")
                raise e

    return closure


# Generates a procedure that runs a SELECT query based on the provided map data
# structure and compares the given data with the data returned from the
# database.
# The interface is similar to INSERT().
# If we were using a database over a socket rather than in core then it would
# be nice to work out a way to do this with range queries rather than point
# queries.
def CHECK_ROW(table, the_map):

    # Build the query string.
    table            = sqlite3_quote_identifier(table)
    columns          = [sqlite3_quote_identifier(x[0]) for x in the_map]
    where_conditions = ["(%s = ? OR %s IS ?)" % (x, x) for x in columns]  # (= OR IS) to handle NULLs.
    where_conditions = " AND ".join(where_conditions)
    query            = "SELECT COUNT(*) FROM %s WHERE %s;" % (table, where_conditions)

    # Prepare the value that will be used in the WHERE clause.
    def bind(spec, dictionary):
        (column, converter, state) = spec
        return converter(state, dictionary)

    # Perform the SELECT given the data in dictionary and check that the count
    # is 1.
    def closure(cursor, dictionary):
        # SELECT the row unless one of the converters tells us otherwise.
        # We go to some lengths to ensure that every converter for every
        # binding runs because some (for example autoincrements) have side
        # effects and we want the side effects to have a chance to happen even
        # if the row is not INSERTed.
        bindings = []
        failed   = 0

        for x in the_map:
            try:
                b = bind(x, dictionary)
                bindings.append(b)  # =  condition
                bindings.append(b)  # IS condition
            except SkipRow:
                failed +=1

        if (failed == 0):
            r = cursor.execute(query, bindings)
            r = r.fetchone()
            if (r[0] != 1):
                raise AssertionError("CHECK_ROW: Expected to find a single row in the database but there were %d during %s with %s!" % (r[0], query, bindings))

    return closure


# Generates a procedure that does not run any operations on the database.
# This provides an interface compatible with INSERT and CHECK_ROW that can be
# used to ignore rows that cause known problems.
def IGNORE_ROW(table, the_map):

    def closure(cursor, dictionary):
        pass

    return closure


# A helper to drive INSERT and CHECK_ROW to make a list of database
# query-running procedures.
def make_procs(table, the_map, insert, check):

    procs = []

    if (table in insert):
        procs.append(INSERT(table, the_map))

    if (table in check):
        procs.append(CHECK_ROW(table, the_map))

    return procs



################################################################################
### URLs and HTTP.

# Checks the URI cache and returns information about the cached object if it
# was found.
def check_uri_cache(uri, timestamp=None):

    c             = db.cursor()
    c.row_factory = sqlite3.Row

    if (timestamp is None):
        r = c.execute(SELECT("spider_uri_cache",
            ("timestamp", "status", "content-type", "content-length", "filename"),
            "WHERE `uri` = ?"), (uri,))
    else:
        r = c.execute(SELECT("spider_uri_cache",
            ("timestamp", "status", "content-type", "content-length", "filename"),
            "WHERE `uri` = ? AND `timestamp` = ?"), (uri, timestamp))

    r = r.fetchone()

    return r


# Fetches a uri either from the net or the cache.
# If the uri is fetched from the net then it is put into the cache.
#
# base - The Base URI as a string. Usually ebu or wbu but can be a complete
#        URI such as one returned as an OData pagination link.
# path - The path as a list or tuple of path segments to append to the base.
#
# Returns the location of the local data in the cache.
def fetch_uri(base, path=None):

    now = time.time()

    # Prepare the URI.
    if (path is None):
        uri = base
    else:
        if (isinstance(path, str)):
            path = urllib.parse.quote(path, safe="")
        else:
            path = [urllib.parse.quote(x, safe="") for x in path]
            path = "/".join(path)

        uri = urllib.parse.urljoin(base, path)

    pretty_uri = uri if (len(uri) <= 30) else "%s...%s" % (uri[:15], uri[-15:])
    warn("fetch_uri: %s" % pretty_uri)

    # Check the cache.
    local = check_uri_cache(uri)

    # Cache hit: just return it
    if (local is not None):
        warn(" using ugc/%s... from cache.\n" % local["filename"][:7])
        return local

    # Fetch the URI from the Internet.
    # Start with quite a large timeout because some of the metadata Responses
    # are large, take the server a long time to generate and it seems that it
    # doesn't start sending anything until it's generated the whole response.
    retry_interval = 64
    while True:
        try:
            # Set timeouts for connect() and read() to retry_interval.
            response = http.get(uri, timeout = (retry_interval, retry_interval))
            break
        except requests.exceptions.ReadTimeout:
            warn(" [TIMEOUT]\n")
            warn("  HTTP Request timed out! Retrying in %d seconds...\n" % retry_interval)
        except requests.exceptions.ConnectTimeout:
            warn(" [TIMEOUT]\n")
            warn("  TCP connect  timed out! Retrying in %d seconds...\n" % retry_interval)
        time.sleep(retry_interval)
        retry_interval *= 2
        if (retry_interval > retry_max):
            retry_interval = retry_max
        warn("fetch_uri: %s" % pretty_uri)

    # Cache the response body.
    filename = "temp-%d" % os.getpid()
    digest   = hashlib.sha256()

    with open("tmp/%s" % filename, 'xb') as fd:
        for chunk in response.iter_content(chunk_size=4096):
            fd.write(chunk)
            digest.update(chunk)

    # Rename it the first time as we know what it is now, but there's a chance
    # we may not be able to get it into the URI cache if for some reason it's
    # already there (either because another URI results in exactly the same
    # document or there's some vestigial traces of cache entries that also
    # failed or have not been cleared up completely.
    os.rename("tmp/%s" % filename, "tmp/%s" % digest.hexdigest())


    # Populate the cache.
    c = db.cursor()
    c.execute("SAVEPOINT fetch_uri");
    r = c.execute("INSERT INTO `spider_uri_cache` (`uri`, `timestamp`, `status`, `content-type`, `content-length`, `cache-control`, `pragma`, `expires`, `date`, `filename`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
            (uri,
                now,
                response.status_code,
                response.headers['content-type'],
                response.headers['content-length'],
                response.headers['cache-control'],
                response.headers['pragma'],
                response.headers['expires'],
                response.headers['date'],
                digest.hexdigest()))

    os.rename("tmp/%s" % digest.hexdigest(), "ugc/%s" % digest.hexdigest())

    warn(" [%d], cached as ugc/%s...\n" % (response.status_code, digest.hexdigest()[:7]))

    c.execute("RELEASE fetch_uri")

    # Check the cache so that we return the same thing in cached and non-cached
    # situations.
    local = check_uri_cache(uri, now)

    return local


# Generates a path to a file in the User Generated Content store.
def from_ugc(filename):
    return "ugc/%s" % filename


# The content-type response header is a string containing a MIME Type followed
# by an optional set of key/value pairs.
#
# Returns a two element tuple thus:
#   (mime_type, dictionary)
def parse_content_type(header):

    components = header.split(";")
    value      = components[0]
    options    = components[1:]
    dictionary = {}

    for kv in options:
        kv            = kv.strip().split("=")
        k             = kv[0]
        v             = kv[1]
        dictionary[k] = v

    return (value, dictionary)



################################################################################
### Load things from StatsWales2.

ebu = "http://open.statswales.gov.wales/en-gb/"     # English Base URI.
wbu = "http://agored.statscymru.llyw.cymru/cy-gb/"  # Welsh Base URI.


## Conversion Procedures
##
## Procedures to convert individual values received from StatsWales to values
## we can put in the database.

# Looks up key in the dictionary.
# If lang is specified then first extracts the key from the supplied
# dictionary.
def lookup(lang = None):
    if (lang is None):
        return lambda key, dictionary: dictionary[key]
    else:
        return lambda keys, dictionary: dictionary[keys[lang]]

# Looks up key in the dictionary.
# If the value retrieved from the dictionary for the key is "" then the SkipRow
# exception is thrown to indicate that no database INSERT should be generated
# for this row..
def lookup_require_not_empty(key, dictionary):
    v = dictionary[key]
    if (v == ""):
        raise SkipRow
    else:
        return v

# Returns the first argument.
def identity(x, _):
    return x

dimension_name_to_key_re = re.compile(r"[^a-zA-Z]+")

# It's seems to be the dimension name in english with all the spaces and non
# alphabetic characters removed!
# We've seen some non-alphanumeric characters, including 'Standard Output on a
# farm (\u00e2\u0082\u00ac000s)' and we are confident that applying the regex
# results in the correct JSON Key-part.
def dimension_name_to_key(lang):
    def closure(keys, dictionary):
        v = dictionary[keys[lang]]
        v = dimension_name_to_key_re.sub("", v)
        return v
    return closure

# Make a procedure that implements an automatically incrementing column value
# that starts at n.
def make_autoincrement(n):
    def incr(_1, _2):
        nonlocal n
        v = n
        n += 1
        return v
    return incr


## Drivers
##
## Procedures that can load different types of commonly formatted data.

# Loads OData JSON files
# For ones that are paginated, the pages will be followed until there are no
# more.
def load_json_pages(local_file, cursor, insert_procs):

    more = True

    while (more):

        filename = from_ugc(local_file["filename"])

        content_type = parse_content_type(local_file["content-type"])
        mime_type    = content_type[0]
        options      = content_type[1]

        if (mime_type != "application/json"):
            raise AssertionError("load_odata_catalogue: Expected mime-type of application/json but %s has %s!" % (filename, content_type))

        tree = {}
        with open(filename, 'rb') as fd:
            tree = json.load(fd)

        value = tree["value"]
        more  = tree.get("odata.nextLink", False)

        # Insert the correct data from each value into each table.
        for p in insert_procs:
            for v in value:
                p(cursor, v)

        # Carry on with the next page.
        if (more):
            local_file = fetch_uri(more)


# Loads the dataset_collection table.
# Does not depend on any other tables being loaded.
# Succeeds or Throws.
def load_dataset_collections():

    warn("load_dataset_collections()\n")

    c = db.cursor()
    c.execute("SAVEPOINT load_dataset_collections");

    def load_collection(dataset, href, lang):

            QUERY = None
            if (lang == "en-gb"):
                QUERY = INSERT
            else:
                QUERY = CHECK_ROW

            collection_map = [
                    ("dataset", identity, dataset),
                    ("href",    identity, href),
                    ]

            info_map       = [
                    ("dataset", identity, dataset),
                    ("lang",    identity, lang),
                    ]

            QUERY ("dataset_collection",      collection_map)(c, {})
            INSERT("dataset_collection_info", info_map)      (c, {})

    def load_from(base, lang):

        local    = fetch_uri(base, ("dataset"));
        filename = from_ugc(local["filename"])

        content_type = parse_content_type(local["content-type"])
        mime_type    = content_type[0]
        options      = content_type[1]

        if (mime_type != "application/atomsvc+xml"):
            raise AssertionError("load_dataset_collections: Expected mime-type of application/atomsvc+xml but %s has %s!" % (filename, content_type))

        tree       = xml.etree.ElementTree.parse(filename)
        workspaces = tree.findall("./{http://www.w3.org/2007/app}workspace")

        if (len(workspaces) != 1):
            raise AssertionError("load_dataset_collections: Expected a single workspace but %d has %d!" % (filename, len(workspaces)))

        workspace = workspaces[0]
        titles    = workspace.findall("./{http://www.w3.org/2005/Atom}title[@type='text']")

        if (len(titles) != 1):
            raise AssertionError("load_dataset_collections: Expected a single title but %d has %d!" % (filename, len(title)))

        title = titles[0]
        title = title.text

        if (title != "Default"):
            raise AssertionError("load_dataset_collections: Expected a workspace with title 'Default' but %s has %s" % (filename, title))

        collections = workspace.findall("./{http://www.w3.org/2007/app}collection")

        for x in collections:
            load_collection(x[0].text, x.attrib["href"], lang)


    load_from(ebu, "en-gb")
    load_from(wbu, "cy-gb")

    # Datasets referenced by odata_dataset_dimension.
    for x in ["equ1014", "educ0024", "educ0023", "care0138", "schs0263", "schs0265", "schs0268", "schs0270", "tran0305"]:
        warn("load_dataset_collections: Loading missing dataset %s for en-gb.\n" % x)
        load_collection(x, None, "en-gb")
        warn("load_dataset_collections: Loading missing dataset %s for cy-gb.\n" % x)
        load_collection(x, None, "cy-gb")

    # Datasets referenced by odata_catalogue.
    for x in ["educ0192", "educ0196", "hlth0458", "hlth0459"]:
        warn("load_dataset_collections: Loading missing dataset %s for en-gb.\n" % x)
        load_collection(x, None, "en-gb")
        warn("load_dataset_collections: Loading missing dataset %s for cy-gb.\n" % x)
        load_collection(x, None, "cy-gb")

    c.execute("RELEASE load_dataset_collections")


# Loads the dataset_property table.
# Depends on dataset_collection and dataset_collection_info being loaded.
# Succeeds or Throws.
def load_dataset_properties():

    warn("load_dataset_properties()\n")

    c = db.cursor()
    c.execute("SAVEPOINT load_dataset_properties");

    def load_from(local, lang):

        filename = from_ugc(local["filename"])

        content_type = parse_content_type(local["content-type"])
        mime_type    = content_type[0]
        options      = content_type[1]

        if (mime_type != "application/xml"):
            raise AssertionError("load_dataset_properties: Expected mime-type of application/xml but %s has %s!" % (filename, content_type))

        tree          = xml.etree.ElementTree.parse(filename)
        data_services = tree.findall("./{http://schemas.microsoft.com/ado/2007/06/edmx}DataServices")

        if (len(data_services) != 1):
            raise AssertionError("load_dataset_properties: Expected a single data service but %d has %d!" % (filename, len(data_services)))

        data_service = data_services[0]
        schemas      = data_service.findall("./{http://schemas.microsoft.com/ado/2009/11/edm}Schema")

        if (len(schemas) != 1):
            raise AssertionError("load_dataset_properties: Expected a single schema but %d has %d!" % (filename, len(schemas)))

        schema = schemas[0]
        title  = schema.attrib["Namespace"]

        if (title != "Default"):
            raise AssertionError("load_dataset_properties: Expected a schema with title 'Default' but %s has %s" % (filename, title))

        entity_containers = schema.findall("./{http://schemas.microsoft.com/ado/2009/11/edm}EntityContainer")

        if (len(entity_containers) != 1):
            raise AssertionError("load_dataset_properties: Expected a single entity container but %d has %d!" % (filename, len(entity_containers)))

        entity_sets = entity_containers[0].findall("./{http://schemas.microsoft.com/ado/2009/11/edm}EntitySet")

        for e in entity_sets:
            dataset = e.attrib["Name"]
            type    = e.attrib["EntityType"].split(".")

            if (type[0] != "Default"):
                raise AssertionError("load_dataset_properties: Type %s is not in the default schema!" % e.attrib["EntityType"])

            if (len(type) != 2):
                raise AssertionError("load_dataset_properties: Type %s cannot be parsed!" % e.attrib["EntityType"])

            type = type[1]

            entity_types = schema.findall("./{http://schemas.microsoft.com/ado/2009/11/edm}EntityType[@Name='%s']" % type)

            if (len(entity_types) != 1):
                raise AssertionError("load_dataset_properties: Expected 1 definition for type %s but found %d: %s!" % (type, len(entity_types), entity_types))

            properties = entity_types[0].findall("./{http://schemas.microsoft.com/ado/2009/11/edm}Property")

            key_to_column_prefix = {
                    "Data"        : "measure",
                    "Percentage"  : "measure",  # hlth0602
                    "RowKey"      : "row_key",
                    "PartitionKey": "partition_key",
                    "Code"        : "item",
                    "ItemName"    : "item_name",
                    "SortOrder"   : "sort_order",
                    "Hierarchy"   : "hierarchy",
                    "ItemNotes"   : "item_notes",
                    }

            recursive_dict = lambda: collections.defaultdict(recursive_dict)

            measure      = collections.defaultdict(dict)            # Dictionary of column_prefixes to signatures.
            dimensions   = collections.defaultdict(dict)            # Dictionary of dimension names to column_prefixes to signatures.
            alternatives = collections.defaultdict(recursive_dict)  # Dictionary of dimension names to alternative_indexes to signatures.

            # Group the properties into type signatures for Measure or Dimension data.
            for p in properties:

                type      = p.attrib["Type"]
                nullable  = p.attrib.get("Nullable", "true")
                name      = p.attrib["Name"].split("_")

                # If name has one element then it's data about the Measure itself.
                # If name has more than one element (2 or 3) then it's data about a Dimension.
                if (len(name) == 1):

                    # Put it in dataset_property_measure.
                    measure_property                     = name[0]
                    column_prefix                        = key_to_column_prefix[measure_property]
                    measure[column_prefix + "_type"]     = type
                    measure[column_prefix + "_nullable"] = nullable

                else:

                    # Put it in dataset_property_dimension or dataset_property_dimension_alternative.

                    if ((len(name) != 2) and (len(name) != 3)):
                        raise AssertionError("load_dataset_properties: Expected property name with 2 or 3 components but %s has %d!" % (p.attrib["Name"], len(name)))

                    dimension_name     = name[0]
                    dimension_property = name[1]

                    if (dimension_property[:-1] == "AltCode"):
                        alternative_index                                                       = int(dimension_property[-1:])
                        alternatives[dimension_name][alternative_index]["alternative_type"]     = type
                        alternatives[dimension_name][alternative_index]["alternative_nullable"] = nullable
                    else:
                        column_prefix                                           = key_to_column_prefix[dimension_property]
                        dimensions[dimension_name][column_prefix + "_type"]     = type
                        dimensions[dimension_name][column_prefix + "_nullable"] = nullable

                if (len(name) == 3):
                    if ((name[2] != "ENG") and (name[2] != "WEL")):
                        raise AssertionError("load_dataset_properties: Expected property name to have suffix _ENG or _WEL but we got %s!" % p.attrib["Name"])


            # INSERT the data into the database.

            QUERY = None
            if (lang == "en-gb"):
                QUERY = INSERT
            else:
                if (dataset in ["hous0701", "hous0702", "schs0255"]):
                    warn("load_dataset_properties: Not checking dataset %s for language %s because it's known to mismatch en-gb.\n" % (dataset, lang))
                    #
                    # These datasets don't match in the English and Welsh
                    # versions of the property metadata.
                    #
                    # hous0701: Has Area_ItemNotes_WEL but no Area_ItemNotes_ENG.
                    # hous0702: Has Area_ItemNotes_WEL but no Area_ItemNotes_ENG.
                    # schs0255: English has Name="YearGroup_ItemName_ENG", Type="Edm.String".
                    #           Welsh   has Name="YearGroup_ItemName_WEL", Type="Edm.Int64", Nullable="false".
                    #
                    QUERY = IGNORE_ROW
                else:
                    QUERY = CHECK_ROW

            the_map  = [("dataset", identity, dataset)]
            the_map += [(k, identity, measure[k]) for k in sorted(measure)]

            QUERY("dataset_property_measure", the_map)(c, {})


            for dimension, d in dimensions.items():

                the_map  = [
                        ("dataset",   identity, dataset),
                        ("dimension", identity, dimension)
                        ]
                the_map += [(k, identity, d[k]) for k in sorted(d)]

                QUERY("dataset_property_dimension", the_map)(c, {})


            for dimension, alternatives in alternatives.items():
                for index, a in alternatives.items():

                    the_map  = [
                            ("dataset",           identity, dataset),
                            ("dimension",         identity, dimension),
                            ("alternative_index", identity, index)
                            ]
                    the_map += [(k, identity, a[k]) for k in sorted(a)]

                    QUERY("dataset_property_dimension_alternative", the_map)(c, {})


    # Load the English and check the Welsh
    load_from(fetch_uri(ebu, ("dataset", "$metadata")), "en-gb")
    warn("load_dataset_properties: Loading missing dataset properties for en-gb.\n")
    load_from({"filename": "extra.dataset_property_dimension.xml", "content-type": "application/xml"}, "en-gb")

    load_from(fetch_uri(ebu, ("dataset", "$metadata")), "cy-gb")
    warn("load_dataset_properties: Loading missing dataset properties for cy-gb.\n")
    load_from({"filename": "extra.dataset_property_dimension.xml", "content-type": "application/xml"}, "cy-gb")

    c.execute("RELEASE load_dataset_properties")


# Loads the odata_catalogue and odata_catalogue_info tables.
# Depends on dataset_collection and dataset_collection_info being loaded.
# Succeeds or Throws.
def load_odata_catalogue():

    warn("load_odata_catalogue()\n")

    c = db.cursor()
    c.execute("SAVEPOINT load_odata_catalogue");

    def load_from(local_file, lang, insert, check):

        procs = []

        # Declare which tables we want to load and how we want to load them.

        odata_catalogue_map = [
                ("dataset",       lookup(), "Dataset"),
                ("partition_key", lookup(), "PartitionKey"),
                ("row_key",       lookup(), "RowKey"),
                ("folder_path",   lookup(), "FolderPath"),
                ]

        procs += make_procs("odata_catalogue", odata_catalogue_map, insert, check)

        odata_catalogue_info_map = [
                ("dataset",        lookup(),     "Dataset"),
                ("partition_key",  lookup(),     "PartitionKey"),
                ("row_key",        lookup(),     "RowKey"),
                ("lang",           identity,     lang),
                ("dataset_uri",    lookup(lang), {"en-gb": "DatasetURI_ENG",    "cy-gb": "DatasetURI_WEL"}),
                ("hierarchy_path", lookup(lang), {"en-gb": "HierarchyPath_ENG", "cy-gb": "HierarchyPath_WEL"}),
                ("view_name",      lookup(lang), {"en-gb": "ViewName_ENG",      "cy-gb": "ViewName_WEL"}),
                ]

        procs += make_procs("odata_catalogue_info", odata_catalogue_info_map, insert, check)

        # Load the tables from each page of OData JSON.
        load_json_pages(local_file, c, procs)


    # For one language INSERT the rows into both tables and for every other
    # language check that the data matches in the odata_catalogue table and
    # insert the language specific data into the odata_catalogue_info table.
    load_from(fetch_uri(ebu, ("discover", "catalogue")), "en-gb", ["odata_catalogue", "odata_catalogue_info"], [])
    load_from(fetch_uri(wbu, ("discover", "catalogue")), "cy-gb", ["odata_catalogue_info"], ["odata_catalogue"])

    c.execute("RELEASE load_odata_catalogue")


# Loads the odata_metadata_tag table.
# Depends on dataset_collection_info being loaded.
# Succeeds or Throws.
def load_odata_metadata_tags():

    warn("load_odata_metadata_tags()\n")

    c = db.cursor()
    c.execute("SAVEPOINT load_odata_metadata_tags");

    def load_from(local_file, lang, insert, check):

        procs = []

        # Declare which tables we want to load and how we want to load them.

        odata_metadata_tag_map = [
                ("dataset",       lookup(),     "Dataset"),
                ("partition_key", lookup(),     "PartitionKey"),
                ("row_key",       lookup(),     "RowKey"),
                ("lang",          identity,     lang),
                ("tag_type",      lookup(lang), {"en-gb": "TagType_ENG",     "cy-gb": "TagType_WEL"}),
                ("tag",           lookup(lang), {"en-gb": "Tag_ENG",         "cy-gb": "Tag_WEL"}),
                ("description",   lookup(lang), {"en-gb": "Description_ENG", "cy-gb": "Description_WEL"}),
                ("timestamp",     lookup(),     "Timestamp"),
                ("etag",          lookup(),     "ETag"),
                ]

        procs += make_procs("odata_metadata_tag", odata_metadata_tag_map, insert, check)

        # Load the tables from each page of OData JSON.
        load_json_pages(local_file, c, procs)


    # INSERT the data into the table for both languages.
    load_from(fetch_uri(ebu, ("discover", "metadata")), "en-gb", ["odata_metadata_tag"], [])
    load_from(fetch_uri(wbu, ("discover", "metadata")), "cy-gb", ["odata_metadata_tag"], [])

    c.execute("RELEASE load_odata_metadata_tags")


# Loads the odata_dimension_type and odata_dimension_type_info tables.
# Does not depend on any other tables being loaded.
# Succeeds or Throws.
def load_odata_dimension_types():

    warn("load_odata_dimension_types()\n")

    c = db.cursor()
    c.execute("SAVEPOINT load_odata_dimension_types");

    def load_from(local_file, lang, insert, check):

        procs = []

        # Declare which tables we want to load and how we want to load them.

        odata_dimension_type_map = [
                ("semantic_key", lookup(), "SemanticKey"),
                ("type",         lookup(), "Type"),
                ("subtype",      lookup(), "SubType"),
                ]

        procs += make_procs("odata_dimension_type", odata_dimension_type_map, insert, check)

        odata_dimension_type_info_map = [
                ("semantic_key",        lookup(),     "SemanticKey"),
                ("lang",                identity,     lang),
                ("type_description",    lookup(lang), {"en-gb": "TypeDesc_ENG",    "cy-gb": "TypeDesc_WEL"}),
                ("subtype_description", lookup(lang), {"en-gb": "SubTypeDesc_ENG", "cy-gb": "SubTypeDesc_WEL"}),
                ("external_uri",        lookup(lang), {"en-gb": "ExternalURI_ENG", "cy-gb": "ExternalURI_WEL"}),
                ]

        procs += make_procs("odata_dimension_type_info", odata_dimension_type_info_map, insert, check)

        # Load the tables from each page of OData JSON.
        load_json_pages(local_file, c, procs)


    # For one language INSERT the rows into both tables and for every other
    # language check that the data matches in the odata_dimension_type table
    # and insert the language specific data into the odata_dimension_type_info
    # table.
    load_from(fetch_uri(ebu, ("discover", "dimensiontypes")), "en-gb", ["odata_dimension_type", "odata_dimension_type_info"], [])
    load_from(fetch_uri(wbu, ("discover", "dimensiontypes")), "cy-gb", ["odata_dimension_type_info"], ["odata_dimension_type"])

    c.execute("RELEASE load_odata_dimension_types")


# Loads the odata_dimension_item, odata_dimension_item_info and
# odata_dimension_item_alternative tables.
# Depends on odata_dimension_type being loaded.
# Succeeds or Throws.
def load_odata_dimension_items():

    warn("load_odata_dimension_items()\n")

    c = db.cursor()
    c.execute("SAVEPOINT load_odata_dimension_items");

    def load_from(local_file, lang, insert, check):

        procs = []

        # Declare which tables we want to load and how we want to load them.

        odata_dimension_item_map = [
                ("semantic_key",  lookup(), "SemanticKey"),
                ("item",          lookup(), "Code"),
                ("hierarchy",     lookup(), "Hierarchy"),
                ("partition_key", lookup(), "PartitionKey"),
                ("row_key",       lookup(), "RowKey"),
                ("etag",          lookup(), "ETag"),
                ]

        procs += make_procs("odata_dimension_item", odata_dimension_item_map, insert, check)

        odata_dimension_item_info_map = [
                ("semantic_key", lookup(),     "SemanticKey"),
                ("item",         lookup(),     "Code"),
                ("lang",         identity,     lang),
                ("description",  lookup(lang), {"en-gb": "Description_ENG", "cy-gb": "Description_WEL"}),
                ]

        procs += make_procs("odata_dimension_item_info", odata_dimension_item_info_map, insert, check)

        # Each item can have up to three "alternative code": "AltCode1",
        # "AltCode2" and "AltCode3".
        # We need to INSERT (or CHECK_ROW) a row into
        # odata_dimention_item_alternative for each alternative that is
        # specified (non-"") for each item.
        for n in (1, 2, 3):
            odata_dimension_item_alternative_map = [
                    ("semantic_key",      lookup(),                 "SemanticKey"),
                    ("item",              lookup(),                 "Code"),
                    ("alternative_index", identity,                 n),
                    ("alternative_item",  lookup_require_not_empty, "AltCode%d" % n),
                    ]

            procs += make_procs("odata_dimension_item_alternative", odata_dimension_item_alternative_map, insert, check)

        # Load the tables from each page of OData JSON.
        load_json_pages(local_file, c, procs)


    # For one language INSERT the rows into odata_dimension_item,
    # odata_dimension_item_info and odata_dimension_item_alternative tables.
    # For every other language INSERT the language specific data into the
    # odata_dimension_item_info table and check that the data matches in the
    # odata_dimension_alternative table, but don't bother checking the
    # odata_dimension_item table because (at least for Welsh) the non-PRIMARY
    # KEY fields are not present in the OData response so there's nothing to
    # check that can't be enforced by a FOREIGN KEY constraint.
    load_from(fetch_uri(ebu, ("discover", "dimensionitems")), "en-gb", ["odata_dimension_item", "odata_dimension_item_info", "odata_dimension_item_alternative"], [])
    load_from(fetch_uri(wbu, ("discover", "dimensionitems")), "cy-gb", ["odata_dimension_item_info"], ["odata_dimension_item_alternative"])

    c.execute("RELEASE load_odata_dimension_items")


# Loads the odata_dataset_dimension and odata_dataset_dimension_info tables.
# Depends on dataset_collection, dataset_collection_info and
# dataset_property_dimension being loaded.
# Succeeds or Throws.
def load_odata_dataset_dimensions():

    warn("load_odata_dataset_dimensions()\n")

    c = db.cursor()
    c.execute("SAVEPOINT load_odata_dataset_dimensions");

    autoincrement      = None
    autoincrement_info = None

    def load_from(local_file, lang, insert, check):

        procs = []

        # Declare which tables we want to load and how we want to load them.

        odata_dataset_dimension_map = []

        # Ignore the dimension field for the odata_dataset_dimension table in
        # every language except English because that's the only language where
        # we get enough information to turn it into the correct key string that
        # can be used to decode the .../dataset/<dataset> OData responses. As
        # it's part of the PRIMARY KEY (i.e. the identity of the dimension), we
        # have to rely on the order of the data in the English and Welsh
        # .../discover/datasetdimensions Responses being the same to be able to
        # INSERT the non-English translations in the
        # odata_dataset_dimension_info table for the correct dimension. We
        # synthesise the dimension_index key to keep track of this.
        # We also insert translated versions of the synthesised dimension key
        # into the odata_dataset_dimension_info table because it's needed to
        # verify the dimesion_items for the correct dimension when we load the
        # non-English translations into the odata_dataset_dimension_item*
        # tables later.
        # It would be nice if we could find another OData endpoint that would
        # give us the metadata we need rather than having to infer it in this
        # way.
        if (lang == "en-gb"):
            odata_dataset_dimension_map = [
                    ("dataset",         lookup(),                    "Dataset"),
                    ("dimension",       dimension_name_to_key(lang), {"en-gb": "DimensionName_ENG"}),
                    ("dimension_index", autoincrement,               None),
                    ("semantic_key",    lookup(),                    "SemanticKey"),
                    ]
        else:
            odata_dataset_dimension_map = [
                    ("dataset",         lookup(),      "Dataset"),
                    ("dimension_index", autoincrement, None),
                    ("semantic_key",    lookup(),      "SemanticKey"),
                    ]

        procs += make_procs("odata_dataset_dimension", odata_dataset_dimension_map, insert, check)

        odata_dataset_dimension_info_map = [
                ("dataset",               lookup(),                    "Dataset"),
                ("dimension_index",       autoincrement_info,          None),
                ("lang",                  identity,                    lang),
                ("dimension_localised",   dimension_name_to_key(lang), {"en-gb": "DimensionName_ENG",       "cy-gb": "DimensionName_WEL"}),
                ("dimension_name",        lookup(lang),                {"en-gb": "DimensionName_ENG",       "cy-gb": "DimensionName_WEL"}),
                ("description",           lookup(lang),                {"en-gb": "DatasetDescription_ENG",  "cy-gb": "DatasetDescription_WEL"}),
                ("dataset_uri",           lookup(lang),                {"en-gb": "DatasetURI_ENG",          "cy-gb": "DatasetURI_WEL"}),
                ("dataset_dimension_uri", lookup(lang),                {"en-gb": "DatasetDimensionURI_ENG", "cy-gb": "DatasetDimensionURI_WEL"}),
                ("notes",                 lookup(lang),                {"en-gb": "Notes_ENG",               "cy-gb": "Notes_WEL"}),
                ("external_uri",          lookup(lang),                {"en-gb": "ExternalURI_ENG",         "cy-gb": "ExternalURI_WEL"}),
                ]

        procs += make_procs("odata_dataset_dimension_info", odata_dataset_dimension_info_map, insert, check)

        # Load the tables from each page of OData JSON.
        load_json_pages(local_file, c, procs)


    # For English (it must be English because of how the dimension names are
    # used as keys), INSERT the rows into both tables and for every other
    # language check that the data matches in the odata_dataset_dimension table
    # and insert the language specific data into the
    # odata_dataset_dimension_info table.
    # This data does not contain a proper key for the identity of the
    # dimension: we just get the user displayable DimensionName_ENG and
    # DimensionNameWEL fields so we have to rely on the apparent fact the
    # dimensions appear in the same order for both the English and Welsh
    # versions of the service.

    # Load English
    autoincrement               = make_autoincrement(0)
    autoincrement_info          = make_autoincrement(0)
    load_from(fetch_uri(ebu, ("discover", "datasetdimensions")), "en-gb", ["odata_dataset_dimension", "odata_dataset_dimension_info"], [])

    # Load the missing dataset that have items but are not declared.
    warn("load_odata_dataset_dimensions: Loading missing dataset dimensions for en-gb.\n")
    load_from({"filename": "extra.odata_dataset_dimensions.json", "content-type": "application/json"}, "en-gb", ["odata_dataset_dimension", "odata_dataset_dimension_info"], [])

    # Load Welsh
    autoincrement               = make_autoincrement(0)
    autoincrement_info          = make_autoincrement(0)
    load_from(fetch_uri(wbu, ("discover", "datasetdimensions")), "cy-gb", ["odata_dataset_dimension_info"], ["odata_dataset_dimension"])

    # Load the missing dataset that have items but are not declared.
    warn("load_odata_dataset_dimensions: Loading missing dataset dimensions for cy-gb.\n")
    load_from({"filename": "extra.odata_dataset_dimensions.json", "content-type": "application/json"}, "cy-gb", ["odata_dataset_dimension_info"], ["odata_dataset_dimension"])


    c.execute("RELEASE load_odata_dataset_dimensions")

# Loads the odata_dataset_dimension_item, odata_dataset_dimension_item_info and
# odata_dataset_dimension_item_alternative tables.
# Depends on dataset_collection, dataset_collection_info and
# odata_dataset_dimension being loaded.
# Succeeds or Throws.
def load_odata_dataset_dimension_items():

    warn("load_odata_dataset_dimension_items()\n")

    c = db.cursor()
    c.execute("SAVEPOINT load_odata_dataset_dimension_items");

    def load_from(local_file, lang, insert, check):

        procs = []

        # Declare which tables we want to load and how we want to load them.

        odata_dataset_dimension_item_map = []
        autoincrement                    = make_autoincrement(0)
        autoincrement_info               = make_autoincrement(0)

        # Ignore the dimension field for the odata_dataset_dimension_item table in
        # every language except English because that's the only language where
        # we get enough information to turn it into the correct key string that
        # can be used to decode the .../dataset/<dataset> OData responses. As
        # it's part of the PRIMARY KEY (i.e. the identity of the dimension item), we
        # have to rely on the order of the data in the English and Welsh
        # .../discover/datasetdimensionitems Responses being the same to be able to
        # INSERT the non-English translations in the
        # odata_dataset_dimension_item_info table for the correct item / code. We
        # synthesise the item_index key to keep track of this.
        # We also insert translated versions of the synthesised dimension key
        # into the odata_dataset_dimension_item_info table so that items can be
        # verified and indexed.
        # It would be nice if we could find another OData endpoint that would
        # give us the metadata we need rather than having to infer it in this
        # way.
        if (lang == "en-gb"):
            odata_dataset_dimension_item_map = [
                    ("dataset",         lookup(),                    "Dataset"),
                    ("dimension",       dimension_name_to_key(lang), {"en-gb": "DimensionName_ENG"}),
                    ("item",            lookup(),                    "Code"),
                    ("item_index",      autoincrement,               None),
                    ("hierarchy",       lookup(),                    "Hierarchy"),
                    ("sort_order",      lookup(),                    "SortOrder"),
                    ("semantic_key",    lookup(),                    "SemanticKey"),
                    ]
        else:
            odata_dataset_dimension_item_map = [
                    ("dataset",         lookup(),                    "Dataset"),
                    ("item",            lookup(),                    "Code"),
                    ("item_index",      autoincrement,               None),
                    ("hierarchy",       lookup(),                    "Hierarchy"),
                    ("sort_order",      lookup(),                    "SortOrder"),
                    ("semantic_key",    lookup(),                    "SemanticKey"),
                    ]

        procs += make_procs("odata_dataset_dimension_item", odata_dataset_dimension_item_map, insert, check)

        odata_dataset_dimension_item_info_map = [
                ("dataset",             lookup(),                    "Dataset"),
                ("item_index",          autoincrement_info,          None),
                ("lang",                identity,                    lang),
                ("dimension_localised", dimension_name_to_key(lang), {"en-gb": "DimensionName_ENG", "cy-gb": "DimensionName_WEL"}),
                ("description",         lookup(lang),                {"en-gb": "Description_ENG",   "cy-gb": "Description_WEL"}),
                ("notes",               lookup(lang),                {"en-gb": "Notes_ENG",         "cy-gb": "Notes_WEL"}),
                ]

        procs += make_procs("odata_dataset_dimension_item_info", odata_dataset_dimension_item_info_map, insert, check)

        # Each item can have up to three "alternative code": "AltCode1",
        # "AltCode2" and "AltCode3".
        # We need to INSERT (or CHECK_ROW) a row into
        # odata_dataset_dimention_item_alternative for each alternative that is
        # specified (non-"") for each item.
        for n in (1, 2, 3):

            autoincrement_alternative = make_autoincrement(0)

            odata_dataset_dimension_item_alternative_map = [
                    # The autoincrement counter will increment for every
                    # candidate row even if lookup_require_not_empty decides to
                    # skip the row because INSERT and CHECK_ROW guarantee it to
                    # be so.
                    ("dataset",             lookup(),                  "Dataset"),
                    ("item_index",          autoincrement_alternative, None),
                    ("alternative_index",   identity,                  n),
                    ("alternative_item",    lookup_require_not_empty,  "AltCode%d" % n),
                    ]

            procs += make_procs("odata_dataset_dimension_item_alternative", odata_dataset_dimension_item_alternative_map, insert, check)

        # Load the tables from each page of OData JSON.
        load_json_pages(local_file, c, procs)


    # For English (it must be English because of how the English dimension
    # names are used as keys), INSERT the rows into the
    # odata_dataset_dimension_item, odata_dataset_dimension_item_info and
    # odata_dataset_dimension_item_alternative tables. For every other language
    # INSERT the language specific data into the
    # odata_dataset_dimension_item_info table and check that the data matches
    # in the odata_dataset_dimension_item and odata_dataset_dimension_item
    # alternative tables.
    # This data does not contain a proper key for the identity of the
    # dimension: we just get the user displayable DimensionName_ENG and
    # DimensionNameWEL fields so we have to rely on the apparent fact the
    # dimension items appear in the same order for both the English and Welsh
    # versions of the service.
    load_from(fetch_uri(ebu, ("discover", "datasetdimensionitems")), "en-gb", ["odata_dataset_dimension_item", "odata_dataset_dimension_item_info", "odata_dataset_dimension_item_alternative"], [])
    load_from(fetch_uri(wbu, ("discover", "datasetdimensionitems")), "cy-gb", ["odata_dataset_dimension_item_info"], ["odata_dataset_dimension_item", "odata_dataset_dimension_item_alternative"])

    c.execute("RELEASE load_odata_dataset_dimension_items")


def load_all():
    load_dataset_collections()
    load_dataset_properties()
    load_odata_catalogue()
    load_odata_metadata_tags()
    load_odata_dimension_types()
    load_odata_dimension_items()
    load_odata_dataset_dimensions()
    load_odata_dataset_dimension_items()



################################################################################
### Main Program.

def initialise():

    global db
    global http

    # Initialise the database.

    db = sqlite3.connect("statswales2.hypercube.sqlite")

    # Autocommit is sqlite3.LEGACY_TRANSACTION_CONTROL until at least v3.12 and
    # the attribute is not available at all in v3.7
    # We want to:
    #   + Do our own transaction control
    #   + Have consistent behaviour across various runtime environments
    #   + Use nested transactions
    # So we force everything to the legacy behaviour as that's the only one
    # that is always available and makes it easy to do nested transactions.
    if hasattr(db, "autocommit"):
        db.autocommit      = sqlite3.LEGACY_TRANSACTION_CONTROL
        db.isolation_level = None
    else:
        db.isolation_level = None

    db.execute("PRAGMA foreign_keys = ON;")

    upgrade_database()


    # Initialise the filesystem.

    # "User Generated Content" - i.e. stuff we've downloaded.
    pathlib.Path("ugc/").mkdir(exist_ok=True)

    # Temporary files
    pathlib.Path("tmp/").mkdir(exist_ok=True)


    # Initialise the HTTP Session.
    http = requests.Session()


def main(argv):
    initialise()
    load_all()


if __name__ == "__main__":
    main(sys.argv)

################################################################################

