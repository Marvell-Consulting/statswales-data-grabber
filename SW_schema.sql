CREATE TABLE `spider_uri_cache` (
`uri`   TEXT NOT NULL,
`timestamp`     TEXT NOT NULL,
`status`        INTEGER NOT NULL,
`content-type`  TEXT,
`content-length`        INTEGER,
`cache-control` TEXT,
`pragma`        TEXT,
`expires`       TEXT,
`date`  TEXT,
`filename`      TEXT NOT NULL,
PRIMARY KEY(`uri`,`timestamp`)
) WITHOUT ROWID;
CREATE TABLE `db_meta` (
`key`   TEXT NOT NULL,
`value`,
PRIMARY KEY(`key`)
) WITHOUT ROWID;
CREATE TABLE `odata_metadata_tag` (
`dataset`       TEXT NOT NULL,
`partition_key` TEXT NOT NULL,
`row_key`       TEXT NOT NULL,
`lang`  TEXT NOT NULL NOT NULL,
`tag_type`      TEXT NOT NULL,
`tag`   TEXT NOT NULL,
`description`   TEXT NOT NULL,
`timestamp`     TEXT,
`etag`  TEXT,
PRIMARY KEY(`dataset`,`partition_key`,`row_key`,`lang`),
FOREIGN KEY(`dataset`,`lang`) REFERENCES `dataset_collection_info`(`dataset`,`lang`)
) WITHOUT ROWID;
CREATE VIEW `odata_metadata_tag_duplicates`
AS
SELECT
        `dataset`,
        `lang`,
        `tag_type`,
        `tag`,
        COUNT(*) AS `count`
FROM `odata_metadata_tag`
GROUP BY
        `dataset`,
        `lang`,
        `tag_type`,
        `tag`
HAVING `count` > 1
ORDER BY `count` DESC
/* odata_metadata_tag_duplicates(dataset,lang,tag_type,tag,count) */;
CREATE TABLE `odata_dimension_type` (
`semantic_key`  TEXT NOT NULL,
`type`  TEXT NOT NULL,
`subtype`       TEXT NOT NULL,
PRIMARY KEY(`semantic_key`)
) WITHOUT ROWID;
CREATE TABLE `odata_dimension_type_info` (
`semantic_key`  TEXT NOT NULL,
`lang`  TEXT NOT NULL,
`type_description`      TEXT,
`subtype_description`   TEXT,
`external_uri`  TEXT,
PRIMARY KEY(`semantic_key`,`lang`),
FOREIGN KEY(`semantic_key`) REFERENCES `odata_dimension_type`(`semantic_key`)
) WITHOUT ROWID;
CREATE TABLE `odata_dimension_item` (
`semantic_key`  TEXT NOT NULL,
`item`  TEXT NOT NULL,
`hierarchy`     TEXT,
`partition_key` TEXT,
`row_key`       TEXT,
`etag`  TEXT,
PRIMARY KEY(`semantic_key`,`item`),
FOREIGN KEY(`semantic_key`) REFERENCES `odata_dimension_type`(`semantic_key`)
) WITHOUT ROWID;
CREATE TABLE `odata_dimension_item_info` (
`semantic_key`  TEXT NOT NULL,
`item`  TEXT NOT NULL,
`lang`  TEXT NOT NULL,
`description`   TEXT NOT NULL,
PRIMARY KEY(`semantic_key`,`item`,`lang`),
FOREIGN KEY(`semantic_key`,`item`) REFERENCES `odata_dimension_item`(`semantic_key`,`item`)
) WITHOUT ROWID;
CREATE TABLE `odata_dimension_item_alternative` (
`semantic_key` TEXT NOT NULL,
`item` TEXT NOT NULL,
`alternative_index`     INTEGER NOT NULL,
`alternative_item`      TEXT NOT NULL,
PRIMARY KEY(`semantic_key`,`item`,`alternative_index`),
FOREIGN KEY(`semantic_key`,`item`) REFERENCES `odata_dimension_item`(`semantic_key`,`item`)
) WITHOUT ROWID;
CREATE TABLE `odata_dataset_dimension` (
`dataset`       TEXT NOT NULL,
`dimension`     TEXT NOT NULL,
`dimension_index`       INTEGER NOT NULL UNIQUE,
`semantic_key`  TEXT,
PRIMARY KEY(`dataset`,`dimension`),
FOREIGN KEY(`dataset`) REFERENCES `dataset_collection`(`dataset`),
FOREIGN KEY(`dataset`,`dimension`) REFERENCES `dataset_property_dimension`(`dataset`,`dimension`)
) WITHOUT ROWID;
CREATE UNIQUE INDEX `odata_dataset_dimension_fk_dataset_dimension_index` ON `odata_dataset_dimension` (
`dataset`,
`dimension_index`
);
CREATE TABLE `odata_dataset_dimension_info` (
`dataset`       TEXT NOT NULL,
`dimension_index`       INTEGER NOT NULL,
`lang`  TEXT NOT NULL,
`dimension_localised`   TEXT,
`dimension_name`        TEXT,
`description`   TEXT,
`dataset_uri`   TEXT,
`dataset_dimension_uri` TEXT,
`notes` TEXT,
`external_uri`  TEXT,
PRIMARY KEY(`dataset`,`dimension_index`,`lang`),
FOREIGN KEY(`dataset`,`dimension_index`) REFERENCES `odata_dataset_dimension`(`dataset`,`dimension_index`),
FOREIGN KEY(`dataset`,`lang`) REFERENCES `dataset_collection_info`(`dataset`,`lang`)
) WITHOUT ROWID;
CREATE TABLE `odata_dataset_dimension_item` (
`dataset`       TEXT NOT NULL,
`dimension`     TEXT NOT NULL,
`item`  TEXT NOT NULL,
`item_index`    INTEGER NOT NULL UNIQUE,
`hierarchy`     TEXT,
`sort_order`    INTEGER,
`semantic_key`  TEXT,
PRIMARY KEY(`dataset`,`dimension`,`item`,`item_index`),
FOREIGN KEY(`dataset`,`dimension`) REFERENCES `odata_dataset_dimension`(`dataset`,`dimension`)
) WITHOUT ROWID;
CREATE UNIQUE INDEX `odata_dataset_dimension_item_fk_dataset_item_index` ON `odata_dataset_dimension_item` (
`dataset`,
`item_index`
);
CREATE TABLE `odata_dataset_dimension_item_info` (
`dataset`       TEXT NOT NULL,
`item_index`    INTEGER NOT NULL,
`lang`  TEXT NOT NULL,
`dimension_localised`   TEXT NOT NULL,
`description`   TEXT,
`notes` TEXT,
PRIMARY KEY(`dataset`,`item_index`,`lang`),
FOREIGN KEY(`dataset`,`item_index`) REFERENCES `odata_dataset_dimension_item`(`dataset`,`item_index`),
FOREIGN KEY(`dataset`,`lang`) REFERENCES `dataset_collection_info`(`dataset`,`lang`)
) WITHOUT ROWID;
CREATE TABLE `odata_dataset_dimension_item_alternative` (
`dataset`       TEXT NOT NULL,
`item_index`    INTEGER NOT NULL,
`alternative_index`     INTEGER NOT NULL,
`alternative_item`      TEXT NOT NULL,
PRIMARY KEY(`dataset`,`item_index`,`alternative_index`),
FOREIGN KEY(`dataset`,`item_index`) REFERENCES `odata_dataset_dimension_item`(`dataset`,`item_index`)
) WITHOUT ROWID;
CREATE TABLE `odata_catalogue` (
`dataset` TEXT NOT NULL,
`partition_key` TEXT NOT NULL,
`row_key` TEXT NOT NULL,
`folder_path` TEXT NOT NULL,
PRIMARY KEY(`dataset`,`partition_key`,`row_key`),
FOREIGN KEY(`dataset`) REFERENCES `dataset_collection`(`dataset`)
) WITHOUT ROWID;
CREATE TABLE `odata_catalogue_info` (
`dataset`       TEXT NOT NULL,
`partition_key` TEXT NOT NULL,
`row_key`       TEXT NOT NULL,
`lang`  TEXT NOT NULL,
`dataset_uri`   TEXT NOT NULL,
`hierarchy_path`        TEXT NOT NULL,
`view_name`     TEXT NOT NULL,
PRIMARY KEY(`dataset`,`partition_key`,`row_key`,`lang`),
FOREIGN KEY(`dataset`,`partition_key`,`row_key`) REFERENCES `odata_catalogue`(`dataset`,`partition_key`,`row_key`),
FOREIGN KEY(`dataset`,`lang`) REFERENCES `dataset_collection_info`(`dataset`,`lang`)
) WITHOUT ROWID;
CREATE TABLE `dataset_collection` (
`dataset`       TEXT NOT NULL,
`href`  TEXT,
PRIMARY KEY(`dataset`)
) WITHOUT ROWID;
CREATE TABLE `dataset_collection_info` (
`dataset`       TEXT NOT NULL,
`lang`  TEXT NOT NULL,
PRIMARY KEY(`dataset`,`lang`),
FOREIGN KEY(`dataset`) REFERENCES `dataset_collection`(`dataset`)
) WITHOUT ROWID;
CREATE TABLE `dataset_property_measure` (
`dataset`       TEXT NOT NULL,
`measure_type`  TEXT NOT NULL,
`measure_nullable`      INTEGER NOT NULL,
`row_key_type`  TEXT NOT NULL,
`row_key_nullable`      INTEGER NOT NULL,
`partition_key_type`    TEXT NOT NULL,
`partition_key_nullable`        INTEGER NOT NULL,
PRIMARY KEY(`dataset`),
FOREIGN KEY(`dataset`) REFERENCES `dataset_collection`(`dataset`)
) WITHOUT ROWID;
CREATE TABLE `dataset_property_dimension` (
`dataset`       TEXT NOT NULL,
`dimension`     TEXT NOT NULL,
`item_type`     TEXT NOT NULL,
`item_nullable` INTEGER NOT NULL,
`item_name_type`        TEXT NOT NULL,
`item_name_nullable`    INTEGER NOT NULL,
`sort_order_type`       TEXT,
`sort_order_nullable`   INTEGER,
`hierarchy_type`        TEXT,
`hierarchy_nullable`    INTEGER,
`item_notes_type`       TEXT,
`item_notes_nullable`   INTEGER,
PRIMARY KEY(`dataset`,`dimension`),
FOREIGN KEY(`dataset`) REFERENCES `dataset_collection`(`dataset`)
) WITHOUT ROWID;
CREATE TABLE `dataset_property_dimension_alternative` (
`dataset`       TEXT NOT NULL,
`dimension`     TEXT NOT NULL,
`alternative_index`     INTEGER NOT NULL,
`alternative_type`      TEXT NOT NULL,
`alternative_nullable`  INTEGER NOT NULL,
PRIMARY KEY(`dataset`,`dimension`,`alternative_index`),
FOREIGN KEY(`dataset`,`dimension`) REFERENCES `dataset_property_dimension`(`dataset`,`dimension`)
) WITHOUT ROWID;
CREATE VIEW `check_dataset_collection`
(`dataset`, `lang`, `error`)
AS
SELECT
 a.`dataset`,
 a.`lang`,
 'Missing from cy-gb' AS `error`
FROM
        `dataset_collection` AS a
LEFT JOIN
        `dataset_collection` AS b
ON
        a.`dataset` = b.`dataset` AND
        b.lang = 'cy-gb'
WHERE
        a.lang = 'en-gb' AND
        b.dataset IS NULL

UNION ALL

SELECT
 a.`dataset`,
 a.`lang`,
 'Missing from en-gb' AS `error`
FROM
        `dataset_collection` AS a
LEFT JOIN
        `dataset_collection` AS b
ON
        a.`dataset` = b.`dataset` AND
        b.lang = 'en-gb'
WHERE
        a.lang = 'cy-gb' AND
        b.dataset IS NULL;
CREATE VIEW `check_odata_catalogue`
(`dataset`, `partition_key`, `row_key`, `odata_catalogue_info_entries`)
AS
SELECT
        a.`dataset`,
        a.`partition_key`,
        a.`row_key`,
        COUNT(b.`dataset`) as `odata_catalogue_info_entries`
FROM
        `odata_catalogue` AS a
LEFT JOIN
        `odata_catalogue_info` AS b
ON
        a.`dataset`       = b.`dataset` AND
        a.`partition_key` = b.`partition_key` AND
        a.`row_key`       = b.`row_key`
GROUP BY
        a.`dataset`,
        a.`partition_key`,
        a.`row_key`
HAVING `odata_catalogue_info_entries` != 2
/* check_odata_catalogue(dataset,partition_key,row_key,odata_catalogue_info_entries) */;
CREATE VIEW `check_odata_dimension_type_info`
(`semantic_key`, `lang`, `error`)
AS
SELECT
 a.`semantic_key`,
 a.`lang`,
 'Missing from cy-gb' AS `error`
FROM
        `odata_dimension_type_info` AS a
LEFT JOIN
        `odata_dimension_type_info` AS b
ON
        a.`semantic_key` = b.`semantic_key` AND
        b.lang = 'cy-gb'
WHERE
        a.lang = 'en-gb' AND
        b.semantic_key IS NULL

UNION ALL

SELECT
 a.`semantic_key`,
 a.`lang`,
 'Missing from en-gb' AS `error`
FROM
        `odata_dimension_type_info` AS a
LEFT JOIN
        `odata_dimension_type_info` AS b
ON
        a.`semantic_key` = b.`semantic_key` AND
        b.lang = 'en-gb'
WHERE
        a.lang = 'cy-gb' AND
        b.semantic_key IS NULL
/* check_odata_dimension_type_info(semantic_key,lang,error) */;
CREATE VIEW `odata_dimension_type_info_empty_translation`
(`semantic_key`, `lang`, `error`)
AS
SELECT
 a.`semantic_key`,
 a.`lang`,
 'Translation is blank for cy-gb' AS `error`
FROM
        `odata_dimension_type_info` AS a
LEFT JOIN
        `odata_dimension_type_info` AS b
ON
        a.`semantic_key` = b.`semantic_key` AND
        b.`lang` = 'cy-gb'
WHERE
        a.`lang` = 'en-gb' AND
        (b.`semantic_key` IS NULL OR
        b.`type_description` == "" OR
        b.`subtype_description` == "")

UNION ALL

SELECT
 a.`semantic_key`,
 a.`lang`,
 'Translation is blank for en-gb' AS `error`
FROM
        `odata_dimension_type_info` AS a
LEFT JOIN
        `odata_dimension_type_info` AS b
ON
        a.`semantic_key` = b.`semantic_key` AND
        b.`lang` = 'en-gb'
WHERE
        a.`lang` = 'cy-gb' AND
        (b.`semantic_key` IS NULL OR
        b.`type_description` == "" OR
        b.`subtype_description` == "");
CREATE VIEW `check_odata_dimension_item_info`
(`semantic_key`, `item`, `lang`, `error`)
AS
SELECT
 a.`semantic_key`,
 a.`item`,
 a.`lang`,
 'Missing from cy-gb' AS `error`
FROM
        `odata_dimension_item_info` AS a
LEFT JOIN
        `odata_dimension_item_info` AS b
ON
        a.`semantic_key` = b.`semantic_key` AND
        a.`item` = b.`item` AND
        b.lang = 'cy-gb'
WHERE
        a.lang = 'en-gb' AND
        b.semantic_key IS NULL

UNION ALL

SELECT
 a.`semantic_key`,
 a.`item`,
 a.`lang`,
 'Missing from en-gb' AS `error`
FROM
        `odata_dimension_item_info` AS a
LEFT JOIN
        `odata_dimension_item_info` AS b
ON
        a.`semantic_key` = b.`semantic_key` AND
        a.`item` = b.`item` AND
        b.lang = 'en-gb'
WHERE
        a.lang = 'cy-gb' AND
        b.semantic_key IS NULL
/* check_odata_dimension_item_info(semantic_key,item,lang,error) */;
CREATE VIEW `check_odata_dataset_dimension_info`
(`dataset`, `dimension_index`, `lang`, `error`)
AS
SELECT
        a.`dataset`,
        a.`dimension_index`,
        a.`lang`,
        'Missing from cy-gb' AS `error`
FROM
        `odata_dataset_dimension_info` AS a
LEFT JOIN
        `odata_dataset_dimension_info` AS b
ON
        a.`dataset` = b.`dataset` AND
        a.`dimension_index` = b.`dimension_index` AND
        b.`lang` = 'cy-gb'
WHERE
        a.`lang` = 'en-gb' AND
        b.`dataset` IS NULL

UNION ALL

SELECT
        a.`dataset`,
        a.`dimension_index`,
        a.`lang`,
        'Missing from en-gb' AS `error`
FROM
        `odata_dataset_dimension_info` AS a
LEFT JOIN
        `odata_dataset_dimension_info` AS b
ON
        a.`dataset` = b.`dataset` AND
        a.`dimension_index` = b.`dimension_index` AND
        b.`lang` = 'en-gb'
WHERE
        a.`lang` = 'cy-gb' AND
        b.`dataset` IS NULL
/* check_odata_dataset_dimension_info(dataset,dimension_index,lang,error) */;