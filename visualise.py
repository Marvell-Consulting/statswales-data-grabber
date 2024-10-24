#!/usr/bin/python3

################################################################################
#
# visualise.py
#
# Visualise and manipulate the data in the SQLite file populated by spider.py
#
# $ python3 visualise.py
#
#
# Installing Flask
# https://flask.palletsprojects.com/en/3.0.x/installation/
#
# $ pip3 install Flask
# $ pip3 install more_itertools
#
#
# Andy Bennett <andyjpb@register-dynamics.co.uk>, 2024/05/30 14:23.
#
################################################################################


import flask
import queue
import sqlite3
import xml.etree.ElementTree
import time
import collections
import urllib
import re
import itertools
import more_itertools

import widgets
from widgets_html import *
import widgets_govwales as WG

from i18n import _


################################################################################
### Configuration.

db_pool_size = 3
db_file      = "/var/spool/dam/bitbucket/stats-wales/statswales-data-grabber/snapshots/2024-05-01--2024-05-04/statswales2.hypercube.sqlite"


################################################################################
### Program State.

app     = flask.Flask(__name__)
db_pool = None


################################################################################
### Helpers.

# Formats ordinal numbers.
#   format_ordinal(3) -> "3rd"
def format_ordinal(n):
    suffix = "th"
    if (10 < n < 20):
        suffix = "th"
    else:
        m = n % 10
        if (1 <= m <= 3):
            suffix = ["st","nd","rd"][m - 1]
    return "%s%s" % (n, suffix)



################################################################################
### Dataset and cube data processing.

# Adapted from https://gist.github.com/jeremyBanks/1083518/584008c38a363c45acb84e4067b5188bb36e20f4
def sqlite3_quote_identifier(s, errors="strict"):
    encodable = s.encode("utf-8", errors).decode("utf-8")

    nul_index = encodable.find("\x00")

    if nul_index >= 0:
        error = UnicodeEncodeError("utf-8", encodable, nul_index, nul_index + 1, "NUL not allowed")
        error_handler = codecs.lookup_error(errors)
        replacement, _ = error_handler(error)
        encodable = encodable.replace("\x00", replacement)

    return "\"" + encodable.replace("\"", "\"\"") + "\""


# Generates a SELECT query string.
# Quotes all the identifiers supplied in table and columns.
# Any remaining arguments should be generated with WHERE or similar procedures
# as they will be joined with whitespace and included literally.
def SQL_SELECT(table, columns, *args):

    table   = sqlite3_quote_identifier(table)
    columns = [sqlite3_quote_identifier(x) for x in columns]
    columns = ", ".join(columns)

    q = "SELECT %s FROM %s" % (columns, table)
    if args:
        q = "%s %s" % (q, " ".join(args))

    return q


################################################################################
### HTML generation.

element = widgets.element


# Safely redirects to the specified URL by ensuring the path elements are
# properly encoded.
def redirect(*path):

    path = [urllib.parse.quote(x, safe="") for x in path]
    path = "/" + "/".join(path)

    return flask.redirect(path, code = 302)

# Returns the user to the page they came from.
def redisplay():
    return flask.redirect("", code = 302)

## Application level widgets.

def LOGO():
    return element("a", {"id": "logo", "href": "/"},
            IMG("https://struct.register-dynamics.co.uk/logo.svg"))

def MENU(*items):
    return element("nav", {"id": "menu"},
            *[A(href, text) for href, text in items])

# A page template that sorts out the HEAD section and leaves the BODY to the
# caller.
def PAGE(title, *contents):
    return HTML(
            HEAD(
                META("charset", "UTF-8"),
                element("link", {"rel": "stylesheet", "type": "text/css", "href": "/visualise.css"}),
                TITLE(title, ": Datacube Visualiser")),
            BODY(PENULTIMATE_FORM(*contents)))

# A widget that allows a user to pick the dimensions for a table of values from
# the specified dataset.
# TODO:
#   Refactor `r` argument.
#   Return everything in a LOGICAL_FORM.
def DIMENSION_PICKER(dataset, dimensions, r, x, y):
    return [
            P("Pick which dimensions you want on each axis and how you want different dimensions on the same axis to nest."),
            TABLE(
                *[TR(
                    # Allocate columns for the row headers.
                    *[TD() for d in r],
                    # Column headers.
                    TH(SELECT("col/%s" % n,
                        *OPTIONS(dimensions, (x[n] if (n < len(x)) else "")),
                        OPTION("", not (n < len(x)), "(omit)"))),
                    ) for d,n in zip(r, range(0, len(r)))],
                TR(
                    # Row headers.
                    *[TH(SELECT("row/%s" % n,
                        *OPTIONS(dimensions, (y[n] if (n < len(y)) else "")),
                        OPTION("", not (n < len(y)), "(omit)")),
                        ) for d,n in zip(r, range(0, len(r)))],
                    # Data space.
                    TD("<aggregated data>"))),
                HIDDEN("dataset", dataset),
                ACTION("specify_table", "Render Table"),
                ]

def REPORT(*contents):
    return element("div", {"class": "report"}, *contents)

def LARGE_TABLE(*contents):
    return element("table", {"class": "large-table"}, *contents)

## Application page management.

# Renders a complete page based on the PAGE widget and optional caller-supplied
# inserts.
def render_request(Title = None, Menu = None, Main = None, Footer = None):

    if (Title is None):
        Title = "tram rabbit"

    if (Menu is None):
        Menu = MENU(
                ("/", "Datasets"),
                ("/filter-cubes-by-dimension/", "Filters"),
                ("/demo", "Consumer Demo"),
                )

    if (Main is None):
        Main = P("No page content specified!")

    if (Footer is None):
        Footer = []

    perf_counter  = time.perf_counter_ns()
    thread_time   = time.thread_time_ns()

    perf_counter -= flask.g.start_perf_counter
    thread_time  -= flask.g.start_thread_time

    return widgets.elements_to_str(
            PAGE(Title,
                HEADER(
                    LOGO(),
                    Menu),
                Main,
                FOOTER(
                    *Footer,
                    widgets.format_ns(perf_counter), " (wallclock)",
                    BR(),
                    widgets.format_ns(thread_time),  " (sys+user cpu)")))

# Renders a complete page in the style of the Welsh Government based on the
# PAGE widget and optional caller-supplied inserts.
# This is a place to specify application-wide defaults for various parts of the
# page.
def render_wg_request(Lang = None, Home = None, Phase = "Beta", Menu = None, **kwargs):

    if (Lang is None):
        # flask.g is only available when a request is in flight so cannot be a
        # keywork argument default.
        Lang = flask.g.lang

    return WG.render_request(
            Lang  = Lang,
            Home  = Home,
            Phase = Phase,
            Menu  = Menu,
            **kwargs)


################################################################################
### HTTP Views: GET Request routers and dispatchers.

# Index page.
@app.route("/")
def hello() -> str:

    c = flask.g.db.cursor()

    r = c.execute("SELECT `dataset`, count(*) as `count` from `dataset_measure` GROUP BY `dataset`;")

    r = r.fetchall()

    dimensions_histogram = c.execute("""
        SELECT
        dimensions_per_dataset,
        count(dimensions_per_dataset) AS n_datasets
        FROM
        (
        SELECT
        COUNT(distinct dimension) AS dimensions_per_dataset,
        dataset
        FROM dataset_property_dimension
        GROUP BY dataset
        ORDER BY dimensions_per_dataset
        )
        GROUP BY dimensions_per_dataset
        ORDER BY dimensions_per_dataset
    """)

    dimensions_histogram = dimensions_histogram.fetchall()


    return render_request(
            Title = "List of datasets",
            Main  = MAIN(
                H1("Datasets"),
                H2("Overall Statistics"),
                LARGE_TABLE(
                    TR(
                        TH("Number of Datasets"),
                        *[TD(f'{d[1]:,}') for d in dimensions_histogram],
                    ),
                    TR(
                        TH("Dimensions Per Dataset"),
                        *[TD(A("/by-dimension-count/%d" % d[0], f'{d[0]:,}')) for d in dimensions_histogram],
                        )),
                H2("List of datasets"),
                P("Pick a dataset to visualise."),
                UL(
                    *[LI(x[0], " (", f'{x[1]:,}', " facts)",
                        UL(LI(A("/render/%s" % x[0], "Table Visualiser")),
                            LI(A("/fact-table/%s" % x[0], "Fact Table")),
                            )) for x in r],
                        )))

# WG test.
@app.route("/wg")
def wg() -> str:
    return render_wg_request(
            Title = "bus rabbit",
            Home = "/wg",
            Main = WG.MAIN(
                WG.GRID_ROW(
                    WG.GRID_COL("full",
                        WG.H1(_("StatsWales"),
                            SPAN({"class": "float-right"}, element("button", {"class": "secondary blue"}, "Sign in")),
                            caption = _("Find statistics and data from the Welsh Government")),
                        WG.SEARCH_BANNER("Search for Welsh Statistics"),
                        )),
                WG.GRID_ROW(
                    WG.GRID_COL("one-half",
                        WG.ARTICLE("Browse data",
                            "Browse all datasets available on StatsWales",
                            A("browse-list", "Browse all datasets"))),
                    WG.GRID_COL("one-half",
                        WG.ARTICLE("Build your own table",
                            "Select a dataset and build your own table",
                            A("build-step-logic", "Build your own table")))),

                H3("Browse by topic"),
                *[WG.GRID_ROW(
                    *[(WG.TOPIC(title, href, description) if (title != None) else DIV({})) for (title, href, description) in x])
                    for x in itertools.zip_longest(*(iter([
                        ("Agriculture", "", "Information about agricultural land, livestock and farm workers."),
                        ("Business, economy and labour market", "", "Business, economy and labour market statistical data."),
                        ("Census", "", "Statistical data from the Census."),
                        ("Community safety and social inclusion", "", "Including fire and crime, poverty (HBAI) and deprivation (WIMD), and Communities First."),
                        ("Education and skills", "", "Statistics on education, training and skills from pre-school to school, through to Further and Higher Education and Adult and Community learning."),
                        ("Environment and countryside", "", "Topics such as waste, climate change and land."),
                        ("Equality and diversity", "", "Data and analysis of protected charactistics and other associated characteristics."),
                        ("Health and social care", "", "Information on health, health services and social services, including NHS primary and community activity, waiting times and NHS staff."),
                        ("Housing", "", "Statistical information on all aspects of housing in Wales."),
                        ("Local government", "", "Statistical information on the finances and national strategic indicators for local government"),
                        ("National Survey for Wales", "", "Statistics from the National Survey for Wales, covering a range of topics such as wellbeing and people’s views on public services."),
                        ("Population and migration", "", "Covers topics such as estimates and projections of population and estimates of migration."),
                        ("Sustainable development", "", "Measures of everyday concerns including health, housing, jobs, crime, education and our environment."),
                        ("Taxes devolved to Wales", "", "Statistics for collecting and managing the first devolved Welsh taxes."),
                        ("Tourism", "", "Statistics are related to all aspects of tourism."),
                        ("Transport", "", "Statistics on all aspects of transport."),
                        ("Well-being", "", "Data on health and wellbeing for children."),
                        ("Welsh Government", "", "Information relating to Welsh Government."),
                        ("Welsh language", "", "Statistical information on the Welsh language skills of people in Wales, and their use of the language."),
                        ]),) * 3, fillvalue= (None, None, None))],
                WG.GRID_ROW(
                    WG.DATASET_TIMELINE("Published recently", [
                        ("Primary and secondary grassland, woodland and crop fires", "", "14/3/2024"),
                        ("Fatal and non-fatal casualties by quarter", "", "14/3/2024"),
                        ("Deliberate fires by quarter", "", "14/3/2024"),
                        ("Value of exports by quarter and UK region (£m)", "", "13/3/2024"),
                        ("Value of Welsh exports by quarter and product (£m)", "", "13/3/2024"),
                        ]),
                    WG.DATASET_TIMELINE("Most popular", [
                        ("Population estimates by local authority and year", "", "1/12/2023"),
                        ("Ethnicity by area and ethnic group", "", "14/3/2024"),
                        ("Employment rate by Welsh local area, year and gender", "", "13/5/2023"),
                        ("Children looked after at 31 March by local authority, gender and age", "", "19/12/2023"),
                        ("Pupils eligible for free school meals by local authority, region and year", "", "9/3/2023"),
                        ])

                        )

                ))

# Starting page for consumer filter demo.
@app.route("/demo")
def demo() -> str:
    return redirect("demo", "")

# Consumer filter demo for testing with participants on 2024/10/21.
@app.route("/demo/")
def demo_0() -> str:

    lang = flask.g.lang
    c    = flask.g.db.cursor()

    r = c.execute("""SELECT "category", "description", "notes" FROM "lut_category_info" WHERE "lang" = ?""", (lang,))

    return render_wg_request(
            Title = "Consumer Filter Demo",
            Home = "/demo",
            Main = WG.MAIN(
                WG.GRID_ROW(
                    WG.GRID_COL("full",
                        WG.H1(_("StatsWales"),
                            caption = _("Find statistics and data from the Welsh Government")),
                        )),

                H3(_("Filter by category")),
                *[WG.GRID_ROW(
                    *[(WG.TOPIC(title, "./search/" + href, description if (description != "") else (_("Information about") + " " + title)) if (href != None) else DIV({})) for (href, title, description) in x])
                    for x in itertools.zip_longest(*(iter(r),) * 3, fillvalue= (None, None, None))],
                WG.GRID_ROW(
                    WG.DATASET_TIMELINE("Published recently", [
                        ("Primary and secondary grassland, woodland and crop fires", "", "14/3/2024"),
                        ("Fatal and non-fatal casualties by quarter", "", "14/3/2024"),
                        ("Deliberate fires by quarter", "", "14/3/2024"),
                        ("Value of exports by quarter and UK region (£m)", "", "13/3/2024"),
                        ("Value of Welsh exports by quarter and product (£m)", "", "13/3/2024"),
                        ]),
                    WG.DATASET_TIMELINE("Most popular", [
                        ("Population estimates by local authority and year", "", "1/12/2023"),
                        ("Ethnicity by area and ethnic group", "", "14/3/2024"),
                        ("Employment rate by Welsh local area, year and gender", "", "13/5/2023"),
                        ("Children looked after at 31 March by local authority, gender and age", "", "19/12/2023"),
                        ("Pupils eligible for free school meals by local authority, region and year", "", "9/3/2023"),
                        ])

                        )

                ))

# Criteria editor for consumer filter demo.
# Find all the cubes that match the given criteria.
# Criteria is a list of category names and, optionally, keys and possible
# values for those categories.
# criteria - Category(Value,value...);Category:Key(Value,value...);Category;Category:Key;...
# FIXME: Decode values so that [,()] can appear in category names, keys and values.
@app.route("/demo/search/<criteria>")
def demo_search(criteria) -> str:

    lang = flask.g.lang
    c    = flask.g.db.cursor()

    criteria_list = list(filter(None, criteria.split(";")))

    # Regex to match and bind "Category:Key(Value...)".
    p = re.compile(r"^([^(]*)\(([^)]*)\)$")

    filtered_dimensions = []
    filters             = {}

    for c in criteria_list:
        m = p.match(c)
        if m != None:
            # "Dimension(Value...)"
            name          = m.group(1)
            values        = m.group(2).split(",")
            filters[name] = filters.get(name, []) + values
            if name not in filtered_dimensions:
                filtered_dimensions.append(name)
        else:
            # "Dimension"
            name = c
            filters[name] = filters.get(name, []) + []
            if name not in filtered_dimensions:
                filtered_dimensions.append(name)

    # LUT is a string encoded as "<Category>:<Key>".
    def fetch_items_for_lut(lut):

        lut    = lut.split(":")
        length = len(lut)

        c = flask.g.db.cursor()

        q = """
            SELECT
            DISTINCT "ld"."item_id", "li"."description"
            FROM
            "lut_category_key" AS "lk",
            "lut_reference_data" AS "ld",
            "lut_reference_data_info" AS "li"
            WHERE
            "ld"."category_key" = "lk"."category_key"
            AND "li"."item_id" = "ld"."item_id"
			AND "li"."category_key" = "ld"."category_key"
			AND "li"."lang" = ?1
            """

        if (length >= 1):
            q += """
                AND "lk"."category" = ?2
                """

        if (length >= 2):
            q += """
                AND "lk"."category_key" = ?2 || "/" || ?3
                """

        if (length >= 3):
            raise AssertionError("LUT contains too many parts")

        q += """
            ORDER BY "li"."description"
            """

        r = c.execute(q, (lang, *lut))

        return r

    # Cons up the lists of items for all dimensions we have filters on.
    dimension_values = {}
    for d in filtered_dimensions:

        r = fetch_items_for_lut(d)


        # Arrange the results in a form suitable for OPTIONS.
        # A dictionary of keys to localised strings.
        # The keys are a ; separated list of all items with the same localised string.
        # Relies on r being sorted by description.

        items     = {}
        prev_id   = None
        prev_desc = None
        for i in r:

            curr_id   = i[0]
            curr_desc = i[1]

            new_id    = None

            if (prev_desc == curr_desc and True):

                # Group IDs that have the same localised text together so that
                # selecting the localised version returns a list of all items
                # with that ID.

                del items[prev_id]
                new_id        = prev_id + ";" + curr_id
                items[new_id] = prev_desc  ## OPTIONS structure.

                prev_id = new_id
                #print("%s -> %s" % (curr_desc, new_id))

            else:

                # We've not seen an item with this description before.

                new_id        = curr_id
                items[new_id] = curr_desc  # OPTIONS Structure.
                prev_id       = new_id
                prev_desc     = curr_desc


        # Store these items against the category they relate to.
        dimension_values[d] = items


    # Work out what criteria are still possible.
    # This is supposed to check what types of dimensions datasets in the
    # result-set have and limit it to just those but we can't do that
    # efficiently until we have category_key type annotations for each
    # dimension.
    # So, for now, always list everything.
    # This may mean the user can easily construct filters that lead to zero
    # results.

    c = flask.g.db.cursor()
    q = """
        SELECT
        "category",
        "description"
        FROM "lut_category_info"
        WHERE "lang" = ?
        """
    r = c.execute(q, (lang,))

    remaining_dimensions = {}
    for r in r:

        category    = r[0]
        description = r[1]

        remaining_dimensions[category] = description


    # TODO: Decode the category / key / value data and display it in the three columns of each Step.
    # Rearrange the criteria into a tree of Category->Key->Value.
    # When rendering, if there's no key, get the list of candidates from the db
    # when rendering, if there's a key and no value, get the list of candidates from the db.
    # if there's no key then get all values for the whole category


    # Find all the cubes that meet the criteria.

    c = flask.g.db.cursor()

    # Parameters to be bound to the generated query.
    parameters   = []
    parameters.append(lang)

    intersection = []
    for d in filtered_dimensions:

        lut    = d.split(":")
        length = len(lut)

        q = """
            SELECT
            DISTINCT
            oi.dataset
            FROM
            lut_category_key AS lk,
            lut_reference_data AS ld,
            odata_dataset_dimension_item AS oi
            WHERE
            ld.category_key = lk.category_key
            AND oi.item = ld.item_id
            """
        if (length >= 1):
            q += """
                AND "lk"."category" = ?
                """
            parameters.append(lut[0])

        if (length >= 2):
            q += """
                AND "lk"."category_key" = ? || "/" || ?
                """
            parameters.append(lut[0])
            parameters.append(lut[1])

        intersection.append(q)

    intersection = " INTERSECT ".join(intersection)

    q = """
         SELECT
         DISTINCT
        "h"."dataset",
        "h"."topic",
        "t"."description",
        "d"."description"
        FROM
        (SELECT
        "dataset",
        substr(hierarchy_path, 0 , instr(hierarchy_path, "/")) AS "topic"
        FROM "odata_catalogue_info"
        WHERE "lang" = ?
        AND "dataset" in (%s)
        ) AS "h"
        LEFT JOIN "odata_metadata_tag" AS "t"
        ON "h"."dataset" = "t"."dataset"
        LEFT JOIN "odata_metadata_tag" AS "d"
        ON "h"."dataset" = "d"."dataset"
        WHERE
        "t"."lang" = ?
        AND "t"."tag" = ?
        AND "d"."lang" = ?
        AND "d"."tag" = ?
        ORDER BY "topic"
        """ % intersection

    parameters.append(lang)
    parameters.append("Title" if (lang == "en-gb") else "Teitl")
    parameters.append(lang)
    parameters.append("Last update" if (lang == "en-gb") else "Diweddariad nesaf")

    q = c.execute(q, parameters)

    results = {}
    count   = 0
    for r in q:
        count += 1

        dataset = r[0]
        topic   = r[1]
        title   = r[2]
        updated = r[3]

        #uri = "./" + criteria + "/" + dataset
        # Always link to Andy's prototype of WIMD1901.
        uri = "https://statswales-prototype.ashysand-42d8a180.ukwest.azurecontainerapps.io/consumers-v3/dataset"

        l = results.get(topic, list())
        l.append((title, uri, updated))
        results[topic] = l

    topics = sorted(results.keys())


    # Render the filters.

    wg_filters = []
    n          = 0
    for f in filtered_dimensions:
        step     = None
        category = None
        key      = None
        values   = None

        step = SPAN({}, _("STEP"), " ", n + 1)

        lut    = f.split(":")
        length = len(lut)

        # Look up the localised name for the category.

        category = lut[0]
        c = flask.g.db.cursor()
        q = """
            SELECT
            "description"
            FROM "lut_category_info"
            WHERE "lang" = ?
            AND "category" = ?
            """
        r = c.execute(q, (lang, category))
        r = r.fetchall()
        if (len(r) != 1):
            category = SPAN({}, EM("--***-- ", _("Unknown"), " --***--"), " (%s" % (category), ")")
        else:
            category = r[0][0]

        # Look up the localised name for the category_key.

        if (length == 1):
            # No category_key has been specified yet.
            # If values are already selected then say "-Any-", otherwise
            # populate a dropdown.
            n_filters   = len(filters[f])

            if (n_filters == 0):
                # Generate localised dropdown.
                c = flask.g.db.cursor()
                q = """
                    SELECT
                    "k"."category",
                    substr("k"."category_key", instr("k"."category_key", "/") + 1) AS "key",
                    "i"."description"
                    FROM "lut_category_key" AS "k"
                    LEFT JOIN "lut_category_key_info" AS "i"
                    ON "i"."category_key" = "k"."category_key"
                    WHERE "i"."lang" = ?
                    AND "k"."category" = ?
                    """

                r = c.execute(q, (lang, lut[0]))
                r = r.fetchall()

                keys = {}
                for r in r:

                    c = r[0]
                    k = r[1]
                    d = r[2]

                    keys["%s:%s" % (c, k)] = d

                key = SELECT("d",
                        *OPTIONS(keys),
                        OPTION("", True, "(pick something)"))

            else:
                # No dropdown.
                key = "-%s-" % _("Any")

        if (length == 2):
            # A particular category_key has been selected.
            key = lut[1]

            c = flask.g.db.cursor()
            q = """
                SELECT
                "description"
                FROM lut_category_key_info
                WHERE "lang" = ?
                AND "category_key" = ? || "/" || ?
                """
            r = c.execute(q, (lang, lut[0], key))
            r = r.fetchall()
            if (len(r) != 1):
                key = SPAN({}, EM("--***-- ", _("Unknown"), " --***--"), " (%s" % (key), ")")
            else:
                key = r[0][0]


        # This only works when we have a category and a key (e.g. Geog:LA) in
        # category_key because dimension_values for categories can have codes
        # that are grouped together due to duplicates with the same localised
        # name for different keys.
        def code_to_localised_name(dimension_values, category_key, item_id):
            return dimension_values.get(category_key).get(item_id, SPAN({}, EM("--***-- ", _("Unknown"), " --***--"), " (%s:%s" % (category_key, item_id), ")"))

        values =  [LI(
            code_to_localised_name(dimension_values, f, v),
            HIDDEN("v/%s" % f, v),
            SPAN({"class": "float-right"}, A("#", _("Remove")))

            ) for v in filters[f]]
        values.append(LI(SELECT("v/%s" % f,
                    *OPTIONS(dimension_values[f], None),
                    OPTION("", True, ("(nothing else)" if (len(filters[f]) > 0) else "(any value)")),
                    )))
        values = UL(*values)

        wg_filters.append((step, category, key, values))
        n += 1


    # Generate the page.

    return render_wg_request(
            Title = "Filter By Category : Consumer Filter Demo",
            Home = "/demo",
            Main = WG.MAIN(
                WG.GRID_ROW(
                    WG.GRID_COL("two-thirds",
                        WG.H1(_("Search for datasets")),
                        )),

                H3(_("Your Filters")),
                P(_("Choose the topic, dataset, dimensions and time period you want to view. You'll then be able to view the table and download the data.")),
                P(criteria),

                WG.FILTERS(
                    *[WG.FILTER(step, category, key, values) for (step, category, key, values) in wg_filters],
                    # An extra row for adding a new filter.
                    WG.TR(
                        element("td", {"class": "govuk-table__cell", "style": "width: 120px;"},
                            SPAN({"class": "badge"}, _("AND") if (len(filtered_dimensions) > 0) else "STEP 1")),
                        WG.TD("there is information about ",
                            SELECT("d",
                                *OPTIONS(remaining_dimensions),
                                OPTION("", True, "(pick something)")),
                            colspan = 3)
                        )),
                SPAN({"class": "float-right"}, WG.ACTION("dbg", "dbg"), WG.ACTION("demo-update-filters", _("Update Filters"), classes = ["secondary", "blue"])),

                H3(_("Results by topic"), " (", "{:,}".format(count), " ", _("results"), ")"),
                *[WG.GRID_ROW(
                    *[(WG.DATASET_TIMELINE(topic,
                        results[topic]
                        ) if (topic != None) else DIV({})) for topic in t])
                    for t in itertools.zip_longest(*(iter(topics),) * 2, fillvalue= None)],


                ))

# Alternative starting page for finding cubes that match a given criteria
# during consumer demo.
@app.route("/demo/search/")
def demo_search_0() -> str:
    return demo_search("")

# Support files
@app.route("/visualise.css")
def stylesheet() -> str:
    return flask.send_from_directory(".", "visualise.css")

@app.route("/widgets_govwales/<f>")
def widgets_govwales(f) -> str:
    return flask.send_from_directory("./widgets_govwales", f)

# Display lists of datasets by the count of the number of dimensions they have.
@app.route("/by-dimension-count/<n>")
def by_dimension_count(n) -> str:

    n = int(n)

    c = flask.g.db.cursor()

    r = c.execute("""
        SELECT
        COUNT(distinct dimension) AS dimensions_per_dataset,
        dataset
        FROM dataset_property_dimension
        GROUP BY dataset
        HAVING dimensions_per_dataset = ?
        ORDER BY dataset;
        """, (n,))

    r = r.fetchall()

    return render_request(
            Title = "Datasets with %d dimensions" % n,
            Main  = MAIN(
                H1("Datasets with %d dimensions" % n),
                P("Pick a dataset to visualise."),
                UL(
                    *[LI(x[1],
                        UL(LI(A("/render/%s" % x[1], "Table Visualiser")),
                            LI(A("/fact-table/%s" % x[1], "Fact Table")),
                            )) for x in r],
                        )))

# Display cube metadata for table specification.
@app.route("/render/<dataset>")
def show_metadata(dataset) -> str:

    c             = flask.g.db.cursor()
    c.row_factory = sqlite3.Row

    r = c.execute("SELECT DISTINCT `dimension` FROM `dataset_dimension` WHERE `dataset` = ?;", (dataset,))

    r = r.fetchall()

    dimensions = {}
    for d in r:
        dimensions[d["dimension"]] = d["dimension"]


    return render_request(
            Title = "Specify table for %s" % dataset,
            Main  = MAIN(
                H1("Specify table for ", dataset),
                *DIMENSION_PICKER(dataset, dimensions, r, [], []),
                ))


# Render a cube into a table with the axes specified.
@app.route("/render/<dataset>/<x>/<y>")
def render_grid_report(dataset, x, y) -> str:

    c             = flask.g.db.cursor()
    c.row_factory = sqlite3.Row

    r = c.execute("SELECT DISTINCT `dimension` FROM `dataset_dimension` WHERE `dataset` = ? ORDER BY `dimension`;", (dataset,))

    r = r.fetchall()

    dimensions = {}
    for d in r:
        dimensions[d["dimension"]] = d["dimension"]

    x = x.split(",")
    y = y.split(",")

    for v in x:
        if v not in dimensions:
            flask.abort(404)

    for v in y:
        if v not in dimensions:
            flask.abort(404)

    # for each dimension in x:
    # number of rows occupied for hierarchy
    # number of cols occupied by values
    # and vice-versa for each dimension in y.
    #
    # count them first to lay out table headings
    # query them later for cell values.



    return render_request(
            Title = dataset,
            Main  = MAIN(
                H1(dataset),
                *DIMENSION_PICKER(dataset, dimensions, r, x, y),
                REPORT(
                    LARGE_TABLE(
                        # a colgroup that covers the row headers, one for each in the row hierarchy
                        # a colgroup for each of the column dimensions
                        # thead containing trs and ths for the dimension items in the columns
                        # tbody with trs with ths for the dimension items in the rows and tds for the data
                        THEAD(
                            TR(TH(""),          TH("2024/04/01"), TH("2024/04/02"), TH("2024/04/03"))),
                        TBODY(
                            TR(TH("Blaenau"),   TD("55"),         TD("59"),         TD("58")),
                            TR(TH("Bridgend"),  TD("99"),         TD("100"),        TD("101")),
                            TR(TH("Cardiff"),   TD("88"),         TD("70"),         TD("60")),
                            TR(TH("Swansea"),   TD("-"),          TD("-"),          TD("-")),
                            )
                        ))))

# Render a cube into its underlying fact table with one row per fact and one
# column per dimension.
@app.route("/fact-table/<dataset>")
def fact_table(dataset) -> str:

    # Builds a query with
    #  + one binding for each of the dimensions that should be bound to the
    #    name of the dimension.
    #  + one binding that should be bound to the name of the dataset.
    def build_dimension_query(dimensions):

        dimensions = ["""\tGROUP_CONCAT(CASE WHEN "dimension" == ? THEN "item" END) AS %s""" % (sqlite3_quote_identifier(d)) for d in dimensions]
        query      = """
            SELECT\n\t"fact",\n%s\nFROM "dataset_dimension"\nWHERE "dataset" = ?\nGROUP BY "fact"
            """ % (",\n".join(dimensions))

        return query

    # Builds a query that embeds the result from build_dimension_query and therefore has
    #  + one binding that should be bound to the name of the dataset.
    #  + one binding for each of the dimensions that should be bound to the
    #    name of the dimension.
    #  + one binding that should be bound to the name of the dataset.
    def build_measure_query(dimension_query, dimensions):

        dimensions = ["%s" % (sqlite3_quote_identifier(d)) for d in dimensions]
        query      = """
            WITH
            "m" AS (SELECT "fact", "value" FROM "dataset_measure" WHERE "dataset" = ?),
            "d" AS (%s)

            SELECT
                "value",
                %s
            FROM "m"
            JOIN "d" ON "m"."fact" = "d"."fact"
            """ % (dimension_query, ",\n".join(dimensions))

        return query

    c             = flask.g.db.cursor()
    c.row_factory = sqlite3.Row

    r = c.execute("""
            SELECT DISTINCT "dimension" FROM "dataset_dimension" WHERE "dataset" = ? ORDER BY "dimension"
            """, (dataset,))

    r = r.fetchall()

    dimensions = []
    for d in r:
        dimensions.append(d["dimension"])

    q = build_measure_query(build_dimension_query(dimensions), dimensions)
    r = c.execute(q, (dataset, *dimensions, dataset))

    return render_request(
            Title = "%s Fact Table" % dataset,
            Main  = MAIN(
                H1(dataset, " Fact Table"),
                LARGE_TABLE(
                    THEAD(
                        TR(TH("Measure"), *[TH(A("/reference-table/%s/%s" % (dataset, d), d)) for d in dimensions])),
                    TBODY(
                        *[TR(
                            TD(f'{r[0]:,}', classes = ["n"]),
                            *[TD("%s" % c, classes = ["s"]) for c in r[1:]]) for r in r]),
                        )))

# Render a reference data table with one row per reference item and the
# standard columns we hold for every reference data table.
@app.route("/reference-table/<dataset>/<dimension>")
def reference_table(dataset, dimension) -> str:

    c = flask.g.db.cursor()

    # First  binding is dataset.
    # Second binding is dimension.
    q = """
        WITH
        "pre_table" AS (
            SELECT
                "di"."item" AS "item",
                "di"."hierarchy",
                "di"."sort_order",
                "dii"."lang",
                "dii"."description",
                "dii"."notes"
        FROM "odata_dataset_dimension_item" AS "di"
        JOIN "odata_dataset_dimension_item_info" AS "dii" ON "di"."item_index" = "dii"."item_index"
        WHERE "di"."dataset" = ? AND "di"."dimension" = ?)

        SELECT
            "item",
            "hierarchy",
            "sort_order",
            GROUP_CONCAT(CASE WHEN "lang" == 'cy-gb' THEN "description" END) AS "description_cy",
            GROUP_CONCAT(CASE WHEN "lang" == 'en-gb' THEN "description" END) AS "description_en",
            GROUP_CONCAT(CASE WHEN "lang" == 'cy-gb' THEN "notes"       END) AS "notes_cy",
            GROUP_CONCAT(CASE WHEN "lang" == 'en-gb' THEN "notes"       END) AS "notes_en"
        FROM "pre_table"
        GROUP BY "item"
        """

    r = c.execute(q, (dataset, dimension))

    col_types = ["symbol", "symbol", "symbol", "text", "text", "text", "text"]

    # Filter stuff
    c2 = flask.g.db.cursor()

    q2 = """
         SELECT
         DISTINCT "dataset"
         FROM "dataset_property_dimension"
         WHERE "dimension" = ?;
         """

    r2 = c2.execute(q2, (dimension,))

    c3             = flask.g.db.cursor()
    c3.row_factory = sqlite3.Row

    r3 = c3.execute("SELECT DISTINCT `dimension` FROM `dataset_dimension` WHERE `dataset` = ?;", (dataset,))

    r3 = r3.fetchall()

    dimensions = {}
    for d in r3:
        dimensions[d["dimension"]] = d["dimension"]

    c4             = flask.g.db.cursor()
    c4.row_factory = sqlite3.Row

    r4 = c4.execute(q, (dataset, dimension))

    items = {}
    for i in r4:
        items[i["item"]] = i["item"]




    # End of filter stuff

    return render_request(
            Title = "%s Reference Table for %s" % (dimension, dataset),
            Main  = MAIN(
                H1(dimension, " Reference Table for ", dataset),
                LARGE_TABLE(
                    THEAD(
                        TR(TH("Item", rowspan = 2), TH("Hierarchy", rowspan = 2), TH("Sort Order", rowspan = 2), TH("Description", colspan = 2), TH("Notes", colspan = 2)),
                        TR(TH("Welsh"), TH("English"), TH("Welsh"), TH("English"))),
                    TBODY(
                        *[TR(*[TD() if (c is None) else TD("%s" % c, classes = ["s"] if (t == "symbol") else ["t"]) for c, t in zip(r, col_types)]) for r in r]),
                )
                ,
                H2("Find similar cubes where..."),
                dimension, " = ",
                SELECT("v/%s" % dimension,
                    *OPTIONS(items),
                    OPTION("", True, "(omit)"),
                    ),
                SELECT("v/%s" % dimension, *OPTIONS(items)),  BR(),
                SELECT("d", *OPTIONS(dimensions, dimension),
                    OPTION("", True, "(omit)"),
                    ),
                SELECT("d", *OPTIONS(dimensions, dimension)),
                ACTION("filter-cubes-by-dimension", "Filter"),

                H2("Other cubes using this dimension"),
                UL(
                    *[LI(x[0]) for x in r2],
                    )


                ))

# Find all the cubes that match the given criteria.
# Criteria is a list of dimension names and, optionally, possible values for
# those dimensions.
# criteria - Dimension(Value,value...);Dimension;...
# FIXME: Decode values so that [,()] can appear in dimension names and values.
@app.route("/filter-cubes-by-dimension/<criteria>")
def filter_cubes_by_dimension(criteria) -> str:

    criteria = list(filter(None, criteria.split(";")))

    # Regex to match and bind "Dimension(Value...)".
    p = re.compile(r"^([^(]*)\(([^)]*)\)$")

    filtered_dimensions = []
    filters             = {}

    for c in criteria:
        m = p.match(c)
        if m != None:
            # "Dimension(Value...)"
            name          = m.group(1)
            values        = m.group(2).split(",")
            filters[name] = filters.get(name, []) + values
            if name not in filtered_dimensions:
                filtered_dimensions.append(name)
        else:
            # "Dimension"
            name = c
            filters[name] = filters.get(name, []) + []
            if name not in filtered_dimensions:
                filtered_dimensions.append(name)

    # Cons up the lists of items for all dimensions we have filters on.
    dimension_values = {}
    for d in filtered_dimensions:

        c = flask.g.db.cursor()
        q = """
            SELECT DISTINCT "item"
            FROM "odata_dataset_dimension_item"
            WHERE
            "dimension" = ?
            ORDER BY lower("item");
            """
        r = c.execute(q, (d,))

        items = {}
        for i in r:
            # Convert into a form suitable for OPTIONS.
            items[i[0]] = i[0]

        dimension_values[d] = items

    # Find all the cubes that meet the criteria.
    intersection = []
    for d in filtered_dimensions:
        q = """
            SELECT "dataset"
            FROM "odata_dataset_dimension"
            WHERE
            "dimension" = ?
            """
        intersection.append(q)
    intersection = " INTERSECT ".join(intersection)

    c = flask.g.db.cursor()
    q = """
        SELECT "dataset", "description"
        FROM "odata_metadata_tag"
        WHERE
        "tag" = 'Title'
        AND
        "dataset" in (%s)
        ORDER BY "dataset"
        """ % intersection
    r = c.execute(q, filtered_dimensions)

    datasets = r.fetchall()

    # Cons up the list of remaining dimensions. i.e. the subset of all
    # dimensions that are used by the cubes we have currently filtered in.
    remaining_dimensions = {}
    c = flask.g.db.cursor()
    q = ""
    r = None

    if (len(criteria) > 0):
        # Return all the dimensions when we're just starting out.
        q = """
            SELECT DISTINCT "dimension"
            FROM "odata_dataset_dimension"
            WHERE
            "dataset" in (%s)
            ORDER BY lower("dimension");
            """ % intersection
        r = c.execute(q, filtered_dimensions)
    else:
        # Otherwise only show the dimensions that the filtered in cubes actually have.
        q = """
            SELECT DISTINCT "dimension"
            FROM "odata_dataset_dimension"
            ORDER BY lower("dimension");
            """
        r = c.execute(q)

    for d in r:
        # Convert into a form suitable for OPTIONS.
        remaining_dimensions[d[0]] = d[0]


    return render_request(
            Title = "Cubes filtered by dimension criteria",
            Main  = MAIN(
                H1("A list of publications where..."),

                TABLE(
                    *[TR(  # A row for each filter.
                        TD("and" if (d != filtered_dimensions[0]) else ""),
                        # Name of the dimension.
                        TD(d),
                        TD(" is one of "),
                        # The criteria applied to the dimension plus an extra one for adding a new critera.
                        TD(
                            *more_itertools.intersperse(
                                BR(),
                                (SELECT("v/%s" % d,
                                    *OPTIONS(dimension_values[d], v),
                                    OPTION("", (v == ""), ("(nothing else)" if (len(filters[d]) > 0) else "(any value)")),
                                    ) for v in (filters[d] + [""])))),
                                ) for d in filtered_dimensions],
                    # An extra row for adding a new filter.
                    TR(
                        TD("and" if (len(filtered_dimensions) > 0) else ""),
                        TD("there is information about ",
                            SELECT("d",
                                *OPTIONS(remaining_dimensions),
                                OPTION("", True, "(pick something)")),
                            colspan = 3))
                    ),
                ACTION("filter-cubes-by-dimension", "Apply Filter"),

                BR(),

                UL(
                    *[LI(A("/render/%s" % d[0], "%s" % d[0]), ": ", d[1]) for d in datasets])
                ))

# Starting page for finding cubes that match a given criteria.
@app.route("/filter-cubes-by-dimension/")
def filter_cubes_by_dimension_0() -> str:
    return filter_cubes_by_dimension("")


################################################################################
### HTTP Controllers: POST Request handlers.

# Converts a table specification in rows and columns into a URL for the dataset.
def specify_table(dataset = None, col = None, row = None, **kwargs):

    if not (dataset and col and row):
        return ("specify_table: dataset, col and row are required!", 400)

    x = []
    for n in range(0, len(col)):
        v = col.get("%s" %n)
        if (v != None) and (v != ""):
            x.append(v)

    y = []
    for n in range(0, len(row)):
        v = row.get("%s" %n)
        if (v != None) and (v != ""):
            y.append(v)

    if ((len(x) > 0) and (len(y) > 0)):
        return redirect("render", dataset, ",".join(x), ",".join(y))
    else:
        return redirect("render", dataset)


# Converts a set of dimension and dimension/value filters into a URL that
# displays that filter and its results.
# v is a dictionary of dimension names to values to filter by.
# d is a dimension name to add to the filter.
# FIXME: Encode values so that [,()] can appear in dimension names and values.
# FIXME: We don't sort dimension names or values so that things stay in "the
#        same order" as the user specified them. But (a) we don't know that
#        this actually preserves the order as the args are stored in a hash
#        table and (b) this results in lots of URLs for the same criteria.
# FIXME: We don't filter out non-unique values.
def action_filter_cubes_by_dimension(v = {}, d = ""):

    criteria = []

    for (k,kv) in v.items():
        values = []
        if isinstance(kv, list):
            for x in kv:
                if x not in values:
                    values.append(x)
        else:
            values = [kv]
        values = list(filter(None,values))
        if (len(values) > 0):
            criteria.append("%s(%s)" % (k, ",".join(values)))
        else:
            criteria.append(k)

    if isinstance(d, list):
        for x in d:
            if x not in v:
                criteria.append(x)
    else:
        if d not in v:
            criteria.append(d)

    criteria = ";".join(filter(None, criteria))

    return redirect("filter-cubes-by-dimension", criteria)

# Converts a set of dimension and dimension/value filters into a URL that
# displays that filter and its results.
# v is a dictionary of dimension names to values to filter by.
# d is a dimension name to add to the filter.
# FIXME: Encode values so that [,()] can appear in dimension names and values.
# FIXME: We don't sort dimension names or values so that things stay in "the
#        same order" as the user specified them. But (a) we don't know that
#        this actually preserves the order as the args are stored in a hash
#        table and (b) this results in lots of URLs for the same criteria.
# FIXME: We don't filter out non-unique values.
# Adapted from action_filter_cubes_by_dimension so that it works with the
# multi-valued selections in dimension items.
def action_demo_update_filters(v = {}, d = "", **kwargs):

    criteria = []

    for (k,kv) in v.items():
        values = []
        if isinstance(kv, list):
            for x in kv:
                if x not in values:
                    values.append(x)
        else:
            values = [kv]
        values = list(filter(None,values))
        if (len(values) > 0):
            criteria.append("%s(%s)" % (k, ",".join(values)))
        else:
            criteria.append(k)

    if isinstance(d, list):
        for x in d:
            if x not in v:
                # Extract the category.
                c = x.split(":")[0]
                print("c: %s, v: %s" % (c, v))

                if ((c in v) and (v[c] == False)):
                    print ("v[%s] = %s" % (c, v[c]))
                    criteria.append(x)
                else:
                    criteria.append(x)
    else:
        raise("d is not a list")
        if d not in v:
            criteria.append(d)

    criteria = ";".join(filter(None, criteria))

    return redirect("demo", "search", criteria)


#    return render_request(
#            Title = "x",
#            Main  = MAIN(
#                UL(
#                *[LI("v[", x, "] = ", v[x]) for x in v],
#                LI("d = ", d)),
#                criteria
#                ))

def action_set_lang(lang = None):

    TEN_YEARS = 60 * 60 * 24 * 365.25 * 10

    if lang in ["en-gb", "cy-gb"]:

        @flask.after_this_request
        def set_lang(response):
            response.set_cookie("lang", lang, TEN_YEARS)
            return response

        return redisplay()

    else:

        return ("Bad request: language must be 'en-gb' or 'cy-gb'!", 400)


# POST action dispatch table.
actions = {
        "specify_table": specify_table,
        "filter-cubes-by-dimension": action_filter_cubes_by_dimension,
        "set-lang": action_set_lang,
        "demo-update-filters": action_demo_update_filters,
        }

@app.route('/', defaults={'path': ''}, methods=['POST'])
@app.route('/<path:_>', methods=['POST'])
def penultimate_form_handler(_):

    recursive_dict = lambda: collections.defaultdict(recursive_dict)

    form_tree = collections.defaultdict(recursive_dict)

    # The list of tuples containing the action and its place in form_tree.
    form_actions = []

    for path, value in flask.request.form.to_dict(flat = False).items():

        path     = path.split("/")
        dirname  = path[:-1]
        basename = path[-1]

        # Navigate a path into the form_tree
        d = form_tree
        for name in dirname:
            d = d[name]

        if basename == "action":
            # Record the action and it's location.
            if len(value) != 1:
                return ("Bad request: malformed action: action must not be multivalued.", 400)
            form_actions.append((value[0], d))
        else:
            # Poke the value into the form_tree at the selected position.
            if len(value) > 1:
                # Multivalued form fields.
                d[basename] = value
            else:
                # Single valued form fields.
                d[basename] = value[0]

    if len(form_actions) > 1:
        # We only support a single action for now!
        return ("Bad request: more than one action specified!", 400)
    elif len(form_actions) == 0:
        return ("Bad request: an action must be specified!", 400)

    (form_action, form_tree) = form_actions[0]
    form_action              = actions.get(form_action)

    def show_error(e):

        def dict_to_table(d):
            return element("table", {"border": "1", "style": "border-collapse: collapse;"},
                    *[TR(TD(k), TD(v) if (not isinstance(v, dict)) else TD(dict_to_table(v))) for k, v in d.items()])

        error  = None
        status = None

        if (e is None):
            error  = [H1("Specified action not found!")]
            status = 404
        else:
            error = [
                    H1("Action threw exception"),
                    P(e)
                    ]
            status = 500

        return (render_request(
            Title = "Form Submission Error",
            Menu  = DIV({}),
            Main = MAIN(
                *error,
                H2("Data from active form"),
                element("table", {"border": "1"},
                    TR(TD("form_action"), TD(form_actions[0][0])),
                    TR(TD("form_tree"),   TD(dict_to_table(form_tree)))),
                H2("Data from all logical forms"),
                dict_to_table(flask.request.form.to_dict(flat = False))

            )),
                status)

    if (form_action == None):
        return show_error(None)
    else:
        try:
            r = form_action(**form_tree)
            return r
        except Exception as e:
            return show_error(e)


################################################################################
### HTTP Connection & Request management.

# Things to do at the beginning of each HTTP Request.
@app.before_request
def before_request():

    # For measuring wallclock time of Request.
    # Includes time spent sleeping.
    flask.g.start_perf_counter = time.perf_counter_ns()

    # For measuring processing time of Request.
    # System plus user CPU time.
    flask.g.start_thread_time = time.thread_time_ns()

    # Get a database handle from the pool.
    flask.g.db = db_pool.get(block = True)

    # Work out if the user has specified a preferred language.
    lang = flask.request.cookies.get("lang")
    if lang in ["en-gb", "cy-gb"]:
        flask.g.lang = lang
    else:
        flask.g.lang = "en-gb"

# Things to do at the end of each HTTP Request.
@app.teardown_request
def teardown_request(err):
    # Return the database handle to the pool.
    db         = flask.g.db
    flask.g.db = None
    db_pool.put(db, block = True)


################################################################################
### Main Program.

def initialise():

    global db_pool

    # Initialise the database pool.
    db_pool = queue.Queue(db_pool_size)

    # Fill the pool with the right number of connection objects.
    for _ in range(db_pool_size):

        db = sqlite3.connect(db_file, check_same_thread = False)

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

        db_pool.put(db)


if __name__ == "__main__":
    initialise()
    app.run()

