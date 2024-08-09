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


################################################################################
### Configuration.

db_pool_size = 3
db_file      = "statswales2.hypercube.sqlite"


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


# Formats a count of nanoseconds into an appropriate number of ns, us, ms or s.
def format_ns(s):

    i = 0
    while ((s >= 1000) and (i < 3)):
        s /= 1000.0
        i += 1

    return "%s%s" % (f'{s:.1f}', ["ns", "μs", "ms", "s"][i])



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

# Construct the XML structure for an element and its contents.
# Test cases:
#   visualise.elements_to_str(visualise.element("P", {}, "this is a test"))
#     -> b'<P>this is a test</P>
#   visualise.elements_to_str(visualise.element("P", {}, "this is", "a test"))
#     -> b'<P>this isa test</P>
#   visualise.elements_to_str(visualise.element("P", {}, "this is", visualise.element("i", {}, "a"), "test"))
#     -> b'<P>this is<i>a</i>test</P>
def element(tag, attribs = {}, *contents):
    e = xml.etree.ElementTree.Element(tag, attribs)

    prev  = None
    first = True
    for c in contents:
        if (first == True):
            if (isinstance(c, (xml.etree.ElementTree.Element,))):
                e.append(c)
                prev = c
            else:
                e.text = str(c)
                prev  = e
            first = False
        else:
            if (not isinstance(c, (xml.etree.ElementTree.Element,))):
                if (prev == e):
                    p = prev.text
                else:
                    p = prev.tail
                if (isinstance(p, str)):
                    p += str(c)
                else:
                    p = str(c)
                if (prev == e):
                    prev.text = p
                else:
                    prev.tail = p
            else:
                e.append(c)
                prev = c

    return e


def elements_to_str(elements):
    return xml.etree.ElementTree.tostring(elements, encoding='utf-8', method='html')

# Safely redirects to the specified URL by ensuring the path elements are
# properly encoded.
def redirect(*path):

    path = [urllib.parse.quote(x, safe="") for x in path]
    path = "/" + "/".join(path)

    return flask.redirect(path, code = 302)


## Lo-level HTML elements.

def HTML(*contents):
    return element("html", {}, *contents)

def HEAD(*contents):
    return element("head", {}, *contents)

def META(key, value):
    return element("meta", {key: value})

def TITLE(*title):
    return element("title", {}, *title)

def BODY(*contents):
    return element("body", {}, *contents)

def A(href, *contents):
    return element("a", {"href": href}, *contents)

def P(*contents):
    return element("p", {}, *contents)

def BR():
    return element("br", {})

def SPAN(*contents):
    return element("span", {}, *contents)

def IMG(src):
    return element("img", {"src": src})

def H1(*contents):
    return element("h1", {}, *contents)

def H2(*contents):
    return element("h2", {}, *contents)

def UL(*contents):
    return element("ul", {}, *contents)

def OL(*contents):
    return element("ol", {}, *contents)

def LI(*contents):
    return element("li", {}, *contents)

def TABLE(*contents, classes = []):
    if (len(classes) == 0):
        return element("table", {}, *contents)
    else:
        return element("table", {"class": ", ".join(classes),}, *contents)

def THEAD(*contents):
    return element("thead", {}, *contents)

def TBODY(*contents):
    return element("tbody", {}, *contents)

def TR(*contents):
    return element("tr", {}, *contents)

def TH(*contents, colspan = None, rowspan = None):
    attribs = {}
    if (colspan != None):
        attribs["colspan"] = str(colspan)
    if (rowspan != None):
        attribs["rowspan"] = str(rowspan)
    return element("th", attribs, *contents)

def TD(*contents, classes = []):
    if (len(classes) == 0):
        return element("td", {}, *contents)
    else:
        return element("td", {"class": ", ".join(classes),}, *contents)

def FORM(*contents, enctype = "application/x-www-form-urlencoded"):
    return element("form",
            {
                "method": "POST",
                "accept-charset": "utf-8",
                "enctype": enctype,
                "action": "",
                },
            *contents)

def INPUT(type, name, value):
    return element("input",
            {
                "type": type,
                "name": name,
                "value": value,
                })

def OPTION(value, selected, *contents):
    if selected:
        return element("option", {"value": value, "selected": ""}, *contents)
    else:
        return element("option", {"value": value}, *contents)

def SELECT(name, *contents):
    return element("select", {"name": name}, *contents)


## Lo-level HTML5 elements.

# The main content of the page
def MAIN(*contents):
    return element("main", {}, *contents)

def HEADER(*contents):
    return element("header", {}, *contents)

def FOOTER(*contents):
    return element("footer", {}, *contents)


## Helpers for Lo-level HTML & HTML5 elements.

# Takes a dictionary and returns it as a list of OPTIONs.
# Marks the appropriate OPTION as selected.
def OPTIONS(options, selected = None):
    return [OPTION(k, (selected == k), v) for k,v in options.items()]

def HIDDEN(name, value):
    return INPUT("hidden", name, value)

def NAV(items):
    return element("nav",
            *[A(href, text) for href, text in items])


## Higher level widgets to support the app server.

# One per page.
def PENULTIMATE_FORM(*contents, csrf_token = None):
    return FORM(
            element("input", {"type": "submit", "disabled": ""}),  # Prevent Enter from submitting forms.
            *([HIDDEN("csrf-token", csrf_token)] if csrf_token else []),
            *contents)

# For delimiting logical sub-forms within a page.
def LOGICAL_FORM(name, *contents):
    return element("__logical_form__", {"name": name}, *contents)

# A submit button for a form.
def ACTION(action, *contents):
    return element("button", {
        "type": "submit",
        "name": "action",
        "value": action,
        }, *contents)


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
                )

    if (Main is None):
        Main = P("No page content specified!")

    if (Footer is None):
        Footer = []

    perf_counter  = time.perf_counter_ns()
    thread_time   = time.thread_time_ns()

    perf_counter -= flask.g.start_perf_counter
    thread_time  -= flask.g.start_thread_time

    return elements_to_str(
            PAGE(Title,
                HEADER(
                    LOGO(),
                    Menu),
                Main,
                FOOTER(
                    *Footer,
                    format_ns(perf_counter), " (wallclock)",
                    BR(),
                    format_ns(thread_time),  " (sys+user cpu)")))


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

# Support files
@app.route("/visualise.css")
def stylesheet() -> str:
    return flask.send_from_directory(".", "visualise.css")

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
                )))



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


# POST action dispatch table.
actions = {
        "specify_table": specify_table,
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

    if (form_action == None):
        return ("Specified action not found!", 404)

    return form_action(**form_tree)


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
    flask.g.db = db_pool.get(block = False)

# Things to do at the end of each HTTP Request.
@app.teardown_request
def teardown_request(err):
    # Return the database handle to the pool.
    db         = flask.g.db
    flask.g.db = None
    db_pool.put(db, block = False)


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
