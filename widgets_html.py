################################################################################
#
# widgets_html.py
#
# Lo-level HTML elements for use in other widget sets.
#
#
# Andy Bennett <andyjpb@register-dynamics.co.uk>, 2024/10/17 15:51.
#
################################################################################


import widgets


element = widgets.element


################################################################################
### Widget definitions.

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

def DIV(attributes, *contents):
    return element("div", attributes, *contents)

def SPAN(attributes, *contents):
    return element("span", attributes, *contents)

def IMG(src):
    return element("img", {"src": src})

def H1(*contents):
    return element("h1", {}, *contents)

def H2(*contents):
    return element("h2", {}, *contents)

def H3(*contents):
    return element("h3", {}, *contents)

def STRONG(*contents):
    return element("strong", {}, *contents)

def EM(*contents):
    return element("em", {}, *contents)

def UL(*contents):
    return element("ul", {}, *contents)

def OL(*contents):
    return element("ol", {}, *contents)

def LI(*contents):
    return element("li", {}, *contents)

def HR():
    return element("hr", {})

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

#def TD(*contents, classes = []):
#    if (len(classes) == 0):
#        return element("td", {}, *contents)
#    else:
#        return element("td", {"class": ", ".join(classes),}, *contents)
def TD(*contents, colspan = None, classes = []):
    attribs = {}
    if (colspan != None):
        attribs["colspan"] = str(colspan)
    if (len(classes) > 0):
        attribs["classes"] = ", ".join(classes)
    return element("td", attribs, *contents)

def SCRIPT(*contents):
    return element("script", {}, *contents)

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

