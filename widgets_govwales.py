################################################################################
#
# widget_govwales.py
#
# A quick and dirty implementation of the GOV.WALES design system that the
# Welsh Government calles the GOV.WALES Global Experience Language (GEL).
# https://www.gov.wales/govwales-design-standards
#
#
# Andy Bennett <andyjpb@register-dynamics.co.uk>, 2024/10/17 15:46.
#
################################################################################


import flask
import time
import widgets
from widgets_html import *
from i18n import _


element = widgets.element


################################################################################
### Widget definitions.

def BODY(*contents):
    return element("body",
            {"class": "govuk-template__body"},
            SCRIPT("document.body.className += ' js-enabled' + ('noModule' in HTMLScriptElement.prototype ? ' govuk-frontend-supported' : '');"),
            element("a", {"href": "#main-content", "class": "govuk-skip-link", "data-module": "govuk-skip-link"}, "Skip to main content"),
            *contents)

# A submit button for a form.
def ACTION(action, *contents, classes = []):
    attribs = {
            "type": "submit",
            "name": "action",
            "value": action,
            }
    if (len(classes) > 0):
        attribs["class"] = " ".join(classes)
    return element("button", attribs, *contents)


## Application level widgets.

# A page template that sorts out the HEAD section and leaves the BODY to the
# caller.
def PAGE(title, *contents):
    return HTML(
            HEAD(
                META("charset", "UTF-8"),
                TITLE(title, " : StatsWales : GOV.WALES"),
                element("meta", {"name": "viewport",    "content": "width=device-width, initial-scale=1, viewport-fit=cover"}),
                element("meta", {"name": "theme-color", "content": "#0b0c0c"}),
                element("link", {"rel": "icon", "sizes": "48x48", "href": "/widgets_govwales/favicon.ico"}),
                element("link", {"rel": "icon", "sizes": "any",   "href": "/widgets_govwales/favicon.svg", "type": "image/svg+xml"}),
                element("link", {"rel": "mask-icon",        "href": "/widgets_govwales/govuk-icon-mask.svg", "color": "#0b0c0c"}),
                element("link", {"rel": "apple-touch-icon", "href": "/widgets_govwales/govuk-icon-180.png"}),
                element("link", {"rel": "manifest", "href": "/widgets_govwales/manifest.json"}),
                element("link", {"rel": "stylesheet", "href": "/widgets_govwales/application.css", "type": "text/css"}),
                element("link", {"rel": "stylesheet", "href": "/widgets_govwales/wg.css",          "type": "text/css"}),
                ),
            BODY(PENULTIMATE_FORM(*contents),
                element("script", {"src": "/widgets_govwales/kit.js"}),
                element("script", {"src": "/widgets_govwales/auto-store-data.js"}),
                element("script", {"type": "module", "src": "/widgets_govwales/govuk-frontend.min.js"}),
                element("script", {"type": "module", "src": "/widgets_govwales/init.js"}),
                element("script", {"src": "/widgets_govwales/all.js"}),
                element("script", {"src": "/widgets_govwales/init-all.js"}),
                element("script", {"type": "module", "src": "/widgets_govwales/application.js"})))

def LANGUAGE_SWITCHER(lang):
    return DIV({"class": "language-switcher-language-url", "id": "block-govwales-languageswitcher", "role": "navigation", "aria-label": "Language"},
            element("ul", {"class": "links"},
                element("li", {"class": ("en is-active" if (lang == "en-gb") else "en")},
                    HIDDEN("lang/en/lang", "en-gb"),
                    element("button",{"type": "submit", "name": "lang/en/action", "value": "set-lang", "class": "language-link", "lang": "en", "role": "button", "hreflang": "en"},
                        "English")),
                element("li", {"class": ("cy is-active" if (lang == "cy-gb") else "cy")},
                    HIDDEN("lang/cy/lang", "cy-gb"),
                    element("button",{"type": "submit", "name": "lang/cy/action", "value": "set-lang", "class": "language-link", "lang": "cy", "role": "button", "hreflang": "cy"},
                        "Cymraeg"))))

def LOGO(path):
    return DIV({"id": "block-govwales-branding"},
            element("a", {"href": path, "title": "Welsh Government", "class": "header__logo", "id": "logo"},
                SPAN({"class": "visually-hidden"},
                    "Home")),
                #SPAN({"class": "print header__logo_print"},
                #    element("img", {"src": "/widgets_govwales/logo.png", "alt": "Welsh Government"}))
                )

def HEADER(lang, logo_path):
    return element("header", {"id": "wg-header", "class": "wg-header", "style": "background-color: #323232;"},
            DIV({"class": "layout-container"},
                DIV({"class": "header", "id": "header"},
                    DIV({"class": "header__components container-fluid"},
                        LOGO(logo_path),
                        LANGUAGE_SWITCHER(lang),
                                ))))

def PHASE_BANNER(phase):
    return DIV({"class": "govuk-phase-banner"},
            DIV({"class": "govuk-width-container"},
                element("p", {"class": "govuk-phase-banner__content"},
                    element("strong", {"class": "govuk-tag govuk-phase-banner__content__tag"},
                        phase),
                    SPAN({"class": "govuk-phase-banner__text"},
                        _("You're viewing a new version of StatsWales."),
                        #element("a", {"class": "govuk-link", "href": "feedback.html", "target": "_blank"},
                        #    "Give feedback (opens in new tab)")
                        ))))

def MENU(*contents):
    return []

def FOOTER(*contents):
    return element("footer", {"class": "wg-footer"},
            DIV({"class": "govuk-width-container govuk-!-padding-top-9"},
                element("ul", {"class": "footer-menu govuk-list"},
                    element("li", {"class": "menu__item"}, A("https://www.gov.wales/contact-us",                                "Contact us")),
                    element("li", {"class": "menu__item"}, A("https://www.gov.wales/accessibility-statement-govwales",          "Accessibility")),
                    element("li", {"class": "menu__item"}, A("https://www.gov.wales/copyright-statement",                       "Copyright statement")),
                    element("li", {"class": "menu__item"}, A("https://www.gov.wales/help/cookies",                              "Cookies")),
                    element("li", {"class": "menu__item"}, A("https://www.gov.wales/website-privacy-policy",                    "Privacy")),
                    element("li", {"class": "menu__item"}, A("https://www.gov.wales/terms-and-conditions",                      "Terms and conditions")),
                    element("li", {"class": "menu__item"}, A("https://www.gov.wales/welsh-government-modern-slavery-statement", "Modern slavery statement")),
                    element("li", {"class": "menu__item"}, A("https://www.gov.wales/alternative-languages",                     "Alternative languages")),
                ),
            DIV({"class": "govuk-width-container govuk-!-padding-top-9"}, *contents),
            DIV({"class": "wg-footer-logo"}),
            DIV({}, BR(), BR())))

def MAIN(*contents):
    return DIV({"class": "govuk-width-container"},
            element("main", {"class": "govuk-main-wrapper", "id": "main-content", "role": "main"},
                element("link", {"rel": "stylesheet", "href": "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css"}),
                *contents))


## Content level widgets.

# Grid Layout.
# Most pages are laid out on a grid.

def GRID_ROW(*contents):
    return DIV({"class": "govuk-grid-row"}, *contents)

def GRID_COL(size, *contents):

    if size in ["full", "one-half", "one-third", "two-thirds"]:
        size = "govuk-grid-column-" + size
    else:
        size = "govuk-grid-column"

    return DIV({"class": size}, *contents)

# Headings.

def H1(heading, *contents, caption = None):

    contents = [heading, *contents]

    if (caption is not None):
        contents.append(
                SPAN({"class": "govuk-caption-l"}, caption))

    return element("h1", {"class": "govuk-heading-l"}, *contents)

# Tables.

def TABLE(*contents):
    return element("table", {"class": "govuk-table"}, *contents)

def TBODY(*contents):
    return element("tbody", {"class": "govuk-table__body"}, *contents)

def TR(*contents):
    return element("tr", {"class": "govuk-table__row"}, *contents)

def TD(*contents, colspan = None, classes = []):
    classes.append("govuk-table__cell")
    attribs = {}
    if (colspan != None):
        attribs["colspan"] = str(colspan)
    if (len(classes) > 0):
        attribs["class"] = " ".join(classes)
    return element("td", attribs, *contents)

# Search banner.

def SEARCH_BANNER(label = "Search"):
    return DIV({"class": "search-banner"},
            DIV({"class": "govuk-form-group"},
                element("h2", {"class": "govuk-label-wrapper"},
                    element("label", {"class": "govuk-label govuk-label--m"},
                        label)),
                element("input",
                    {"class": "govuk-input govuk-input--width-30",
                        "id": "search-global",
                        "style": "display: inline-block",
                        "name": "search/terms",
                        "type": "text"}),
                element("button",
                    {"class": "govuk-button",
                        "style": "display: inline-block",
                        "type": "submit",
                        "name": "search/action",
                        "value": "search"},
                    "Search")))

# Article.
# An Article is a little grey box with a blue bar down the left hand side.

def ARTICLE_TITLE(*contents):
    return SPAN({"class": "article-title"}, *contents)

def ARTICLE_SUB_HEADING(*contents):
    return element("p", {"class": "article-sub-heading"}, *contents)

def ARTICLE_P(*contents):
    return element("p", {"style": "margin-bottom: 0px;"}, *contents)

def ARTICLE(title, sub_heading, *contents):
    return DIV({"class": "article"},
            ARTICLE_TITLE(title),
            BR(),
            ARTICLE_SUB_HEADING(sub_heading),
            *[ARTICLE_P(x) for x in contents])

# Topic.
# A Topic is an area delimited by whitespace left, right and bottom and a line
# across the top.

def TOPIC(title, href, description):
    return GRID_COL("one-third",
            HR(),
            element("h3", {"class": "govuk-heading-s"},
                A(href, title)),
            P(description))

# Dataset Timeline.
# A Dataset Timeline is an area delimited by whitespace left, right and bottom
# and a line across the top. It contains a table listing datasets and when they
# were updated.
def DATASET_TIMELINE(title, datasets):
    return GRID_COL("one-half",
            HR(),
            H3(title),
            TABLE(
                TBODY(
                    TR(
                        TD(STRONG("Dataset")),
                        TD(STRONG("Updated"))),
                    *[TR(
                        TD(A(href, title)),
                        TD(date))
                        for (title, href, date) in datasets])))

def FILTERS(*filters):
    return GRID_ROW(
            GRID_COL("full",
                TABLE(
                    TBODY(*filters))))

def FILTER(step, category, values, action):
    return TR(
            element("td", {"class": "govuk-table__cell", "style": "width: 120px;"},
                SPAN({"class": "badge"}, step)),
            TD(STRONG(category)),
            TD(values),
            TD(action))


################################################################################
### Page rendering support.

# Renders a complete page based on the PAGE widget and optional caller-supplied
# inserts.
def render_request(Title = None, Lang = None, Home = None, Phase = None, Menu = None, Main = None, Footer = None):

    if (Title is None):
        Title = "tram rabbit"

    if (Home is None):
        Home = "/"

    if (Phase is None):
        Phase = []
    else:
        Phase = [PHASE_BANNER(Phase)]

    if (Menu is None):
        Menu = MENU(
                ("/", "Datasets"),
                ("/filter-cubes-by-dimension/", "Filters"),
                )

    if (Main is None):
        Main = MAIN(P("No page content specified!"))

    if (Footer is None):
        Footer = []

    perf_counter  = time.perf_counter_ns()
    thread_time   = time.thread_time_ns()

    perf_counter -= flask.g.start_perf_counter
    thread_time  -= flask.g.start_thread_time

    return widgets.elements_to_str(
            PAGE(Title,
                HEADER(
                    Lang,
                    Home),
                *Phase,
                Main,
                FOOTER(
                    *Footer,
                    widgets.format_ns(perf_counter), " (wallclock)",
                    BR(),
                    widgets.format_ns(thread_time),  " (sys+user cpu)")))


################################################################################
### Main Program.

if __name__ == "__main__":
    pass
