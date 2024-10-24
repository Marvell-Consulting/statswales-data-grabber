################################################################################
#
# i18n.py
#
# A quick and dirty implementation of an internationalization procedure.
#
#
# Andy Bennett <andyjpb@register-dynamics.co.uk>, 2024/10/18 15:27.
#
################################################################################


import flask
import widgets


################################################################################
### Welsh Translations.


cy = {
        "You're viewing a new version of StatsWales."          : "Rydych chi'n edrych ar fersiwn newydd o Statswales.",
        "StatsWales"                                           : "StatsCymru",
        "Find statistics and data from the Welsh Government"   : "Dewch o hyd i ystadegau a data gan lywodraeth Cymru",
        "Filter by category"                                   : "Hidlo yn ôl categori",
        "Information about"                                    : "Gwybodaeth am",
        "Search for datasets"                                  : "Chwilio am setiau datao",
        "Your Filters"                                         : "Eich hidlwyr",
        "Choose the topic, dataset, dimensions and time period you want to view. You'll then be able to view the table and download the data." : "Dewiswch y pwnc, y set ddata, y dimensiynau a'r cyfnod amser rydych chi am ei weld. Yna byddwch chi'n gallu gweld y tabl a lawrlwytho'r data.",
        "Results by topic"                                     : "Canlyniadau yn ôl pwnc",
        "results"                                              : "ganlyniadau",
        "Unknown"                                              : "Anhysbys",
        "STEP"                                                 : "CAM",
        "AND"                                                  : "AC",
        "Remove"                                               : "Dileu",
        "Any"                                                  : "Unrhyw",
        "Update Filters"                                       : "Diweddaru Hidlau",
        }



################################################################################
### Translation procedures.

# Translate text into the user's langauge.
def _(text):

    l = flask.g.lang

    if (l == "en-gb"):
        return text
    else:
        translation = cy.get(text)
        if (translation is None):
            translation = widgets.element("font", {"color": "dark-red"}, text)
        return translation


################################################################################
### Main Program.

if __name__ == "__main__":
    pass

