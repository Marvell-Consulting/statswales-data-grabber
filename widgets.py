################################################################################
#
# widgets.py
#
# Rendering and other supporting infrastructure for HTML Widget sets.
#
#
# Andy Bennett <andyjpb@register-dynamics.co.uk>, 2024/10/17 15:52.
#
################################################################################


import xml.etree.ElementTree


################################################################################
### Helpers

# Formats a count of nanoseconds into an appropriate number of ns, us, ms or s.
def format_ns(s):

    i = 0
    while ((s >= 1000) and (i < 3)):
        s /= 1000.0
        i += 1

    return "%s%s" % (f'{s:.1f}', ["ns", "μs", "ms", "s"][i])


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

