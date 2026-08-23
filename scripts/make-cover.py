"""Generate the departmental title page as centred raw OpenXML.

Markdown cannot centre a paragraph and pandoc's gfm reader has no syntax for
it, so the cover is emitted as raw OpenXML paragraphs carrying an explicit
<w:jc w:val="center"/>. That matches the departmental template exactly instead
of approximating it with left-aligned text.
"""
import os, re, sys

src, dst = sys.argv[1], sys.argv[2]

TITLE = ("The Architecture of OpenMolt Network: A Peer-to-Peer Protocol for "
         "Agent-to-Agent Discovery and Delegation")
NAME        = "Sahil Pohare"
YEAR        = "Dissertation 2026"
DEGREE      = "MSc in Computer Science (Software Engineering)"
HEAD        = "Head of Department: Kevin Casey"
SUPERVISOR  = "Supervisor: Kevin Casey"
DATE        = "23/08/2026"
LOGO        = "assets/maynooth-logo.png"

# The departmental limit of 22,000 words excludes appendices, so the count
# stops at Appendix A. Tables and figure captions are counted, which is the
# conservative reading.
text = open(src).read()
body = text[:text.index("## Appendix A.")]
words = len(body.split())

def esc(t):
    return (t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

# A4 is 16838 twips tall and the section uses 1440-twip margins, so the cover
# has 13958 twips of usable height. Rather than hard-coding gaps that leave the
# page two-thirds empty, work out the fixed content height and share the
# remainder across the five block gaps. That keeps the page filled whether or
# not the logo image is present, and survives the title wrapping to two lines.
LINE      = 240                       # one 12pt line
USABLE    = 16838 - 1440 * 2
TARGET    = int(USABLE * 0.92)
MINOR     = 120                       # within a block
LEAD      = 360                       # before a block
have_logo = os.path.exists(os.path.join("docs", LOGO))
LOGO_H    = 2600 if have_logo else LINE

text_lines = 15 + (2 if len(TITLE) > 70 else 1)
fixed  = text_lines * LINE + LOGO_H + MINOR * 6 + LEAD * 3
GAP    = max(400, (TARGET - fixed) // 5)

def para(t="", bold=False, before=0, after=MINOR):
    runs = ""
    if t:
        rpr = "<w:rPr><w:b/></w:rPr>" if bold else ""
        runs = f"<w:r>{rpr}<w:t xml:space=\"preserve\">{esc(t)}</w:t></w:r>"
    return (f'<w:p><w:pPr><w:jc w:val="center"/>'
            f'<w:spacing w:before="{before}" w:after="{after}" w:line="240" w:lineRule="auto"/>'
            f'</w:pPr>{runs}</w:p>')

lines = [para(TITLE, bold=True, after=GAP),
         para(NAME, bold=True, before=LEAD),
         para(YEAR), para(DEGREE, after=GAP)]

out = ['```{=openxml}'] + lines + ['```', '']

# The logo is a normal Markdown image so pandoc builds the media relationship
# for us; raw OpenXML cannot reference an image without one.
if have_logo:
    out += [f"![]({LOGO})", '']
    out += ['```{=openxml}', para(after=GAP), '```', '']
else:
    out += ['```{=openxml}', para("[Maynooth University logo: place the file at "
                                  f"docs/{LOGO}]", after=GAP), '```', '']

tail = [para("Department of Computer Science", before=LEAD),
        para("Maynooth University, Maynooth"),
        para("Co. Kildare, Ireland", after=GAP),
        para("A dissertation submitted in partial", after=0),
        para("fulfilment of", after=0),
        para("the requirements for the", after=0),
        para(DEGREE, after=GAP),
        para(HEAD, before=LEAD),
        para(SUPERVISOR),
        para(DATE),
        para(f"Word Count: {words:,}", after=0),
        '<w:p><w:r><w:br w:type="page"/></w:r></w:p>']
out += ['```{=openxml}'] + tail + ['```', '']

open(dst, "w").write("\n".join(out))
print(f"  cover page: word count {words:,}, gap {GAP} twips, logo {'image' if have_logo else 'placeholder'}", file=sys.stderr)
