"""Collect relevant icons to icon theme subdirectories

To install the KDE breeze theme:

    apt install breeze-icon-theme

Fontawesome icons are downloaded from GitHub.

This script must be run on a linux machine. Please make sure
that `local_root` is correct.
"""
import atexit
import collections
import pathlib
import shutil
import tempfile
from xml.dom import minidom

import requests


# The key identifies the theme; each list contains icon names
icons = {
    "breeze": [
        "application-exit",
        "dialog-cancel",
        "dialog-close",
        "dialog-error",
        "dialog-information",
        "dialog-messages",
        "dialog-ok",
        "dialog-ok-apply",
        "dialog-question",
        "dialog-warning",
        "documentinfo",
        "document-open",
        "document-open-folder",
        "document-save",
        "edit-clear",
        "gtk-preferences",
        "messagebox_warning",
        "preferences-activities",
    ],
    "fontawesome": [
        "ban",
        "book",
        "broom",
        "check-double",
        "child",
        "circle",
        "clinic-medical",
        "code-branch",
        "cogs",
        "eye",
        "exclamation-triangle",
        "filter",
        "folder",
        "globe",
        "hat-wizard",
        "hourglass",
        "info",
        "people-arrows",
        "people-carry",
        "puzzle-piece",
        "redo",
        "shipping-fast",
        "slash",
        "street-view",
        "tag",
        "trash",
        "trash-alt",
        "undo",
        "upload",
        "user",
        "user-lock",
        "user-times",
        "users",
    ],
}

# theme index file
index = """[Icon Theme]
Name=DCORAidMix
Comment=Mix of themes for DCOR-Aid

Directories={directories}
"""

# theme file folder item
index_item = """
[{directory}]
Size={res}
Type=Fixed
"""

local_root = pathlib.Path("/usr/share/icons")
web_roots = {
    "fontawesome": collections.OrderedDict(
        solid="https://raw.githubusercontent.com/FortAwesome/"
              + "Font-Awesome/master/svgs/solid/"
    )}


# create a temporary directory
tmpdir = pathlib.Path(tempfile.mkdtemp(prefix="icons_"))
atexit.register(lambda: shutil.rmtree(tmpdir, ignore_errors=True))


def process_svg(svgdata):
    """Set svg color to gray and add a 10% margin"""
    parsed = minidom.parseString(svgdata).childNodes[0]
    # grey color
    parsed.setAttribute("fill", "#232629")
    # larger viewbox
    vb = [int(it) for it in parsed.getAttribute("viewBox").split()]
    margin = vb[2] // 10
    vb = [-margin, -margin, vb[2] + 2*margin, vb[3] + 2*margin]
    parsed.setAttribute("viewBox", " ".join([str(it) for it in vb]))
    return parsed.toxml()


def find_icons(name, theme):
    cands = []
    if theme in web_roots:  # download icons
        for topic in web_roots[theme]:
            filename = name + ".svg"
            url = web_roots[theme][topic]
            response = requests.get(url + filename)
            topic_dir = tmpdir / theme / topic
            topic_dir.mkdir(exist_ok=True, parents=True)
            if response.status_code == 200:
                path = topic_dir / filename
                svgdata = process_svg(response.content.decode())
                with path.open('w') as f:
                    f.write(svgdata)
                relp = path.parent.relative_to(tmpdir)
                cands.append([path, relp])
                break  # we only need one icon
    else:  # use local icons
        svgs = sorted((local_root / theme).rglob("{}.svg".format(name)))
        pngs = sorted((local_root / theme).rglob("{}.png".format(name)))
        for path in svgs + pngs:
            relp = path.parent.relative_to(local_root)
            cands.append([path, relp])
    return cands


if __name__ == "__main__":
    directories = []
    here = pathlib.Path(__file__).parent
    for theme in icons:
        for name in icons[theme]:
            ipaths = find_icons(name, theme)
            if not ipaths:
                print("Could not find {} {}".format(theme, name))
                continue
            for ipath, relp in ipaths:
                dest = here / relp
                directories.append(str(relp))
                dest.mkdir(exist_ok=True, parents=True)
                shutil.copy(ipath, dest)

    with (here / "index.theme").open("w") as fd:
        directories = sorted(set(directories))
        fd.write(index.format(directories=",".join(
            ["dcoraid"] + directories)))
        # Shape-Out icons
        fd.write(index_item.format(directory="dcoraid", res="16"))
        # theme icons
        for dd in directories:
            for res in ["16", "22", "24", "32", "64", "128"]:
                if res in str(dd):
                    break
            else:
                res = "64"
            fd.write(index_item.format(directory=dd, res=res))
