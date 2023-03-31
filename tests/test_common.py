import pathlib
import tempfile

import dcoraid.common


def test_sha256sum():
    p = pathlib.Path(tempfile.mkdtemp()) / "test.txt"
    p.write_text("Sum this up!")
    ist = dcoraid.common.sha256sum(p)
    soll = "d00df55b97a60c78bbb137540e1b60647a5e6b216262a95ab96cafd4519bcf6a"
    assert ist == soll
