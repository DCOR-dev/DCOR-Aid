from __future__ import annotations

import pathlib

import time


from .._version import version


def get_payload_oxum(bag_path: pathlib.Path) -> str:
    """Return the Payload-Oxum

    The "octetstream sum" of the payload, which is
    intended for the purpose of quickly detecting incomplete bags
    before performing checksum validation.  This is strictly an
    optimization, and implementations MUST perform the standard
    checksum validation process before proclaiming a bag to be valid.
    This element MUST NOT be present more than once and, if present,
    MUST be in the form "_OctetCount_._StreamCount_", where
    _OctetCount_ is the total number of octets (8-bit bytes) across
    all payload file content and _StreamCount_ is the total number of
    payload files.  This metadata element MUST NOT be repeated.
    """
    num_files = 0
    size_files = 0
    for pp in (bag_path / "data").rglob("*"):
        if pp.is_file():
            num_files += 1
            size_files += pp.stat().st_size
    return f"{size_files}.{num_files}"


def write_bag_info(bag_path: pathlib.Path,
                   ds_dict: dict,
                   bag_index: int = 1,
                   num_bags: int = 1,
                   ):
    """Write the bag-info.txt file for a bag

    The content of the info file looks like this:

        Bag-Software-Agent: dcoraid 1.1 <https://github.com/DCOR-dev/DCOR-Aid>
        Bagging-Date: 2026-02-12
        Payload-Oxum: 2307.10
        Bag-Group-Identifier: Name of the Circle
        Bag-Count: 23 of 8472
        External-Identifier: DATASET IDENTIFIER
        Internal-Sender-Identifier: DATASET TITLE
    """
    webloc = "https://github.com/DCOR-dev/DCOR-Aid"
    lines = [
        f"Bag-Software-Agent: dcoraid {version} <{webloc}>",
        time.strftime("Bagging-Date: %Y-%m-%d"),
        f"Payload-Oxum: {get_payload_oxum(bag_path)}",
        f"Bag-Group-Identifier: {ds_dict['organization']['name']}",
        f"Bag-Count: {bag_index} of {num_bags}",
        f"External-Identifier: {ds_dict['name']}",
        f"Internal-Sender-Identifier: {ds_dict['title']}",
    ]
    (bag_path / "bag-info.txt").write_text("\n".join(lines),
                                           encoding="utf-8")
