import os
import pathlib


CIRCLE = "dcoraid-circle"
COLLECTION = "dcoraid-collection"
DATASET = "dcoraid-dataset"
SERVER = "dcor-dev.mpl.mpg.de"
USER = "dcoraid"
USER_NAME = "DCOR-Aid tester"
TITLE = "DCOR-Aid test dataset"


def get_api_key():
    key = os.environ.get("DCOR_API_KEY")
    if not key:
        # local file
        kp = pathlib.Path(__file__).parent / "api_key"
        if not kp.exists():
            raise ValueError("No DCOR_API_KEY variable or api_key file!")
        key = kp.read_text().strip()
    return key
