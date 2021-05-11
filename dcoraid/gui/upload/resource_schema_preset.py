import json
import pathlib

from PyQt5.QtCore import QStandardPaths


class PersistentResourceSchemaPresets(object):
    def __init__(self):
        """Dict-like interface for schema presets for resource uploads"""
        self._presets = {}
        # This is a roaming path on Windows
        self.path = pathlib.Path(QStandardPaths.writableLocation(
            QStandardPaths.AppDataLocation)) / "upload_resource_schema_presets"
        self.path.mkdir(exist_ok=True, parents=True)
        for pp in self.path.glob("*.json"):
            with pp.open() as fd:
                self._presets[pp.stem] = json.load(fd)

    def __contains__(self, key):
        return self._presets.__contains__(key)

    def __getitem__(self, key):
        return self._presets[key]

    def __setitem__(self, key, value):
        self._presets[key] = value
        with (self.path / "{}.json".format(key)).open("w") as fd:
            json.dump(value, fd, indent=2)

    def keys(self):
        return self._presets.keys()
