class SQLiteBackedROListOfDicts:
    def __init__(self, sqlite_dbs, map_index_id, map_id_org):
        """Read-only access to DCOR-Aid dataset databases"""
        self._sqlite_dbs = sqlite_dbs
        self._map_index_id = map_index_id
        self._map_id_org = map_id_org

    def __eq__(self, other):
        if isinstance(other, list):
            for ii in range(len(other)):
                if not self[ii] == other[ii]:
                    return False
            else:
                return True
        else:
            raise NotImplementedError(f"Cannot compare '{type(self)}' with "
                                      f"'{type(other)}'.")

    def __getitem__(self, idx):
        ds_id = self._map_index_id[idx]
        org = self._map_id_org[ds_id]
        return self._sqlite_dbs[org][ds_id]

    def __iter__(self):
        for ds_id in self._map_index_id:
            org = self._map_id_org[ds_id]
            yield self._sqlite_dbs[org][ds_id]

    def __len__(self):
        return len(self._map_index_id)
