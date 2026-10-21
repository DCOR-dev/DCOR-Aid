from dcoraid.dbmodel import DBExtract


def test_dbmodel_add_same():
    de = DBExtract()
    de.add_datasets([{"name": "peter", "id": "hans"}])
    assert len(de) == 1
    de.add_datasets([{"name": "peter", "id": "hans"}])
    assert len(de) == 1


def test_dbmodel_contains():
    de = DBExtract()
    de.add_datasets([{"name": "peter", "id": "hans"}])
    assert "peter" in de
    assert "hans" in de
    assert {"name": "peter", "id": "hans"} in de


def test_dbmodel_get_dataset_dict():
    de = DBExtract()
    de.add_datasets([{"name": "peter", "id": "hans"}])

    assert de.get_dataset_dict("peter") == {"name": "peter", "id": "hans"}
    assert de.get_dataset_dict("hans") == {"name": "peter", "id": "hans"}


def test_dbmodel_get_item():
    de = DBExtract()
    de.add_datasets([{"name": "peter", "id": "hans"}])
    de.add_datasets([{"name": "flink", "id": "flank"}])

    assert de["flink"] == {"name": "flink", "id": "flank"}
    assert de["flank"] == {"name": "flink", "id": "flank"}
    assert de["hans"] == {"name": "peter", "id": "hans"}
    assert de["peter"] == {"name": "peter", "id": "hans"}
    assert de[0] == {"name": "peter", "id": "hans"}
    assert de[1] == {"name": "flink", "id": "flank"}


def test_dbmodel_add():
    de1 = DBExtract([{"name": "peter", "id": "hans"}])
    de2 = DBExtract([{"name": "flink", "id": "flank"},
                     {"name": "peter", "id": "hans"}])
    de = de1 + de2
    assert len(de) == 2
    assert [d["id"] for d in de] == ["hans", "flank"]
    # the summands are not modified
    assert len(de1) == 1
    assert len(de2) == 2


def test_dbmodel_add_list():
    de1 = DBExtract([{"name": "peter", "id": "hans"}])
    de = de1 + [{"name": "flink", "id": "flank"},
                {"name": "peter", "id": "hans"}]
    assert isinstance(de, DBExtract)
    assert [d["id"] for d in de] == ["hans", "flank"]
    assert len(de1) == 1


def test_dbmodel_iadd_list():
    de = DBExtract([{"name": "peter", "id": "hans"}])
    de += [{"name": "flink", "id": "flank"}]
    assert [d["id"] for d in de] == ["hans", "flank"]
