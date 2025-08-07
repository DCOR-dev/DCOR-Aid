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
