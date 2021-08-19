import pathlib

import pytest

from dcoraid.upload import dataset
from dcoraid.api import APIConflictError, APINotFoundError

import common


dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"


def test_dataset_create_same_resource():
    """There should be an error when a resource is added twice"""
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="create-with-same-resource")
    # post dataset creation request
    data = dataset.create_dataset(dataset_dict=dataset_dict,
                                  api=api)
    dataset.add_resource(dataset_id=data["id"],
                         path=dpath,
                         api=api
                         )
    with pytest.raises(APIConflictError):
        # Should not be able to upload same resource twice
        dataset.add_resource(dataset_id=data["id"],
                             path=dpath,
                             exist_ok=False,
                             api=api
                             )


def test_dataset_create_same_resource_exist_ok():
    """There should be an error when a resource is added twice"""
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="create-with-same-resource")
    # post dataset creation request
    data = dataset.create_dataset(dataset_dict=dataset_dict,
                                  api=api)
    dataset.add_resource(dataset_id=data["id"],
                         path=dpath,
                         api=api
                         )

    # Should not be able to upload same resource twice
    dataset.add_resource(dataset_id=data["id"],
                         path=dpath,
                         exist_ok=True,
                         api=api,
                         resource_dict={"sp:chip:channel width": 21.0}
                         )

    pkg_dict = api.get("package_show", id=data["id"])
    res_names = [r["name"] for r in pkg_dict["resources"]]
    idres = res_names.index(dpath.name)
    assert len(pkg_dict["resources"]) == 1
    assert pkg_dict["resources"][idres]["sp:chip:channel width"] == 21.0


def test_dataset_creation():
    """Just test whether we can create (and remove) a draft dataset"""
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="basic_test")
    # post dataset creation request
    data = dataset.create_dataset(dataset_dict=dataset_dict,
                                  api=api,
                                  )
    # simple test
    assert "authors" in data
    assert data["authors"] == common.USER_NAME
    assert data["state"] == "draft"
    # remove draft dataset
    dataset.remove_draft(dataset_id=data["id"],
                         api=api,
                         )
    with pytest.raises(APINotFoundError):
        # make sure it is gone
        api.get("package_show", id=data["id"])


def test_dataset_creation_wrong_resource_supplement():
    """Pass an invalid resource supplement and see if it fails"""
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="basic_test")
    # post dataset creation request
    data = dataset.create_dataset(dataset_dict=dataset_dict,
                                  api=api,
                                  )
    # simple test
    with pytest.raises(APIConflictError):
        dataset.add_resource(dataset_id=data["id"],
                             resource_dict={
                                 "sp:chip:production date": "2020-15-31"},
                             path=dpath,
                             api=api
                             )


def test_dataset_resource_exists():
    """There should be an error when a resource is added twice"""
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="create-with-same-resource")
    # post dataset creation request
    data = dataset.create_dataset(dataset_dict=dataset_dict,
                                  api=api)
    dataset.add_resource(dataset_id=data["id"],
                         path=dpath,
                         api=api
                         )

    assert dataset.resource_exists(dataset_id=data["id"],
                                   resource_name=dpath.name,
                                   api=api)

    assert not dataset.resource_exists(dataset_id=data["id"],
                                       resource_name=dpath.name,
                                       resource_dict={
                                           "sp:chip:channel width": 21.0
                                       },
                                       api=api)


def test_dataset_resource_exists2():
    """There should be an error when a resource is added twice"""
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="create-with-same-resource")
    # post dataset creation request
    data = dataset.create_dataset(dataset_dict=dataset_dict,
                                  api=api)
    dataset.add_resource(dataset_id=data["id"],
                         path=dpath,
                         resource_dict={"sp:chip:channel width": 21.0},
                         api=api,
                         )

    assert dataset.resource_exists(dataset_id=data["id"],
                                   resource_name=dpath.name,
                                   resource_dict={
                                       "sp:chip:channel width": 21.0
                                   },
                                   api=api)


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
