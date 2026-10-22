"""Offline tests for `dcoraid.gui.api`"""
from unittest import mock

import pytest
import requests.exceptions
from PyQt6 import QtCore

import dcoraid.gui.api
from dcoraid.gui.api import ensure_playground_user


@pytest.fixture
def playground_settings():
    """Use separate settings for testing and restore the original ones"""
    app_name = QtCore.QCoreApplication.applicationName()
    QtCore.QCoreApplication.setApplicationName("dcoraid-test-playground")
    settings = QtCore.QSettings()
    settings.clear()
    settings.setValue("user scenario", "dcor-dev")
    settings.setValue("auth/server", "dcor-dev.example.com")
    settings.setValue("auth/api key", "old-key")
    dcoraid.gui.api._CKAN_API = "cached-api"
    yield settings
    settings.clear()
    settings.sync()
    QtCore.QCoreApplication.setApplicationName(app_name)
    dcoraid.gui.api._CKAN_API = None


def test_playground_user_invalid_key(playground_settings):
    with mock.patch.object(dcoraid.gui.api, "is_valid_api_key",
                           return_value=False) as valid, \
            mock.patch.object(dcoraid.gui.api, "create_dcor_dev_user",
                              return_value="new-key") as create:
        assert ensure_playground_user()
    valid.assert_called_once()
    assert valid.call_args.kwargs["server"] == "dcor-dev.example.com"
    assert valid.call_args.kwargs["api_key"] == "old-key"
    create.assert_called_once()
    assert create.call_args.kwargs["server"] == "dcor-dev.example.com"
    assert playground_settings.value("auth/api key") == "new-key"
    # the cached CKANAPI instance with the old key must not be used
    assert dcoraid.gui.api._CKAN_API is None


def test_playground_user_valid_key(playground_settings):
    with mock.patch.object(dcoraid.gui.api, "is_valid_api_key",
                           return_value=True), \
            mock.patch.object(dcoraid.gui.api,
                              "create_dcor_dev_user") as create:
        assert not ensure_playground_user()
    create.assert_not_called()
    assert playground_settings.value("auth/api key") == "old-key"


@pytest.mark.parametrize("scenario,api_key", [
    ["dcor", "old-key"],  # not the playground
    ["medical", "old-key"],
    ["anonymous", ""],
    ["dcor-dev", ""],  # not set up yet (setup wizard does that)
])
def test_playground_user_not_applicable(playground_settings, scenario,
                                        api_key):
    playground_settings.setValue("user scenario", scenario)
    playground_settings.setValue("auth/api key", api_key)
    with mock.patch.object(dcoraid.gui.api, "is_valid_api_key") as valid, \
            mock.patch.object(dcoraid.gui.api,
                              "create_dcor_dev_user") as create:
        assert not ensure_playground_user()
    valid.assert_not_called()
    create.assert_not_called()


def test_playground_user_no_connection(playground_settings):
    """Do not create a new user if the server cannot be reached"""
    error = requests.exceptions.ConnectionError
    with mock.patch.object(dcoraid.gui.api, "is_valid_api_key",
                           side_effect=error), \
            mock.patch.object(dcoraid.gui.api,
                              "create_dcor_dev_user") as create, \
            pytest.raises(error):
        ensure_playground_user()
    create.assert_not_called()
    assert playground_settings.value("auth/api key") == "old-key"
