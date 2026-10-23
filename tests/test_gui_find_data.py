"""Offline tests for the "Find data" panel"""
from unittest import mock

import pytest
from PyQt6 import QtCore

import dcoraid.gui.api
from dcoraid.gui.panel_find_data import widget_find_data


@pytest.fixture
def server_settings():
    """Use separate settings for testing and restore the original ones"""
    app_name = QtCore.QCoreApplication.applicationName()
    QtCore.QCoreApplication.setApplicationName("dcoraid-test-find-data")
    settings = QtCore.QSettings()
    settings.clear()
    dcoraid.gui.api._CKAN_API = None
    yield settings
    settings.clear()
    settings.sync()
    QtCore.QCoreApplication.setApplicationName(app_name)
    dcoraid.gui.api._CKAN_API = None


@pytest.mark.parametrize("server,url", [
    # The playground stores the server without scheme.
    ["dcor-dev.mpl.mpg.de", "https://dcor-dev.mpl.mpg.de"],
    # Local test environments might only support HTTP.
    ["http://192.168.122.143", "http://192.168.122.143"],
])
def test_find_data_title_uses_api_server(qtbot, server_settings, server,
                                         url):
    server_settings.setValue("auth/server", server)
    with mock.patch.object(widget_find_data.StatusWidget, "get_title",
                           return_value="DCOR Test") as get_title:
        widget = widget_find_data.WidgetFindData()
        qtbot.addWidget(widget)
    get_title.assert_called_once_with(url)
    assert widget.ui.label_search.text() == "Search DCOR Test"
