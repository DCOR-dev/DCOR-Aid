from dcoraid.gui.main import DCORAid


def test_simple(qtbot):
    """Open the main window and close it again"""
    main_window = DCORAid()
    main_window.close()
