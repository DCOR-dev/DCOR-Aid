def main(splash=True):
    from importlib import resources
    import logging
    import platform
    import sys

    from .loggers import setup_logging

    setup_logging("dcoraid")
    setup_logging("dclab")
    setup_logging("requests", level=logging.INFO)

    from PyQt6.QtWidgets import QApplication

    if platform.win32_ver()[0] == "7":
        # Use software OpenGL on Windows 7, because sometimes the
        # window content becomes plain white.
        # Not sure whether this actually works.
        from PyQt6.QtCore import Qt, QCoreApplication
        from PyQt6.QtGui import QGuiApplication
        QApplication.setAttribute(Qt.AA_UseSoftwareOpenGL)
        QCoreApplication.setAttribute(Qt.AA_UseSoftwareOpenGL)
        QGuiApplication.setAttribute(Qt.AA_UseSoftwareOpenGL)

    app = QApplication(sys.argv)

    if splash:
        from PyQt6.QtWidgets import QSplashScreen
        from PyQt6.QtGui import QPixmap
        from PyQt6.QtCore import QEventLoop
        ref_splash = resources.files("dcoraid.img") / "splash.png"
        with resources.as_file(ref_splash) as splash_path:
            splash_pix = QPixmap(str(splash_path))
        splash = QSplashScreen(splash_pix)
        splash.setMask(splash_pix.mask())
        splash.show()
        app.processEvents(QEventLoop.ProcessEventsFlag.AllEvents, 300)

    from PyQt6 import QtCore, QtGui
    from .gui import DCORAid

    # Set Application Icon
    ref_icon = resources.files("dcoraid.img") / "splash.png"
    with resources.as_file(ref_icon) as icon_path:
        app.setWindowIcon(QtGui.QIcon(str(icon_path)))

    # Use dots as decimal separators
    QtCore.QLocale.setDefault(QtCore.QLocale(QtCore.QLocale.Language.C))

    window = DCORAid()

    if splash:
        splash.finish(window)

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
