def main(splash=True):
    from importlib import resources
    import logging
    import platform
    import sys

    from ._version import version

    logging.basicConfig(
        level=logging.DEBUG if version.count("post") else logging.INFO,
        format="%(asctime)s %(levelname)s %(processName)s/%(threadName)s "
               + "in %(name)s: %(message)s",
        datefmt='%H:%M:%S')

    from PyQt5.QtWidgets import QApplication

    if platform.win32_ver()[0] == "7":
        # Use software OpenGL on Windows 7, because sometimes the
        # window content becomes plain white.
        # Not sure whether this actually works.
        from PyQt5.QtCore import Qt, QCoreApplication
        from PyQt5.QtGui import QGuiApplication
        QApplication.setAttribute(Qt.AA_UseSoftwareOpenGL)
        QCoreApplication.setAttribute(Qt.AA_UseSoftwareOpenGL)
        QGuiApplication.setAttribute(Qt.AA_UseSoftwareOpenGL)

    app = QApplication(sys.argv)

    if splash:
        from PyQt5.QtWidgets import QSplashScreen
        from PyQt5.QtGui import QPixmap
        from PyQt5.QtCore import QEventLoop
        ref_splash = resources.files("dcoraid.img") / "splash.png"
        with resources.as_file(ref_splash) as splash_path:
            splash_pix = QPixmap(str(splash_path))
        splash = QSplashScreen(splash_pix)
        splash.setMask(splash_pix.mask())
        splash.show()
        app.processEvents(QEventLoop.AllEvents, 300)

    from PyQt5 import QtCore, QtGui
    from .gui import DCORAid

    # Set Application Icon
    ref_icon = resources.files("dcoraid.img") / "splash.png"
    with resources.as_file(ref_icon) as icon_path:
        app.setWindowIcon(QtGui.QIcon(str(icon_path)))

    # Use dots as decimal separators
    QtCore.QLocale.setDefault(QtCore.QLocale(QtCore.QLocale.C))

    window = DCORAid()

    if splash:
        splash.finish(window)

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
