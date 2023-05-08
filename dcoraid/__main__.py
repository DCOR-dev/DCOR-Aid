def main(splash=True):
    import logging
    import os
    import pkg_resources
    import platform
    import sys

    logging.basicConfig(
        level=logging.INFO,
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
    imdir = pkg_resources.resource_filename("dcoraid", "img")

    if splash:
        from PyQt5.QtWidgets import QSplashScreen
        from PyQt5.QtGui import QPixmap
        from PyQt5.QtCore import QEventLoop
        splash_path = os.path.join(imdir, "splash.png")
        splash_pix = QPixmap(splash_path)
        splash = QSplashScreen(splash_pix)
        splash.setMask(splash_pix.mask())
        splash.show()
        app.processEvents(QEventLoop.AllEvents, 300)

    from PyQt5 import QtCore, QtGui
    from .gui import DCORAid

    # Set Application Icon
    icon_path = os.path.join(imdir, "icon.png")
    app.setWindowIcon(QtGui.QIcon(icon_path))

    # Use dots as decimal separators
    QtCore.QLocale.setDefault(QtCore.QLocale(QtCore.QLocale.C))

    window = DCORAid()

    if splash:
        splash.finish(window)

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
