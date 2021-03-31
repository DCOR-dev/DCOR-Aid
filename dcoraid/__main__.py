def main(splash=True):
    import os
    import pkg_resources
    import sys

    from PyQt5.QtWidgets import QApplication

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

    import warnings
    from requests.packages.urllib3.exceptions import SubjectAltNameWarning
    # Ignore SubjectAltNameWarning for certificates in medical branding,
    # because they will show up in the dclab-compress-warnings log.
    warnings.filterwarnings("ignore", category=SubjectAltNameWarning)

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
