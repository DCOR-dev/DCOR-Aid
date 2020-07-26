|DCOR-Manager|
==============

|PyPI Version| |Tests Status Linux| |Tests Status Win| |Coverage Status| |Docs Status|


DCOR-Manager is a GUI for managing data on DCOR (https://dcor.mpl.mpg.de).


Installation
------------
Installers for Windows and macOS are available at the `release page <https://github.com/DCOR-dev/DCOR-Manager/releases>`__.

If you have Python 3 installed, you can install DCOR-Manager with

::

    pip install dcor_manager


Citing DCOR-Manager
-------------------
Please cite DCOR-Manager either in-line

::

  (...) using the software DCOR-Manager version X.X.X (available at
  https://github.com/DCOR-dev/DCOR-Manager).

or in a bibliography

::

  Paul Müller and others (2019), DCOR-Manager version X.X.X [Software].
  Available at https://github.com/DCOR-dev/DCOR-Manager.

and replace ``X.X.X`` with the version of DCOR-Manager that you used.


Testing
-------

::

    pip install -e .
    python setup.py test
    

.. |DCOR-Manager| image:: https://raw.github.com/DCOR-dev/DCOR-Manager/master/dcor_manager/img/dcor_manager_text.png
.. |PyPI Version| image:: https://img.shields.io/pypi/v/dcor_manager.svg
   :target: https://pypi.python.org/pypi/DCOR-Manager
.. |Tests Status Linux| image:: https://img.shields.io/travis/DCOR-dev/DCOR-Manager.svg?label=tests_linux
   :target: https://travis-ci.com/DCOR-dev/DCOR-Manager
.. |Tests Status Win| image:: https://img.shields.io/appveyor/ci/paulmueller/DCOR-Manager/master.svg?label=tests_win
   :target: https://ci.appveyor.com/project/paulmueller/DCOR-Manager
.. |Coverage Status| image:: https://img.shields.io/codecov/c/github/DCOR-dev/DCOR-Manager/master.svg
   :target: https://codecov.io/gh/DCOR-dev/DCOR-Manager
.. |Docs Status| image:: https://readthedocs.org/projects/DCOR-Manager/badge/?version=latest
   :target: https://readthedocs.org/projects/DCOR-Manager/builds/
