************
Installation
************

First verify that you have Python 3.6+.

.. code:: bash

   python3 --version

If necessary, install it by your method of choice (apt, Homebrew, conda, etc.).

Facility-Specific Distributions
===============================

*We plan for facilities that rely on databroker to provide software
distributions that bundle databroker itself and specific Catalogs for their
users. We will list those distributions here. For users, this will be their
one-stop shop; they need read no further.*

TO DO

General Installation
====================

This provides a minimal installation that is sufficent for users who just want
to *access* data.

.. code:: bash

   python3 -m pip install -U databroker

Administrators and developers may require the optional dependencies as well,
which can be installed by:

.. code:: bash

   python3 -m pip install .[all]

Development Installation
========================

.. code:: bash

    git clone https://github.com/bluesky/databroker
    cd databroker
    pip install -e .

To install all the optional dependencies as well, use:

.. code:: bash

    pip install -e .[all]
