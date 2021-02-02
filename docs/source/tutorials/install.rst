Installation Tutorial
=====================

This tutorial covers

* Installation using conda
* Installation using pip
* Installation from source

Conda
-----

We strongly recommend creating a fresh environment.

.. code:: bash

   conda create -n try-databroker
   conda activate try-databroker

Install Databroker from the ``nsls2forge`` conda channel maintained by NSLS-II.

.. code:: bash

   conda install -c nsls2forge databroker

To follow the along with  the tutorials, you will also need
``databroker-pack``.


.. code:: bash

   conda install -c nsls2forge databroker-pack

Pip
---

We strongly recommend creating a fresh environment.

.. code:: bash

   python3 -m venv try-databroker
   source try-databroker/bin/activate

Install Databroker from PyPI.

.. code:: bash

   python3 -m pip install databroker

To follow the along with  the tutorials, you will also need
``databroker-pack``.

.. code:: bash

   python3 -m pip install databroker-pack


Source
------

To install an editable installation for local development:

.. code:: bash

   git clone https://github.com/bluesky/databroker
   cd databroker
   pip install -e .
