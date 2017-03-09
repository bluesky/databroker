##important bits
- `filestore` does not manage files on disk for you.
- `filestore` relies on you (the user!) to know the mapping between your file
  spec and the handler that can open it. If you do not have the handler that
  you need, you must write it yourself!

##Using filestore with pickled numpy files `(*.npy)`

In this example you will see

1. Creating some synthetic data (a noisy sine)
2. Writing it to disk
3. Inserting some information about the files I wrote to disk into filestore
4. Loading it back from disk with filestore
5. Ploting it with matplotlib

**It is important to note that only points 4 and 5 are what filestore is
written to do.**

If you would like to see this example as a jupyter notebook then [go here]
(http://nbviewer.ipython.org/bbcd99efa5faac57972c)

Otherwise, have a look at this script. The code between the two lines of
hashes (####) indicate the parts of this script that are actually relevant to
the operation of filestore. The rest is data generation and plotting.

```python
#### Set up the example

import matplotlib.pyplot as plt
import numpy as np
import tempfile
import uuid
import os

#### create the synthetic data ### (not filestore's job!)
x = np.linspace(0, 10, 10000)
y = np.sin(x) + np.random.random(x.shape)*.1
fig, axs = plt.subplots(2)
axs[0].plot(x, y)
axs[0].set_title('data before going in to filestore')

#### save the data to disk #### (Still not part of filestore's job!)
data_dir = tempfile.gettempdir()
# generate a totally random name for the file to ensure that there are no
# collisions
x_datapath = os.path.join(data_dir, str(uuid.uuid4()) + '.npy')
y_datapath = os.path.join(data_dir, str(uuid.uuid4()) + '.npy')

np.save(x_datapath, x)
np.save(y_datapath, y)

print('x data path = %s' % x_datapath)
print('y data path = %s' % y_datapath)

x_uid = str(uuid.uuid4())
y_uid = str(uuid.uuid4())

###################################################################
###################################################################

# Insert the filestore records that know about these files
import filestore.api as fsapi
from filestore.handlers import NpyHandler
# spec is the identifier that will be used later to link
spec = 'npy'
datum_kwargs = {}

# insert the records into filestore for the x data set
resource_document = fsapi.insert_resource(spec, x_datapath)
datum_document = fsapi.insert_datum(resource_document, x_uid, datum_kwargs)

# insert the records into filestore for the y data set
resource_document = fsapi.insert_resource(spec, y_datapath)
datum_document = fsapi.insert_datum(resource_document, y_uid, datum_kwargs)

# Retrieve data from filestore
# Here is where the payoff happens for using this framework
# Because all you have to keep track of is the uid for the
# datum document and not the filepath.  Client code can
# then keep track of this uid and use the retrieve api to
# get the contents of the file back in ram

# Make sure that the 'npy' spec has a registered handler
fsapi.register_handler('npy', NpyHandler)
# use the retrieve function to get the data back in ram
x_from_filestore = fsapi.retrieve(x_uid)
y_from_filestore = fsapi.retrieve(y_uid)

###################################################################
###################################################################

### show that the data is the same
axs[1].plot(x_from_filestore, y_from_filestore)
axs[1].set_title('data after coming out of filestore')

print('difference between x before and after it went into filestore = '
      '{}'.format(np.sum(x - x_from_filestore)))
print('difference between y before and after it went into filestore = '
      '{}'.format(np.sum(y - y_from_filestore)))
plt.show()
```

##Using filestore with other filetypes
For retrieving from more complex file types (looking at you hdf!), see some
of the built-in writers that are shipped with filestore in `filestore.handlers`
