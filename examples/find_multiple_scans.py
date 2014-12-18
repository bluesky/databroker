# this currently works at CSX 2014-12-18

# coding: utf-8

# In[1]:

# ton of imports
from databroker.api.analysis import as_object, find_last, find2
import six
from matplotlib import pyplot as plt
get_ipython().magic(u'matplotlib inline')
import matplotlib as mpl
from collections import OrderedDict
import numpy as np
import pandas as pd


# In[2]:

# create the peak stats function
def estimate(x, y):
    x = np.asarray(x)
    y = np.asarray(y)
    """Return a dictionary of the vital stats of a 'peak'"""
    stats = OrderedDict()

    fn = lambda num: '{:.5}'.format(num)
    # Center of peak
    stats['ymin'] = y.min()
    stats['ymax'] = y.max()
    stats['avg_y'] = fn(np.average(y))
    stats['x_at_ymin'] = x[y.argmin()]
    stats['x_at_ymax'] = x[y.argmax()]

    # Calculate CEN from derivative
    zero_cross = np.where(np.diff(np.sign(y - (stats['ymax'] + stats['ymin'])/2)))[0]
    if zero_cross.size == 2:
        stats['cen'] = (fn(x[zero_cross].sum() / 2),
                        fn((stats['ymax'] + stats['ymin'])/2))
    elif zero_cross.size == 1:
        stats['cen'] = x[zero_cross[0]]
    if zero_cross.size == 2:
        fwhm = x[zero_cross]
        stats['width'] = fwhm[1] - fwhm[0]
        stats['fwhm_left'] = (fn(fwhm[0]), fn(y[zero_cross[0]]))
        stats['fwhm_right'] = (fn(fwhm[1]), fn(y[zero_cross[1]]))

    # Center of mass
    stats['center_of_mass'] = fn((x * y).sum() / y.sum())
    return stats


# In[3]:

# do some plotting of a single scal
scan = 2037
data = find2(scan_id=scan, data=True)
data_obj = as_object(data['events'])
plt.scatter(data_obj.energy, data_obj.sclr_ch5)


# In[4]:

# grab information for multiple scans that have the same data keys
scans = [1111, 2037, 2043]
data = {scan: find2(scan_id=scan, data=True) for scan in scans}
data_objs = {scan: as_object(data['events']) for scan, data in six.iteritems(data)}


# In[5]:

# plot the data
styles = ['ro', 'bo', 'ko']
for (scan, data), style in zip(six.iteritems(data_objs), styles):
    plt.plot(data.energy, data.sclr_ch5, style, label=scan)
plt.legend()


# In[6]:

# compute and show the peak stats
stats = {scan: estimate(data.energy, data.sclr_ch5) for scan, data in six.iteritems(data_objs)}
df = pd.DataFrame(stats)
df
