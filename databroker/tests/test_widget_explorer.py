from nose.tools import (assert_equal, assert_raises, assert_true,
                        assert_false, raises)

from databroker import
def test_widget():
    fig, ax = plt.subplots()


# Run the metadatastore cosine example 4 times so that we have scan ids 1-4
# Input >> scan ids [1-5]
# Validate >> that the scan_text widget has only the scan ids that exist in
#             the databroker
# Select >> Scan ids 2-4 in the scan_select widget
# Select >> Plotting keys: All
# Validate >> that the x axis and y axis have the same number of keys
# Validate >> that the x axis and y axis have the *correct* keys
# Select >> Plotting keys: Intersection
# Validate >> that the x axis and y axis have the same number of keys
# Validate >> that the x axis and y axis have the *correct* keys
# Select >> linear_motor as the x axis
# Select >> Tsam, scalar_detector as the y-axes
# Validate >> that the matplotlib axes object has two lines
# Validate >> that the **names** of the matplotlib lines are
#             "scan_id, data_key_name". Not sure how to do this?
# Select >> time as the y-axis
# Validate >> that the matplotlib axes object has one line
# Validate >> the **name** of the matplotlib line is "scan_id, data_key_name"
