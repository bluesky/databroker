"This is vendored from ophyd.sim"
import os

import numpy as np


class NumpySeqHandler:

    def __init__(self, filename, root=""):
        self._name = os.path.join(root, filename)

    def __call__(self, index):
        return np.load("{}_{}.npy".format(self._name, index), allow_pickle=False)

    def get_file_list(self, datum_kwarg_gen):
        "This method is optional. It is not needed for access, but for export."
        return [
            "{name}_{index}.npy".format(name=self._name, **kwargs)
            for kwargs in datum_kwarg_gen
        ]


class SESHandler:
    "Given Resource and Datum, return array."
    def __init__(self, resource_path):
        self._resource_path = resource_path

    def __call__(self):
        return np.loadtxt(self._resource_path)
