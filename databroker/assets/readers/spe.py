#
# files.py (c) Stuart B. Wilkins 2010
#
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# Part of the "pyspec" package
#
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six

import numpy
import time
from pims.base_frames import FramesSequence

__author__ = "Stuart B. Wilkins <stuwilkins@mac.com>"


class PrincetonSPEFile(FramesSequence):
    """Class to read SPE files from Princeton CCD cameras"""

    TEXTCOMMENTMAX = 80
    DATASTART = 4100
    NCOMMENTS = 5
    DATEMAX = 10
    TIMEMAX = 7

    def __init__(self, fname):
        """Initialize class.

        Parameters
        ----------
        fname : Filename of SPE file
        fid : File ID of open stream

        This function initializes the class and, if either a filename or fid is
        provided opens the datafile and reads the contents"""

        self._array = None
        self._fname = fname
        with open(fname, "rb") as fid:
            self.readData(fid)

    def __str__(self):
        """Provide a text representation of the file."""
        s = "Filename      : %s\n" % self._fname
        s += "Data size     : %d x %d x %d\n" % (self._shape[::-1])
        s += "CCD Chip Size : %d x %d\n" % self._chipSize[::-1]
        s += "File date     : %s\n" % time.asctime(self._filedate)
        s += "Exposure Time : %f\n" % self.Exposure
        s += "Num ROI       : %d\n" % self.NumROI
        s += "Num ROI Exp   : %d\n" % self.NumROIExperiment
        s += "Contoller Ver.: %d\n" % self.ControllerVersion
        s += "Logic Output  : %d\n" % self.LogicOutput
        # self.AppHiCapLowNoise = self._readInt(4)
        s += "Timing Mode   : %d\n" % self.TimingMode
        s += "Det. Temp     : %d\n" % self.DetTemperature
        s += "Det. Type     : %d\n" % self.DetectorType
        s += "Trigger Diode : %d\n" % self.TriggerDiode
        s += "Delay Time    : %d\n" % self.DelayTime
        s += "Shutter Cont. : %d\n" % self.ShutterControl
        s += "Absorb Live   : %d\n" % self.AbsorbLive
        s += "Absorb Mode   : %d\n" % self.AbsorbMode
        s += "Virtual Chip  : %d\n" % self.CanDoVirtualChip
        s += "Thresh. Min L : %d\n" % self.ThresholdMinLive
        s += "Thresh. Min   : %d\n" % self.ThresholdMin
        s += "Thresh. Max L : %d\n" % self.ThresholdMaxLive
        s += "Thresh. Max   : %d\n" % self.ThresholdMax
        s += "Geometric Op  : %d\n" % self.GeometricOps
        s += "ADC Offset    : %d\n" % self.ADCOffset
        s += "ADC Rate      : %d\n" % self.ADCRate
        s += "ADC Type      : %d\n" % self.ADCType
        s += "ADC Resol.    : %d\n" % self.ADCRes
        s += "ADC Bit. Adj. : %d\n" % self.ADCBitAdj
        s += "ADC Gain      : %d\n" % self.Gain

        i = 0
        for roi in self.allROI:
            s += "ROI %-4d      : %-5d %-5d %-5d %-5d %-5d %-5d\n" % (
                i, roi[0], roi[1], roi[2], roi[3], roi[4], roi[5])
            i += 1

        s += "\nComments :\n"
        i = 0
        for c in self._comments:
            s += "%-3d : " % i
            i += 1
            s += c
            s += "\n"
        return s

    def get_frame(self, n):
        """Return the array with zdimension n

        This method can be used to quickly obtain a 2-D array of the data"""
        return self._array[n]

    def __len__(self):
        return self._shape[0]

    @property
    def frame_shape(self):
        return self._shape[1:]

    @property
    def pixel_type(self):
        return self._array.dtype

    @classmethod
    def class_exts(cls):
        bc = super(PrincetonSPEFile, cls).class_ext()
        return bc | {'spe'}

    def getData(self):
        """Return the array of data"""
        return self._array

    def getBinnedData(self):
        """Return the binned (sum of all frames) data"""
        return self._array.sum(0)

    def readData(self, fid):
        """Read all the data into the class"""
        self._readHeader(fid)
        self._readSize(fid)
        self._readComments(fid)
        self._readAllROI(fid)
        self._readDate(fid)
        self._readArray(fid)

    def getSize(self):
        """Return a tuple of the size of the data array"""
        return self._shape

    def getChipSize(self):
        """Return a tuple of the size of the CCD chip"""
        return self._chipSize

    def getVirtualChipSize(self):
        """Return the virtual chip size"""
        return self._vChipSize

    def getComment(self, n=None):
        """Return the comments in the data file

        If n is not provided then all the comments are returned as a list of
        string values. If n is provided then the n'th comment is returned"""

        if n is None:
            return self._comments
        else:
            return self._comments[n]

    def _readAtNumpy(self, fid, pos, size, ntype):
        fid.seek(pos)
        return numpy.fromfile(fid, ntype, size)

    def _readAtString(self, fid, pos, size):
        fid.seek(pos)
        return fid.read(size).decode('ascii').rstrip(chr(0))

    def _readInt(self, fid, pos):
        return self._readAtNumpy(fid, pos, 1, numpy.int16)[0]

    def _readFloat(self, fid, pos):
        return self._readAtNumpy(fid, pos, 1, numpy.float32)[0]

    def _readHeader(self, fid):
        """This routine contains all other information"""
        self.ControllerVersion = self._readInt(fid, 0)
        self.LogicOutput = self._readInt(fid, 2)
        self.AppHiCapLowNoise = self._readInt(fid, 4)
        self.TimingMode = self._readInt(fid, 8)
        self.Exposure = self._readFloat(fid, 10)
        self.DetTemperature = self._readFloat(fid, 36)
        self.DetectorType = self._readInt(fid, 40)
        self.TriggerDiode = self._readInt(fid, 44)
        self.DelayTime = self._readFloat(fid, 46)
        self.ShutterControl = self._readInt(fid, 50)
        self.AbsorbLive = self._readInt(fid, 52)
        self.AbsorbMode = self._readInt(fid, 54)
        self.CanDoVirtualChip = self._readInt(fid, 56)
        self.ThresholdMinLive = self._readInt(fid, 58)
        self.ThresholdMin = self._readFloat(fid, 60)
        self.ThresholdMaxLive = self._readInt(fid, 64)
        self.ThresholdMax = self._readFloat(fid, 66)
        self.ADCOffset = self._readInt(fid, 188)
        self.ADCRate = self._readInt(fid, 190)
        self.ADCType = self._readInt(fid, 192)
        self.ADCRes = self._readInt(fid, 194)
        self.ADCBitAdj = self._readInt(fid, 196)
        self.Gain = self._readInt(fid, 198)
        self.GeometricOps = self._readInt(fid, 600)

    def _readAllROI(self, fid):
        self.allROI = self._readAtNumpy(fid, 1512, 60,
                                        numpy.int16).reshape(-1, 6)
        self.NumROI = self._readAtNumpy(fid, 1510, 1, numpy.int16)[0]
        self.NumROIExperiment = self._readAtNumpy(fid, 1488, 1, numpy.int16)[0]
        if self.NumROI == 0:
            self.NumROI = 1
        if self.NumROIExperiment == 0:
            self.NumROIExperiment = 1

    def _readDate(self, fid):
        _date = self._readAtString(fid, 20, self.DATEMAX)
        _time = self._readAtString(fid, 172, self.TIMEMAX)
        self._filedate = time.strptime(_date + _time, "%d%b%Y%H%M%S")

    def _readSize(self, fid):
        xdim = self._readAtNumpy(fid, 42, 1, numpy.int16)[0]
        ydim = self._readAtNumpy(fid, 656, 1, numpy.int16)[0]
        zdim = self._readAtNumpy(fid, 1446, 1, numpy.uint32)[0]
        dxdim = self._readAtNumpy(fid, 6, 1, numpy.int16)[0]
        dydim = self._readAtNumpy(fid, 18, 1, numpy.int16)[0]
        vxdim = self._readAtNumpy(fid, 14, 1, numpy.int16)[0]
        vydim = self._readAtNumpy(fid, 16, 1, numpy.int16)[0]
        dt = numpy.int16(self._readAtNumpy(fid, 108, 1, numpy.int16)[0])
        data_types = (numpy.float32, numpy.int32, numpy.int16, numpy.uint16)
        if (dt > 3) or (dt < 0):
            raise Exception("Unknown data type")
        self._dataType = data_types[dt]
        self._shape = (zdim, ydim, xdim)
        self._chipSize = (dydim, dxdim)
        self._vChipSize = (vydim, vxdim)

    def _readComments(self, fid):
        self._comments = []
        for n in range(5):
            self._comments.append(
                self._readAtString(fid, 200 + (n * self.TEXTCOMMENTMAX),
                                   self.TEXTCOMMENTMAX))

    def _readArray(self, fid):
        fid.seek(self.DATASTART)
        in_array = numpy.fromfile(fid, dtype=self._dataType, count=-1)
        self._array = in_array.reshape(self._shape)
