#!/bin/bash
caput -S XF:23ID-CT{Replay}Val:0-I XF:23ID1-OP{Slt:3-Ax:X}Mtr.RBV
caput -S XF:23ID-CT{Replay}Val:1-I XF:23ID1-ES{Dif-Cam:Beam}Stats5:Total_RBV
caput -S XF:23ID-CT{Replay}Val:2-I XF:23ID1-OP{Slt:3-Ax;X}Mtr.RBV
caput -S XF:23ID-CT{Replay}Val:9-I XF:23ID1-ES{Dif-Cam:Beam}image1:ArrayData
