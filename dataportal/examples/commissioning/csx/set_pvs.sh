#!/bin/bash
caput -S XF:23ID-CT{Replay}Val:0-I XF:23ID1-OP{Mono}Enrgy-I
caput -S XF:23ID-CT{Replay}Val:1-I XF:23ID1-ES{Dif-Cam:Beam}Stats5:Total_RBV
caput -S XF:23ID-CT{Replay}Val:2-I XF:23ID1-ES{Sclr:1}_cts1.H
caput -S XF:23ID-CT{Replay}Val:3-I XF:23ID1-ES{Sclr:1}_cts1.I
caput -S XF:23ID-CT{Replay}Val:4-I XF:23ID1-ES{Sclr:1}_cts1.J
caput -S XF:23ID-CT{Replay}Val:5-I XF:23ID1-ES{Sclr:1}_cts2.G
caput -S XF:23ID-CT{Replay}Val:6-I XF:23ID1-ES{Dif-Cam:PIMTE}image1:ArrayData
caput -S XF:23ID-CT{Replay}Val:7-I XF:23ID1-BI{Diag:6-Cam:1}image1:ArrayData
caput -S XF:23ID-CT{Replay}Val:8-I XF:23IDA-BI:1{FS:1-Cam:1}image1:ArrayData
caput -S XF:23ID-CT{Replay}Val:9-I XF:23ID1-ES{Dif-Cam:Beam}image1:ArrayData