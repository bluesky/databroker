#!/bin/bash
apt-get install sysv-rc-softioc
conda create -p /opt/conda_envs/services python=3.5
source activate services
conda install pymongo ujson tornado jsonschema yaml pytz doct
pip install requests
mkdir /services
cd /services
mkdir -p /epics/iocs/
cd /services/
git clone https://github.com/NSLS-II/metadataservice
python setup.py develop
mkdir -p /epics/iocs/metadataservice/
cd /epics/iocs/metadataservice


echo "NAME=metadataservice
PORT=4052
HOST=$HOSTNAME
USER=tornado" > config
echo '#!/bin/bash
export PATH=/opt/conda/bin:$PATH
source activate services
python /services/metadataservice/startup.py --mongo-host=localhost --mongo-port=27017 --database=datastore --service-port=7772 --timezone=US/Eastern' > launch.sh
ln -s -T launch.sh st.cmd

chown -R tornado:tornado /epics/iocs/metadataservice

manage-iocs install metadataservice
manage-iocs enable metadataservice
/etc/init.d/softioc-metadataservice start
