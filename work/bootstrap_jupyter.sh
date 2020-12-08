#!/bin/bash

set -ex

# update/upgrade and install htop
sudo apt-get update -y && sudo apt-get upgrade -y
sudo apt-get install htop

# install required python packages
python3 -m pip install --user --upgrade pip
python3 -m pip install -r requirements.txt --upgrade
jupyter nbextension enable --py widgetsnbextension --sys-prefix

# spark-submit logging level from INFO to WARN
sudo cp log4j.properties /usr/local/spark/conf/log4j.properties