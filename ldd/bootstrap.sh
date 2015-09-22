#!/bin/bash

# init
apt-get update 2> /dev/null

# redis
apt-get install -y redis-server 2> /dev/null

# ntp
apt-get install ntp -y 2> /dev/null
service ntp restart

# install dependencies and services
apt-get install unzip -y 2> /dev/null
apt-get install -y vim curl 2> /dev/null
apt-get install git -y 2> /dev/null

# Python things
echo "Install python"
apt-get install -y build-essentials 2> /dev/null
apt-get install -y python-pip 2> /dev/null
apt-get install -y python-virtualenv 2> /dev/null
apt-get install -y python-dev 2> /dev/null
echo "install other things"
apt-get install -y libssl-dev libsqlite3-dev  libbz2-dev 2> /dev/null
apt-get install -y libffi-dev 2> /dev/null
wget -q https://www.python.org/ftp/python/3.4.3/Python-3.4.3.tgz
tar xfvz Python-3.4.3.tgz
cd Python-3.4.3/
./configure 2> /dev/null
make 2> /dev/null
sudo make install 2> /dev/null
rm /usr/bin/python3.4

# set vim tabs
cat <<EOF > /home/vagrant/.vimrc
set tabstop=4
EOF
chown vagrant.vagrant /home/vagrant/.vimrc

# install ldd
cd /home/vagrant
wget -q https://github.com/launchdarkly/ldd/releases/download/ca7092/ldd_linux_amd64.tar.gz
tar xfvz ldd_linux_amd64.tar.gz
cat <<EOF > /home/vagrant/ldd_linux_amd64/ldd.conf
[redis]
host = "localhost"
port = 6379

[main]
apiKey = "YOUR_API_KEY"
prefix = "launchdarkly"
streamUri = "http://localhost:8000"
EOF
cat <<EOF > /etc/init/ldd.conf
description     "Run LaunchDarkly Daemon"

# no start option as you might not want it to auto-start
# This might not be supported - you might need a: start on runlevel [3]
start on runlevel [2345] stop on runlevel [!2345]

# if you want it to automatically restart if it crashes, leave the next line in
respawn

script
    cd /home/vagrant/ldd_linux_amd64
    su -c "./ldd" vagrant
end script
EOF
service ldd restart
# install project node_modules
su - vagrant
cd /home/vagrant/project/ldd


virtualenv py2
py2/bin/pip install -U -r ../requirements.txt
py2/bin/pip install -U -r ../test-requirements.txt
py2/bin/pip install -U -r ../twisted-requirements.txt
py2/bin/pip install -U -r ../redis-requirements.txt

pyvenv py3
py3/bin/pip install -U -r ../requirements.txt
py3/bin/pip install -U -r ../test-requirements.txt
py3/bin/pip install -U -r ../redis-requirements.txt