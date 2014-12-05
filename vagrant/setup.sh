# Aptitude update
apt-get update

# Install Python tools
apt-get install -y python-pip

# Install python packages
pip install virtualenv virtualenvwrapper ipdb ipython

# Install OpenJDK
apt-get install -y openjdk-7-jdk

# Install Elasticsearch
if [[ ! -d /opt/elasticsearch ]]; then
    pushd /opt
    wget "https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.4.1.tar.gz"
    tar zxvf elasticsearch-1.4.1.tar.gz
    mv elasticsearch-1.4.1 elasticsearch
    popd
fi
