# Use lambdalinux/baseimage-amzn as base image.
# See https://hub.docker.com/r/lambdalinux/baseimage-amzn/tags/ for
# a list of version numbers.
#FROM amazonlinux:latest
#FROM python:3.8
FROM amazonlinux:latest

# update the environment tools
RUN yum -y update 
RUN yum -y groupinstall "Development Tools"
RUN amazon-linux-extras install python3.8
#RUN yum -y install openssl-devel bzip2-devel libffi-devel
#RUN yum -y install wget
#RUN wget https://www.python.org/ftp/python/3.8.3/Python-3.8.3.tgz
#RUN tar xvf Python-3.8.3.tgz
#RUN cd Python-3.8.3/
#RUN configure --enable-optimizations
#RUN make altinstall

# install AWS
RUN pip3.8 install awscli \
    boto3

# Install softwae to /opt/stocks
RUN mkdir /opt/stocks && \
    mkdir /opt/stocks/aws && \
    mkdir /opt/stocks/database

ADD *.py /opt/stocks/ 
ADD ./aws/*.py /opt/stocks/aws/ 
ADD ./database/*.py /opt/stocks/database/
ENV STOCK_DIR="/opt/stocks"
ENV STOCK_EXEC_SCRIPT="/opt/stocks/PriceTargetLaunch.py"
ADD run.sh /usr/local/bin/run.sh
RUN chmod +x /usr/local/bin/run.sh

ADD requirements.txt /opt/stocks/requirements.txt
RUN pip3.8 install -r /opt/stocks/requirements.txt
RUN rm /opt/stocks/requirements.txt

ENTRYPOINT ["/usr/local/bin/run.sh"]



