# This Dockerfile is used to build ROBOKOP

FROM python:3.6.6-stretch


## Install basic tools
RUN apt-get update
RUN apt-get install -yq \
    vim

## Install asciidoctor for building guides from ascii docs
RUN apt-get install -yq asciidoctor
#ascii doctor ruby dependency 
RUN gem install tilt

## Set up home directory
ARG UID=1000
ARG GID=1000
RUN groupadd -g $GID murphy
RUN useradd -m -u $UID -g $GID -s /bin/bash murphy
WORKDIR /home/murphy


COPY  ./startup.sh .
COPY  ./requirements.txt .

 

## Install all requirements

RUN pip install -r ./requirements.txt --src /usr/local/src

RUN git clone https://github.com/neo4j-contrib/neo4j-guides.git  neo4j-guides


WORKDIR /home/murphy/robokopkg




## Finish up
ENV HOME=/home/murphy
ENV USER=murphy

ENTRYPOINT bash ../startup.sh


