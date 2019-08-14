# This Dockerfile is used to build ROBOKOP

FROM python:3.6.6-stretch

LABEL maintainer="patrick@covar.com"
ENV REFRESHED_AT 2018-05-03

ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

## Install basic tools
RUN apt-get update
RUN apt-get install -yq \
    vim

## Set up home directory
ARG UID=1000
ARG GID=1000
RUN groupadd -o -g $GID murphy
RUN useradd -m -u $UID -g $GID -s /bin/bash murphy
WORKDIR /home/murphy

## Get ROBOKOP software
RUN git clone https://github.com/NCATS-Gamma/robokop-interfaces.git

WORKDIR /home/murphy/robokop-interfaces

## Install all requirements
RUN pip install -r ./requirements.txt --src /usr/local/src

## Finish up
ENV HOME=/home/murphy
ENV USER=murphy

WORKDIR /home/murphy
RUN rm -r /home/murphy/robokop-interfaces

ENTRYPOINT ["./robokop-interfaces/deploy/startup.sh"]
CMD ["supervisord", "-c", "./robokop-interfaces/deploy/supervisord.conf"]

# CMD gets called only if you call `docker run` without extra arguments

# to look at things and do stuff:
# * docker exec -it robokop bash
#   - supervisorctl
#   - celery
#   - kill `cat ../supervisord.pid`
