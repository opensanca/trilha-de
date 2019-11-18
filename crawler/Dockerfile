FROM python:3.7.5-stretch

RUN echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 6494C6D6997C215E
RUN apt update && apt install -y google-chrome-stable

COPY crawler.py /run/crawler.py
COPY requirements.txt /run/requirements.txt
RUN pip install -r /run/requirements.txt

COPY driver /run/driver
ENV PATH=/run/driver/:$PATH

WORKDIR /run
