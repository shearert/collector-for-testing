FROM python:latest

LABEL maintainer Ilija Vukotic <ivukotic@cern.ch>

RUN apt-get update && apt-get install -y \
    vim 

RUN mkdir -p /usr/local/sbin
COPY src/collector.py /usr/local/sbin/
COPY requirements.txt requirements.txt
COPY run.sh run.sh
RUN python3 -m pip install -r requirements.txt

# build info
RUN echo "Timestamp:" `date --utc` | tee /image-build-info.txt

CMD ["./run.sh"]
