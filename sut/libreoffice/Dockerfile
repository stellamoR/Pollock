FROM ubuntu:22.04

WORKDIR /app
COPY ./sut/libreoffice/libreoffice.py ./
COPY ./sut/libreoffice/libreoffice-requirements.txt ./
COPY ./sut/utils.py ./

RUN apt-get update -y
RUN apt-get install -y libreoffice python3-pip
RUN pip install --no-cache-dir -r libreoffice-requirements.txt

CMD ["python3", "libreoffice.py"]