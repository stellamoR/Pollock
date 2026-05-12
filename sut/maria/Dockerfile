FROM python:3.12-slim

WORKDIR /app
COPY ./sut/maria/mariad.py ./
COPY ./sut/maria/mariadb-requirements.txt ./
COPY ./sut/utils.py ./

RUN  apt-get update \
  && apt-get install -y wget curl gcc\
  && rm -rf /var/lib/apt/lists/*
RUN wget https://downloads.mariadb.com/MariaDB/mariadb_repo_setup
RUN chmod +x mariadb_repo_setup
RUN ./mariadb_repo_setup
RUN apt-get -y install libmariadb3 libmariadb-dev libmariadbclient-dev
RUN pip install --no-cache-dir -r mariadb-requirements.txt

CMD ["python3", "mariad.py"]
