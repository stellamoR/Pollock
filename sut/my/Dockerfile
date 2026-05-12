FROM python:3.12-slim

WORKDIR /app
COPY ./sut/my/mysq.py ./
COPY ./sut/my/mysql-requirements.txt ./
COPY ./sut/utils.py ./

RUN pip install --no-cache-dir -r mysql-requirements.txt

CMD ["python3", "mysq.py"]
