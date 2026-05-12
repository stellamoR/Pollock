FROM python:3.12-slim

WORKDIR /app
COPY ./sut/pycsv/pycsv.py ./
COPY ./sut/pycsv/pycsv-requirements.txt ./
COPY ./sut/utils.py ./

RUN pip install --no-cache-dir -r pycsv-requirements.txt
CMD ["python3", "pycsv.py"]
