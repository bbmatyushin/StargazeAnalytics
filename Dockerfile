FROM python:3.8

WORKDIR /StargazeParser

COPY ./database/db_* ./database/
COPY ./parser/ ./parser/
COPY ./parser_start.py .
COPY ./requirements_parser.txt .

RUN pip install -r requirements_parser.txt

CMD ["python3", "-u", "./parser_start.py"]
