FROM python:3.8

WORKDIR /StargazeParser

COPY ./database/db_* ./database/
COPY ./parser/ ./parser/
COPY ./parser_start.py .
COPY ./requirements_parser.txt .

RUN pip install -r requirements_parser.txt
RUN mkdir ./database/_db
RUN touch ./database/_db/{mints.db,owners_tokens.db,stargaze_analytics.db}

CMD ["python3", "-u", "./parser_start.py"]
