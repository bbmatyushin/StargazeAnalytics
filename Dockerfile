FROM python:3.8

WORKDIR /StargazeParser

COPY . .

RUN pip install -r requirements_parser.txt

CMD ["python3", "-u", "./parser_start.py"]
