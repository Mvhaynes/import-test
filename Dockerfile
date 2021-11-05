FROM python:3.8

ADD main.py .

RUN pip install requests

CMD [ "python", "./main.py" ]

