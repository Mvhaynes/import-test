FROM python:3.8

ADD main.py config.py .

RUN pip install requests

CMD [ "python", "./main.py" ]

