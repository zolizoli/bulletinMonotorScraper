FROM selenium/standalone-firefox
FROM python:3.10
WORKDIR /code
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
# Get geckodriver
COPY ./app /code/app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80", "--log-config", "app/log.ini"]
