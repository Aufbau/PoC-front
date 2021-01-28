FROM python:3.8.7-alpine3.12
WORKDIR /app
ADD requirements.txt ./
RUN pip install -r requirements.txt
ADD . .
ENV FLASK_ENV=development
CMD ["flask", "run", "--host=0.0.0.0"]