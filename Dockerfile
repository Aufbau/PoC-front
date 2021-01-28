FROM python:3.8.7-alpine3.12
WORKDIR /app
ADD requirements.txt ./

# Fix for psycopg2 not installing via pip. SRC: https://stackoverflow.com/a/47871121 
RUN \
 apk add --no-cache postgresql-libs && \
 apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev && \
 python3 -m pip install -r requirements.txt --no-cache-dir && \
 apk --purge del .build-deps

ADD ./worker_app ./
# ENV FLASK_ENV=development
CMD ["python3", "app.py"]