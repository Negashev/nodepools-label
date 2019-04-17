FROM alpine

WORKDIR /src

RUN apk add --update python3

ADD requirements.txt ./

RUN apk add --no-cache --virtual .build-deps \
    && pip3 --no-cache install -r requirements.txt \
	&& apk del .build-deps \
	&& rm -rf /var/cache/apk/*

CMD ["python3", "-u", "run.py"]

ADD *.py /src/