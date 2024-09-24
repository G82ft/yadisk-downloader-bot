FROM alpine:3.19

RUN apk add alpine-sdk linux-headers git zlib-dev openssl-dev gperf cmake
RUN git clone https://github.com/tdlib/telegram-bot-api.git
WORKDIR telegram-bot-api/
RUN git clone https://github.com/tdlib/td.git
RUN rm -rf build && mkdir build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX:PATH=.. .. && cmake --build . --target install

FROM python:3.10-alpine3.19

COPY . /app
WORKDIR /app

RUN mkdir logs/ && mkdir data/ && mkdir temp/

COPY --from=0 / /

RUN pip install -r requirements.txt

ENTRYPOINT /bin/ash