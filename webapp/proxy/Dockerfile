FROM node:8.15.0-alpine

RUN npm --loglevel=error install -g corsproxy

EXPOSE 1337

ENV CORSPROXY_HOST 0.0.0.0

ENTRYPOINT ["corsproxy"]
