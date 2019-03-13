FROM nginx:1.15.8-alpine

COPY ./nginx/nginx.conf /etc/nginx/conf.d/default.conf

WORKDIR /usr/share/nginx/html

COPY ./dist/uscheduler-ui .