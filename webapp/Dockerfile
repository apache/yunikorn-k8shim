# State 1
FROM node:8.15.0-alpine as node

WORKDIR /usr/uiapp

COPY package.json yarn.lock ./

COPY . .

RUN rm -rf ./dist

RUN yarn install

RUN yarn build:prod

# Stage 2
FROM nginx:1.15.8-alpine

COPY --from=node /usr/uiapp/dist/uscheduler-ui /usr/share/nginx/html

COPY ./nginx/nginx.conf /etc/nginx/conf.d/default.conf

ENTRYPOINT [ "nginx", "-c", "/etc/nginx/nginx.conf", "-g", "daemon off;"]
