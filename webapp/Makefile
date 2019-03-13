deploy-prod:
	docker-compose up -d
build-prod:
	yarn install && yarn build:prod
start-dev:
	yarn start:dev
start-jsonserver:
	json-server --watch db.json --routes routes.json --port 9080
start-corsproxy:
	CORSPROXY_PORT=1337 corsproxy
build-webapp:
	docker build -t yunikorn/scheduler-web:0.1.0 .
start-webapp:
	docker run -d -p 8089:80 yunikorn/scheduler-web:0.1.0
