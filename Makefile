APP_NAME=stiklas
VERSION_VAR=bitbucket.org/shortlyst/stiklas/cmd/stiklas.Version
VERSION=$(shell git describe --tags)
TEST_DB_PWD=user123

build: dep
	CGO_ENABLED=0 GOOS=linux go build -ldflags "-X ${VERSION_VAR}=${VERSION}" -a -installsuffix nocgo -o ./bin ./...

dep:
	@echo ">> Downloading Dependencies"
	@go mod download

docker:
	@echo ">> Building Docker Image"
	@docker build -t ${APP_NAME}:latest .

run-api: dep
	@echo ">> Running API Server"
	@env $$(cat .env | xargs) go run bitbucket.org/shortlyst/stiklas/cmd/stiklas server

run-worker: dep
	@echo ">> Running Worker"
	@env $$(cat .env | xargs) go run bitbucket.org/shortlyst/stiklas/cmd/stiklas worker

migrate: dep
	@echo ">> Running DB migration"
	@env $$(cat .env | xargs) go run bitbucket.org/shortlyst/stiklas/cmd/stiklas migrate

test: dep
	@echo ">> Running Unit Test"
	@env $$(cat .env.testing | xargs) TESTING=true go test -count=1 -cover -covermode=atomic ./...

test-integration: dep test-infra-up test-db-migrate
	@echo ">> Running Integration Test"
	@env $$(cat .env.testing | xargs) TESTING=true go test -tags=integration -count=1 -p=1 -cover -covermode=atomic ./...
	$(MAKE) test-infra-down

test-infra-up:
	$(MAKE) test-infra-down
	@echo ">> Starting Test DB"
	@docker run --name stiklas-test-mysql-db -p 3333:3306 -e MYSQL_ROOT_PASSWORD=${TEST_DB_PWD} -d --rm mysql:5.7
	@-docker exec stiklas-test-mysql-db sh -c 'while ! mysqladmin ping -h"0.0.0.0 -P3306" --silent; do sleep 1; done'
	@echo ">> Starting Test Rabbit MQ"
	@docker run --name stiklas-test-rabbitmq -p 56722:5672 -d --rm rabbitmq:3
	@-docker exec stiklas-test-rabbitmq sh -c 'while ! rabbitmqctl ping; do sleep 3; done'

test-db-migrate:
	@echo ">> Run Migration on Test DB"
	@-docker exec stiklas-test-mysql-db sh -c "mysql -uroot -p${TEST_DB_PWD} -P 3306 -e 'CREATE DATABASE stiklas;'"
	@-env $$(cat .env.testing | xargs) go run bitbucket.org/shortlyst/stiklas/cmd/stiklas migrate

test-infra-down:
	@echo ">> Shutting Down Test DB"
	@-docker kill stiklas-test-mysql-db
	@echo ">> Shutting Down Test Rabbit MQ"
	@-docker kill stiklas-test-rabbitmq

.PHONY: build dep docker run test test-all migrate test-infra-up test-db-migrate test-infra-down