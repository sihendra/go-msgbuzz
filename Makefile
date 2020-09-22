build: dep
	CGO_ENABLED=0 GOOS=linux go build ./...

dep:
	@echo ">> Downloading Dependencies"
	@go mod download

test-all: test-unit test-integration

test-all-no-infra: test-unit test-integration-no-infra

test-unit: dep
	@echo ">> Running Unit Test"
	@go test -tags=unit -count=1 -cover -covermode=atomic ./...

test-integration-no-infra: dep
	@echo ">> Running Integration Test"
	@env $$(cat .env.testing | xargs) go test -tags=integration -failfast -count=1 -p=1 -cover -covermode=atomic ./...

test-integration: test-infra-up test-integration-no-infra test-infra-down

test-infra-up: test-infra-down
	@echo ">> Starting Rabbit MQ"
	@docker run --name go-msgbuzz-test-rabbitmq -p 56723:5672 -d --rm rabbitmq:3
	@docker exec go-msgbuzz-test-rabbitmq sh -c 'sleep 5; rabbitmqctl wait /var/lib/rabbitmq/mnesia/rabbit@$$(hostname).pid'

test-infra-down:
	@echo ">> Shutting Down Rabbit MQ"
	@-docker kill go-msgbuzz-test-rabbitmq

dummy-rabbitmq-infra-up: dummy-rabbitmq-infra-down
	@echo ">> Starting Dummy Rabbit MQ"
	@docker run --name go-msgbuzz-dummy-rabbitmq -p 5673:5672 -d --rm rabbitmq:3
	@docker exec go-msgbuzz-dummy-rabbitmq sh -c 'sleep 5; rabbitmqctl wait /var/lib/rabbitmq/mnesia/rabbit@$$(hostname).pid'	

dummy-rabbitmq-infra-down:
	@echo ">> Shutting Down Dummy Rabbit MQ"
	@-docker kill go-msgbuzz-dummy-rabbitmq

.PHONY: build dep test-all test-all-no-infra test-unit test-integration test-integration-no-infra test-infra-up test-infra-down dummy-rabbitmq-infra-up dummy-rabbitmq-infra-down