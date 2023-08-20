DOCKER_ID = $(docker ps -q -a)
TOPIC=test
ZOOKEEPER=zookeeper:2181
KAFKA=kafka:9092
CONSUMER_GROUP=test-consumer

up:
	#source .env
	docker-compose up

upd:
	docker-compose up -d

fup:
	docker-compose up --force-recreate

down:
	docker-compose down

minio-init:
	sh create-assets.sh


clean:
	docker volume prune -f

clean-docker:
	docker rm $(DOCKER_ID)


ls-topic:
	docker-compose exec kafka kafka-topics.sh --list --zookeeper $(ZOOKEEPER)

ls-consumer:
	docker-compose exec kafka kafka-consumer-groups.sh  --list --bootstrap-server $(KAFKA)

create-consumer:
	docker-compose exec kafka kafka-console-consumer.sh  --bootstrap-server $(KAFKA) --topic $(TOPIC) --group $(CONSUMER_GROUP)

describe:
	docker-compose exec kafka kafka-topics.sh --describe --topic $(TOPIC) --zookeeper $(ZOOKEEPER)

consumer:
	docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server $(KAFKA) --topic $(TOPIC) --group $(CONSUMER_GROUP)  --from-beginning

producer:
	docker-compose exec kafka kafka-console-producer.sh --bootstrap-server $(KAFKA) --topic $(TOPIC)

ssh-flink-master:
	docker-compose exec flink-master bash
#flink master container /usr/local/flink/conf/flink-conf.yaml -> state.backend: filesystem: http://127.0.0.1:9000 for minio

ssh-flink-worker:
	docker-compose exec flink-worker bash #cat /tmp/output.json*

clean-files:
	rm tmp/output*


#minio console：http://127.0.0.1:9001/
#flink console：http://127.0.0.1:8083/

