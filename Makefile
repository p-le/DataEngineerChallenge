BASE_PATH = $(shell pwd)
define DOCKER_PREFIX_SBT
docker container run -it --rm \
	--workdir /app \
	-v ${BASE_PATH}/cache/.cache:/home/sbtuser/.cache \
	-v ${BASE_PATH}/cache/.ivy2:/home/sbtuser/.ivy2 \
	-v ${BASE_PATH}/cache/.sbt:/home/sbtuser/.sbt \
	-v ${BASE_PATH}/project:/app/project \
	-v ${BASE_PATH}/src:/app/src \
	-v ${BASE_PATH}/target:/app/target \
	-v ${BASE_PATH}/build.sbt:/app/build.sbt \
	hseeberger/scala-sbt:11.0.10_1.5.0_2.12.13
endef

.PHONY: build-spark3-python3.8
build-spark3-python3.8:
	docker image build -t spark3-python:3.8 -f images/Dockerfile.spark ./images/

.PHONY: start-stack
start-stack: build-spark3-python3.8
	docker-compose up

.PHONY: compile
compile:
	${DOCKER_PREFIX_SBT} \
		sbt compile


.PHONY: package
package:
	${DOCKER_PREFIX_SBT} \
		sbt clean package


.PHONY: run
run:
	${DOCKER_PREFIX_SBT} \
		sbt "run --${FILE}"

# Run Spark application locally on 2 cores
.PHONY: submit
OUTPUT_JAR = analyze-job_2.12-0.1.0-SNAPSHOT.jar
# INPUT_FILE = /opt/bitnami/spark/data/2015_07_22_mktplace_shop_web_log_sample.log.gz
INPUT_FILE = /data/test.gz
submit: package
	docker-compose exec spark \
		spark-submit \
		--class phu.le.dev.challenge.AnalyzeJob \
		--master local[2] \
		target/scala-2.12/${OUTPUT_JAR} \
		${INPUT_FILE}

.PHONY: test
test:
	${DOCKER_PREFIX_SBT} \
		sbt test


.PHONY: clean
CURRENT_UID := $(shell id -u)
CURRENT_GID := $(shell id -g)
clean:
	$(shell chown -R vagrant:vagrant .)
	$(shell find ./target ! -name '.gitignore' -type f -exec rm -rf {} +)
