#
# Copyright 2016-2020 The OpenZipkin Authors
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under
# the License.
#

# We copy files from the context into a scratch container first to avoid a problem where docker and
# docker-compose don't share layer hashes https://github.com/docker/compose/issues/883 normally.
# COPY --from= works around the issue.
FROM scratch as scratch

COPY . /code/

FROM openzipkin/zipkin-builder as built

COPY --from=scratch /code /code

WORKDIR /code

RUN mvn -B --no-transfer-progress package -DskipTests=true -pl module/collector-sqs,module/collector-kinesis,module/storage-elasticsearch-aws,module/storage-xray -am

WORKDIR /zipkin-aws

RUN cp /code/module/collector-sqs/target/zipkin-module-collector-sqs-*-module.jar sqs.jar && \
  (mkdir sqs && cd sqs && jar -xf ../sqs.jar) && \
  rm sqs.jar

RUN cp /code/module/collector-kinesis/target/zipkin-module-collector-kinesis-*-module.jar kinesis.jar && \
  (mkdir kinesis && cd kinesis && jar -xf ../kinesis.jar) && \
  rm kinesis.jar

RUN cp /code/module/storage-elasticsearch-aws/target/zipkin-module-storage-elasticsearch-aws-*-module.jar elasticsearch-aws.jar && \
  (mkdir elasticsearch-aws && cd elasticsearch-aws && jar -xf ../elasticsearch-aws.jar) && \
  rm elasticsearch-aws.jar

RUN cp /code/module/storage-xray/target/zipkin-module-storage-xray-*-module.jar xray.jar && \
  echo > .xray_profile && \
  (mkdir xray && cd xray && jar -xf ../xray.jar) && \
  rm xray.jar

FROM openzipkin/zipkin:master
MAINTAINER Zipkin "https://zipkin.io/"

COPY --from=built /zipkin-aws/ /zipkin/

ENV MODULE_OPTS="-Dloader.path=sqs,kinesis,elasticsearch-aws,xray -Dspring.profiles.active=sqs,kinesis,elasticsearch-aws,xray"
