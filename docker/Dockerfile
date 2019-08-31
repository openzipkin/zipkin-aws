#
# Copyright 2016-2019 The OpenZipkin Authors
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

FROM maven:3-jdk-11-slim

WORKDIR /code
COPY . /code
RUN mvn -B --no-transfer-progress package -DskipTests=true -pl autoconfigure/collector-sqs,autoconfigure/collector-kinesis,autoconfigure/storage-elasticsearch-aws,autoconfigure/storage-xray -am

RUN \
  cd /code && mkdir -p /zipkin-aws/sqs && cp autoconfigure/collector-sqs/target/zipkin-autoconfigure-collector-sqs-*-module.jar /zipkin-aws/sqs && cd /zipkin-aws/sqs && jar xf *.jar && rm *.jar && \
  cd /code && mkdir -p /zipkin-aws/kinesis && cp autoconfigure/collector-kinesis/target/zipkin-autoconfigure-collector-kinesis-*-module.jar /zipkin-aws/kinesis && cd /zipkin-aws/kinesis && jar xf *.jar && rm *.jar && \
  cd /code && mkdir -p /zipkin-aws/elasticsearch-aws && cp autoconfigure/storage-elasticsearch-aws/target/zipkin-autoconfigure-storage-elasticsearch-aws-*-module.jar /zipkin-aws/elasticsearch-aws && cd /zipkin-aws/elasticsearch-aws && jar xf *.jar && rm *.jar && \
  cd /code && mkdir -p /zipkin-aws/xray && cp autoconfigure/storage-xray/target/zipkin-autoconfigure-storage-xray-*-module.jar /zipkin-aws/xray && cd /zipkin-aws/xray && jar xf *.jar && rm *.jar

FROM openzipkin/zipkin:master
MAINTAINER Zipkin "https://zipkin.io/"

COPY --from=0 /zipkin-aws/ /zipkin/lib/

RUN echo > .xray_profile

ENV MODULE_OPTS="-Dloader.path=lib/sqs,lib/kinesis,lib/elasticsearch-aws,lib/xray -Dspring.profiles.active=sqs,kinesis,elasticsearch-aws,xray"
