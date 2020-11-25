## zipkin-aws Docker image

This repository contains the Docker build definition for `zipkin-aws`.

This layers Amazon Web Services support on the base zipkin docker image.

Currently, this adds Xray Trace storage

## Running

By default, this image will search for credentials in the $HOME/.aws directory.

If you want to try Zipkin against AWS Elasticsearch, the easiest start is to share
your credentials with Zipkin's docker image.

```bash
# Note: this is mirrored as ghcr.io/openzipkin/zipkin-aws
$ docker run -d -p 9411:9411 --rm --name zipkin-aws \
    -e STORAGE_TYPE=elasticsearch \
    -e ES_AWS_DOMAIN=YOUR_DOMAIN -e ES_AWS_REGION=YOUR_REGION \
    -v $HOME/.aws:/zipkin/.aws:ro \
    openzipkin/zipkin-aws
```

## Configuration

Configuration is via environment variables, defined [here](../module/README.md).

In Docker, the following can also be set:

    * `JAVA_OPTS`: Use to set java arguments, such as heap size or trust store location.

## Building

To build a zipkin-aws Docker image from source, in the top level of the repository, run:


```bash
$ build-bin/docker/docker_build openzipkin/zipkin-aws:test
```

To build from a published version, run this instead:

```bash
$ build-bin/docker/docker_build openzipkin/zipkin-aws:test 0.18.1
```

