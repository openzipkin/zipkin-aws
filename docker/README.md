## zipkin-aws Docker image

This build produces the "openzipkin/zipkin-aws" image that integrates all
[zipkin-aws modules](../module)

Here's an example of using Amazon's Elasticsearch Service
```bash
$ docker run -d -p 9411:9411 --name zipkin-aws \
    -e STORAGE_TYPE=elasticsearch \
    -e ES_AWS_DOMAIN=YOUR_DOMAIN -e ES_AWS_REGION=YOUR_REGION \
    -v $HOME/.aws:/zipkin/.aws:ro \
    openzipkin/zipkin-aws
```

### Building for tests:

To build a zipkin-aws Docker image, in the top level of the repository, run something
like

```bash
$ docker build -t openzipkin/zipkin-aws:test -f docker/Dockerfile .
```
