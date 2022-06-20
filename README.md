# Elasticsearch REST client examples

This repository shows a few examples of how to use the [Elasticsearch Java
client](https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/index.html).
It is using Testcontainers for Elasticsearch, so you need to have Docker
up and running.

You can run the tests via `./gradlew clean check`, but I suppose just reading
the source in an IDE is more interesting.

Note: The `main` branch of this repo uses the [new Elasticsearch
Client](https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/index.html).
If you are still using the [old
one](https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/index.html)
``, simply switch to the `hlrc` branch of this repo).

