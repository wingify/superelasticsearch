# superelasticsearch

superelasticsearch is utility library that extends a particular version of
[elasticsearch][es] library to provide some more utility functions on top of it
to make using [Elasticsearch][es_server] even easier.

[![build status](http://office.wingify.com/gitlab-ci/projects/9/status.png?ref=master)](http://office.wingify.com/gitlab-ci/projects/9?ref=master)

## Version

Version of this library indicicates that this library works with the exact same
version of the [official Elasticsearch Python client][es] as well.

## Usage

superelasticsearch is nothing but a sub-class of the Elasticsearch Python
client. It closely follows the API design as the Elasticsearch client library
does.

## Additional APIs

SuperElasticsearch provides a few additional APIs that are sugar coted to
simplify using Elasticsearch in Python. These additional APIs are listed as
follows:

### Iterated Serach (or simple Scroll API)

Iterated search allows you to perform scroll API with ease and helps you reduce
code, especially where you might want to use it in a loop. Iterated search
returns a generator which can be iterated in a loop to get docs in returned by
every Scroll API call. The best part is that the Scroll ID of every scroll is
handled by the ``itersearch`` API.

```
from superelasticsearch import SuperElasticsearch

client = SuperElasticsearch(hosts=['localhost:9200'])

for docs in client.itersearch(index='test_index', doc_type'tweets',
                              scroll='10m'):
    # do something with docs here
    pass

```

[es]: http://github.com/elasticsearch/elasticsearch-py
[es_server]: http://elasticsearch.org
