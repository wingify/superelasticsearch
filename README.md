# superelasticsearch

superelasticsearch is utility library that extends a particular version of
[elasticsearch][es] library to provide some more utility functions on top of it
to make using [Elasticsearch][es_server] even easier.

## Compatibility

This compatibility table indicates compatibility of SuperElasticsearch with
the versions of elasticsearch-py and Elasticsearch. This only means that we
have tested SuperElasticsearch with these versions of Elasticsearch and
the official Elasticsearch Python client. It may actually work with newer
releases. If such is the case, please feel free to updated this table by
opening a pull request.

| Elasticsearch  | elasticsearch-py | SuperElasticsearch | Release date |
| -------------- | ---------------- | ------------------ | ------------ |
| 1.7.2          | 1.7.0            | 0.1.0              | Oct 6, 2015  |

## Design & Usage

superelasticsearch is nothing but a sub-class of the Elasticsearch Python
client. It closely follows the API design as the Elasticsearch client library
does.

## Additional Python APIs

SuperElasticsearch provides a few additional APIs that are sugar coted to
simplify using Elasticsearch in Python. These additional APIs are listed as
follows:

### Iterated Serach (or simpler Scroll API)

Iterated search allows you to perform scroll API with ease and helps you reduce
code, especially where you might want to use it in a loop. Iterated search
returns a generator which can be iterated in a loop to get docs in returned by
every Scroll API call. The best part is that the Scroll ID of every scroll is
handled by the ``itersearch`` API.

```
from superelasticsearch import SuperElasticsearch

client = SuperElasticsearch(hosts=['localhost:9200'])

for doc in client.itersearch(index='test_index', doc_type'tweets',
                             scroll='10m'):
    # do something with doc here
    pass

```

### Simpler Bulk API

Elasitcsearch's Bulk API is extremely helpful but has different semantics.
Using Bulk API means manual handling of all the differences in naming of
parameters, manual construction of the complex bulk body and all the errors
and debugging challenges that come as extra work in the process.

SuperElasticsearch provides a simpler Bulk API that enables you to use Bulk
API in a non-bulk fashion.

Example:

```
from superelasticsearch import SuperElasticsearch

client = SuperElasticsearch(hosts=['localhost:9200'])
bulk = client.bulk_operation()

bulk.index(index='test_index_1', doc_type='test_doc_type',
           body=dict(key1='val1'))
bulk.delete(index='test_index_2', doc_type='test_doc_type',
            id=123)
bulk.update(index='test_index_3', doc_type='test_doc_type',
            id=456, body={
                'script': 'ctx._source.count += count',
                'params': {
                    'count': 1
                }
            })

resp = bulk.execute()
```

SuperElasticsearch's Bulk Operations do all the book keeping of individual
operations that you perform, properly serialize those operations to Bulk APIs
requirements and executes the request.

[es]: http://github.com/elasticsearch/elasticsearch-py
[es_server]: http://elasticsearch.org

## License

This project is licensed under MIT License.

Copyright (c) 2013 Wingify Software Pvt. Ltd.

See [LICENSE.md](LICENSE.md).
