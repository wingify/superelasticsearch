import functools
import json
import logging
import os
import time

from copy import deepcopy
from elasticsearch import Elasticsearch, ElasticsearchException, TransportError
from mock import Mock
from random import randint
from superelasticsearch import SuperElasticsearch
try:
    import unittest2 as unittest
except ImportError:
    import unittest

elasticsearch_logger = logging.getLogger('elasticsearch')
elasticsearch_logger.setLevel(logging.ERROR)

# get file's path in current directory
local_path = lambda x: os.path.join(os.path.dirname(__file__), x)


class TestSuperElasticsearch(unittest.TestCase):

    # create a common Elasticsearch object
    es = Elasticsearch(hosts=['localhost:9200'])

    # create a common SuperElasticsearch object
    ss = SuperElasticsearch(hosts=['localhost:9200'])

    _index = 'automated_test_index__%s' % randint(0, 1000)
    _doc_type = 'automated_test_doc_type__%s' % randint(0, 1000)

    @classmethod
    def setUpClass(cls):
        cls._total_docs = 0

        # read documents from a file and setup test index
        with open(local_path('test_data.dump'), 'r') as f:
            data = json.loads(f.read())
        for doc in data:
            cls.es.index(index=cls._index,
                         doc_type=cls._doc_type,
                         body=doc)
            cls._total_docs += 1

    def test_itersearch_raises_typeerror_when_scroll_param_is_missing(self):
        self.assertRaises(TypeError, self.ss.itersearch().next)

    def test_chunked_itersearch_performs_scroll(self):
        for size in (10, 100):
            scrollCounter = 0
            docsCounter = 0
            time.sleep(1)
            for docs in self.ss.itersearch(index=self._index,
                                           doc_type=self._doc_type,
                                           body=dict(
                                               query=dict(match_all={})),
                                           scroll='10m', size=size):
                scrollCounter += 1
                docsCounter += len(docs)

            self.assertEquals(scrollCounter, self._total_docs / size + 1)

    def test_non_chunked_itersearch_performs_scroll(self):
        for size in (10, 100):
            docsCounter = 0
            time.sleep(1)
            for doc in self.ss.itersearch(index=self._index,
                                          doc_type=self._doc_type,
                                          body=dict(query=dict(match_all={})),
                                          scroll='10m', size=size,
                                          chunked=False):
                docsCounter += 1

            self.assertEquals(docsCounter, self._total_docs)

    def test_chunked_itersearch_with_meta_returns_meta(self):
        for size in (10, 100):
            scrollCounter = 0
            docsCounter = 0
            time.sleep(1)
            for docs, meta in self.ss.itersearch(index=self._index,
                                                 doc_type=self._doc_type,
                                                 body=dict(query=dict(
                                                     match_all={})),
                                                 scroll='10m', size=size,
                                                 chunked=True,
                                                 with_meta=True):
                docsCounter += len(docs)
                scrollCounter += 1

            self.assertEquals(docsCounter, self._total_docs)
            self.assertEquals(scrollCounter, self._total_docs / size + 1)
            self.assertTrue(isinstance(meta, dict))
            self.assertEquals(meta['hits']['total'], self._total_docs)

    def test_non_chunked_itersearch_with_meta_returns_meta(self):
        for size in (10, 100):
            docsCounter = 0
            time.sleep(1)
            for doc, meta in self.ss.itersearch(index=self._index,
                                                doc_type=self._doc_type,
                                                body=dict(query=dict(
                                                    match_all={})),
                                                scroll='10m', size=size,
                                                chunked=False,
                                                with_meta=True):
                docsCounter += 1

            self.assertEquals(docsCounter, self._total_docs)
            self.assertTrue(isinstance(meta, dict))
            self.assertEquals(meta['hits']['total'], self._total_docs)

    def test_itersearch_raises_assertion_error_when_less_docs_fetched(self):
        mocked_value_template = {
            "took": 27,
            "timed_out": False,
            "_scroll_id": 123213,
            "_shards": {
                "total": 2,
                "successful": 2,
                "failed": 0
            },
            "hits": {
                "total": 13,
                "max_score": None,
                "hits": [
                    dict(some_doc="with_some_val") for i in xrange(10)
                ]
            }
        }

        ss = SuperElasticsearch(hosts=['localhost:9200'])

        def assertion(chunked):
            # mock the client's scroll method
            mocked_search_result = deepcopy(mocked_value_template)
            ss.search = Mock(return_value=mocked_search_result)
            mocked_scroll_result = deepcopy(mocked_value_template)
            mocked_scroll_result['_scroll_id'] = 456456
            mocked_scroll_result['hits']['hits'] = [
                dict(some_doc="with_some_val") for i in xrange(2)
            ]
            ss.scroll = Mock(return_value=mocked_scroll_result)

            search_generator = ss.itersearch(index=self._index,
                                             doc_type=self._doc_type,
                                             body=dict(query=dict(
                                                 match_all={})),
                                             scroll='10m',
                                             chunked=chunked)
            if chunked:
                iterate_times = 2
            else:
                iterate_times = 12

            for _ in range(0, iterate_times):
                search_generator.next()

            mocked_scroll_result = deepcopy(mocked_value_template)
            mocked_scroll_result['_scroll_id'] = 789789
            mocked_scroll_result['hits']['hits'] = []
            ss.scroll = Mock(return_value=mocked_scroll_result)
            search_generator.next()

        self.assertRaises(ElasticsearchException,
                          functools.partial(assertion, True))
        self.assertRaises(ElasticsearchException,
                          functools.partial(assertion, False))

    def test_that_itersearch_clears_scroll_on_successful_scroll(self):
        for docs, meta in self.ss.itersearch(index=self._index,
                                             doc_type=self._doc_type,
                                             body=dict(
                                                 query=dict(match_all={})),
                                             scroll='10m', size=100,
                                             with_meta=True):
            scroll_id = meta['_scroll_id']
        # check if it was the right exception
        self.assertRaises(TransportError, self.es.scroll, scroll_id)
        try:
            self.es.scroll(scroll_id)
        except TransportError, err:
            self.assertTrue('SearchContextMissingException' in str(err))

    @classmethod
    def tearDownClass(cls):
        cls.es.indices.delete(index=cls._index)
