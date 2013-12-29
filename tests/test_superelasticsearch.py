import json
import os
import time

from copy import deepcopy
from elasticsearch import Elasticsearch
from mock import Mock
from superelasticsearch import SuperElasticsearch
try:
    import unittest2 as unittest
except ImportError:
    import unittest


# get file's path in current directory
local_path = lambda x: os.path.join(os.path.dirname(__file__), x)


class TestSuperElasticsearch(unittest.TestCase):

    # create a common Elasticsearch object
    es = Elasticsearch(hosts=['localhost:9200'])

    # create a common SuperElasticsearch object
    ss = SuperElasticsearch(hosts=['localhost:9200'])

    _index = 'automated_test_index'
    _doc_type = 'automated_test_doc_type'

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

    def test_itersearch_performs_scroll(self):
        for size in (10, 100):
            scrollCounter = 0
            docsCounter = 0
            time.sleep(1)
            for docs in self.ss.itersearch(index=self._index,
                                           doc_type=self._doc_type,
                                           body=dict(query=dict(match_all={})),
                                           scroll='10m', size=size):
                scrollCounter += 1
                docsCounter += len(docs['hits']['hits'])

            self.assertEquals(scrollCounter, self._total_docs / size + 1)

    def test_itersearch_raises_assertion_error_when_fetched_docs_are_less(self):
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
        mocked_search_result = deepcopy(mocked_value_template)
        ss.search = Mock(return_value=mocked_search_result)
        mocked_scroll_result = deepcopy(mocked_value_template)
        mocked_scroll_result['_scroll_id'] = 456456
        mocked_scroll_result['hits']['hits']  = [
            dict(some_doc="with_some_val") for i in xrange(2)
        ]
        ss.scroll = Mock(return_value=mocked_scroll_result)

        def _assertion():
            search_generator = ss.itersearch(index=self._index,
                                             doc_type=self._doc_type,
                                             body=dict(query=dict(
                                                 match_all={})),
                                             scroll='10m')
            search_generator.next()
            search_generator.next()

            mocked_scroll_result = deepcopy(mocked_value_template)
            mocked_scroll_result['_scroll_id'] = 789789
            mocked_scroll_result['hits']['hits']  = []
            ss.scroll = Mock(return_value=mocked_scroll_result)
            search_generator.next()

        self.assertRaises(AssertionError, _assertion)

    @classmethod
    def tearDownClass(cls):
        cls.es.indices.delete(index=cls._index)
