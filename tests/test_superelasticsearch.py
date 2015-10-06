import functools
import json
import logging
import os
import time

from copy import deepcopy
from datadiff.tools import assert_equal as assertDictEquals
from elasticsearch import Elasticsearch, ElasticsearchException, TransportError
from mock import Mock
from random import randint

from superelasticsearch import SuperElasticsearch
from superelasticsearch import BulkOperation
from superelasticsearch import _BulkAction
try:
    import unittest2 as unittest
except ImportError:
    import unittest

elasticsearch_logger = logging.getLogger('elasticsearch')
elasticsearch_logger.setLevel(logging.ERROR)

# get file's path in current directory
local_path = lambda x: os.path.join(os.path.dirname(__file__), x)


class TestItersearch(unittest.TestCase):

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
        self.assertRaises(TypeError, self.ss.itersearch)

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


class TestBulkAction(unittest.TestCase):

    def test_bulk_action_must_not_accept_invalid_action(self):
        self.assertRaises(Exception, _BulkAction, type='asd', params={})

    def test_bulk_action_must_accept_valid_actions(self):
        _BulkAction('index', params={}, body=dict(key1='val1'))
        _BulkAction('create', params={}, body=dict(key1='val1'))
        _BulkAction('update', params={}, body=dict(key1='val1'))
        _BulkAction('delete', params={})

    def test_bulk_action_must_throw_exception_when_missing_expected_body(self):
        self.assertRaises(Exception, _BulkAction, 'index', params={})
        _BulkAction('index', params={}, body=dict(key1='val1'))

    def test_bulk_action_must_generate_valid_bulk_op_for_es(self):
        body = dict(key1='val1')

        action = _BulkAction('index', params={}, body=body)
        self.assertEquals(action.es_op,
                          (json.dumps({ 'index': {} }) + '\n' +
                           json.dumps(body)))

        action = _BulkAction('create', params=dict(routing='123', refresh=True),
                             body=body)
        self.assertEquals(action.es_op,
                          (json.dumps({ 'create': dict(routing='123',
                                                       refresh=True) }) +
                           '\n' + json.dumps(body)))

        # make sure that body is ignored when the operation does not require it
        action = _BulkAction('delete', params=dict(routing='123', refresh=True),
                             body=body)
        self.assertEquals(action.es_op,
                          (json.dumps({ 'delete': dict(routing='123',
                                                       refresh=True) })))


class TestBulkOperation(unittest.TestCase):

    # create a common Elasticsearch object
    es = Elasticsearch(hosts=['localhost:9200'])

    # create a common SuperElasticsearch object
    ss = SuperElasticsearch(hosts=['localhost:9200'])

    _index = 'automated_test_index__%s' % randint(0, 1000)

    def setUp(self):
        self._bulk = self.ss.bulk

    def tearDown(self):
        # restore bulk method back on SuperElasticsearch object
        self.ss.bulk = self._bulk

    def test_bulk_operation_returns_bulk_operation_object(self):
        self.assertTrue(
            isinstance(self.ss.bulk_operation(), BulkOperation))

    def test_bulk_operation_must_pass_superlelasticsearch_object(self):
        self.assertEquals(self.ss, self.ss.bulk_operation()._client)

    def test_index_or_create_must_push_correct_action(self):
        bulk = self.ss.bulk_operation()
        body = dict(key1='val1')

        # Without params
        bulk._index_or_create('index', body)
        action = bulk._actions[-1]
        self.assertEquals(action.type, 'index')
        assertDictEquals(action.body, body)
        assertDictEquals(action.params, {})

        # With params
        bulk._index_or_create('create', doc_type='test_doc_type', body=body,
                              id=1, consistency='sync', ttl=200)
        action = bulk._actions[-1]
        self.assertEquals(action.type, 'create')
        assertDictEquals(action.body, body)
        assertDictEquals(action.params, {
            '_type': 'test_doc_type',
            '_id': 1,
            'consistency': 'sync',
            'ttl': '200'
        })

        bulk._index_or_create('create', index='test_bulk',
                              doc_type='test_doc_type', body=body, 
                              routing='abcd', refresh=True)
        action = bulk._actions[-1]
        self.assertEquals(action.type, 'create')
        assertDictEquals(action.body, body)
        assertDictEquals(action.params, {
            '_index': 'test_bulk',
            '_type': 'test_doc_type',
            'routing': 'abcd',
            'refresh': 'true',
        })

    def test_index_calls_index_or_create_method_with_correct_args(self):
        bulk = self.ss.bulk_operation()
        body = dict(key1='val1')

        bulk._index_or_create = Mock()

        bulk.index(index='test_bulk', doc_type='test_bulk_doc_type', body=body,
                   timeout=200)
        self.assertTrue(bulk._index_or_create)
        self.assertEquals(bulk._index_or_create.call_args[0][0], 'index')
        assertDictEquals(bulk._index_or_create.call_args[0][1], body)
        self.assertEquals(bulk._index_or_create.call_args[0][2], None)
        self.assertEquals(bulk._index_or_create.call_args[1]['timeout'], 200)
        self.assertEquals(bulk._index_or_create.call_args[1]['index'],
                          'test_bulk')
        self.assertEquals(bulk._index_or_create.call_args[1]['doc_type'],
                          'test_bulk_doc_type')

    def test_create_calls_index_or_create_method_with_correct_args(self):
        bulk = self.ss.bulk_operation()
        body = dict(key1='val1')

        bulk._index_or_create = Mock()

        bulk.create(doc_type='test_bulk_doc_type', body=body,
                    id=4, timeout=200, routing='abcd')
        self.assertTrue(bulk._index_or_create)
        self.assertEquals(bulk._index_or_create.call_args[0][0], 'create')
        assertDictEquals(bulk._index_or_create.call_args[0][1], body)
        self.assertEquals(bulk._index_or_create.call_args[0][2], 4)
        self.assertEquals(bulk._index_or_create.call_args[1]['timeout'], 200)
        self.assertEquals(bulk._index_or_create.call_args[1]['doc_type'],
                          'test_bulk_doc_type')
        self.assertEquals(bulk._index_or_create.call_args[1]['routing'],
                          'abcd')

    def test_execute_must_empty_actions_after_executing_bulk_operation(self):
        bulk = self.ss.bulk_operation()
        body = dict(key1='val1')
        bulk.create(index='test_bulk', doc_type='test_bulk_doc_type', body=body,
                    id=4, routing='abcd')
        bulk.index(index='test_bulk', doc_type='test_bulk_doc_type', body=body)
        bulk.execute()
        self.assertEquals(len(bulk._actions), 0)

    def test_execute_must_return_bulk_response(self):
        bulk = self.ss.bulk_operation()
        body = dict(key1='val1')
        bulk.create(index='test_bulk', doc_type='test_bulk_doc_type', body=body,
                    id=4, routing='abcd')
        bulk.index(index='test_bulk', doc_type='test_bulk_doc_type', body=body)
        resp = bulk.execute()
        self.assertTrue(isinstance(resp, dict))
        self.assertTrue(isinstance(resp['items'], list))
        self.assertEquals(len(resp['items']), 2)

    def test_execute_must_call_bulk_with_correct_body_arg(self):
        body = dict(key1='val1')

        bulk = self.ss.bulk_operation()
        bulk._client.bulk = Mock()
        bulk.create(index='test_bulk', doc_type='test_bulk_doc_type', body=body,
                    id=4, routing='abcd')
        bulk.index(index='test_bulk', doc_type='test_bulk_doc_type', body=body)

        expected_bulk_body = ''
        for action in bulk._actions:
            expected_bulk_body += action.es_op + '\n'

        resp = bulk.execute()
        self.assertTrue(bulk._client.bulk.called)
        self.assertTrue(isinstance(bulk._client.bulk.call_args[1]['body'], str))
        self.assertEquals(bulk._client.bulk.call_args[1]['body'],
                          expected_bulk_body)

    def test_execute_must_use_kwargs_provided_at_the_creation_of_bulk_op(self):
        body = dict(key1='val1')

        bulk = self.ss.bulk_operation(index='default_index',
                                             doc_type='some_type',
                                             refresh=True)
        bulk._client.bulk = Mock()
        bulk.create(index='test_bulk', doc_type='test_bulk_doc_type', body=body,
                    id=4, routing='abcd')
        bulk.index(index='test_bulk', doc_type='test_bulk_doc_type', body=body)
        resp = bulk.execute()
        self.assertTrue(bulk._client.bulk.called)
        self.assertEquals(bulk._client.bulk.call_args[1]['index'],
                          'default_index')
        self.assertEquals(bulk._client.bulk.call_args[1]['doc_type'],
                          'some_type')
        self.assertEquals(bulk._client.bulk.call_args[1]['refresh'],
                          'true')

    def test_execute_must_override_kwargs_provided_at_bulk_op_creation(self):
        body = dict(key1='val1')

        bulk = self.ss.bulk_operation(index='default_index',
                                             doc_type='some_type',
                                             refresh=True)
        bulk._client.bulk = Mock()
        bulk.create(index='test_bulk', doc_type='test_bulk_doc_type', body=body,
                    id=4, routing='abcd')
        bulk.index(index='test_bulk', doc_type='test_bulk_doc_type', body=body)
        resp = bulk.execute(index='some_other_index', refresh=False)
        self.assertTrue(bulk._client.bulk.called)
        self.assertEquals(bulk._client.bulk.call_args[1]['index'],
                          'some_other_index')
        self.assertEquals(bulk._client.bulk.call_args[1]['doc_type'],
                          'some_type')
        self.assertEquals(bulk._client.bulk.call_args[1]['refresh'],
                          'false')

    def test_update_must_push_correct_action(self):
        bulk = self.ss.bulk_operation()
        body = dict(key1='val1')

        # Without params
        bulk.update(id=123, body=body)
        action = bulk._actions[-1]
        self.assertEquals(action.type, 'update')
        assertDictEquals(action.body, body)
        assertDictEquals(action.params, dict(_id=123))

        # With params
        bulk.update(index='test_index', doc_type='test_doc_type', body=body,
                    id=123, consistency='sync', ttl=200)
        action = bulk._actions[-1]
        self.assertEquals(action.type, 'update')
        assertDictEquals(action.body, body)
        assertDictEquals(action.params, {
            '_index': 'test_index',
            '_type': 'test_doc_type',
            '_id': 123,
            'consistency': 'sync',
            'ttl': '200'
        })

    def test_delete_must_push_correct_action(self):
        bulk = self.ss.bulk_operation()
        body = dict(key1='val1')

        # Without params
        bulk.delete(id=123)
        action = bulk._actions[-1]
        self.assertEquals(action.type, 'delete')
        assertDictEquals(action.body, None)
        assertDictEquals(action.params, dict(_id=123))

        # With params
        bulk.delete(index='test_index', doc_type='test_doc_type',
                    id=123, consistency='sync', parent=1)
        action = bulk._actions[-1]
        self.assertEquals(action.type, 'delete')
        assertDictEquals(action.body, None)
        assertDictEquals(action.params, {
            '_index': 'test_index',
            '_type': 'test_doc_type',
            '_id': 123,
            'consistency': 'sync',
            'parent': '1',
        })

        # Make sure delete does not push body even if passed
        bulk.delete(id=123, body=body)
        action = bulk._actions[-1]
        self.assertEquals(action.type, 'delete')
        assertDictEquals(action.body, None)
        assertDictEquals(action.params, dict(_id=123))
