'''
    superelasticsearch
    ~~~~~~~~~~~~~~~~~~
'''

__all__ = ['SuperElasticsearch']

from elasticsearch import Elasticsearch
from elasticsearch import ElasticsearchException
from elasticsearch.client.utils import query_params
from elasticsearch.serializer import JSONSerializer

# Use elasticsearch library's implementation of JSON serializer
json = JSONSerializer()


class SuperElasticsearch(Elasticsearch):
    '''
    Subclass of elasticsearch.Elasticsearch to provide some useful utilities.
    '''

    def __init__(self, *args, **kwargs):
        super(SuperElasticsearch, self).__init__(*args, **kwargs)

        # presevery arguments and keyword arguments for bulk clients
        self._args = args
        self._kwargs = kwargs

    def itersearch(self, **kwargs):
        '''
        Iterated search for making using Scroll API really simple to use.

        .. Usage::
        from superelasticsearch import SuperElasticsearch
        es = SuperElasticsearch(hosts=['localhost:9200'])
        for doc in es.itersearch(index='tweets', doc_type='tweet',
                                 chunked=False):
            print doc['_id']
        '''

        # prepare kwargs for search
        if 'chunked' in kwargs:
            chunked = kwargs.pop('chunked')
        else:
            chunked = True

        if 'with_meta' in kwargs:
            with_meta = kwargs.pop('with_meta')
        else:
            with_meta = False

        # check for scroll argument in kwargs
        if 'scroll' not in kwargs:
            raise TypeError('Value for scroll parameter must be provided.')

        resp = self.search(**kwargs)
        total = resp['hits']['total']
        scroll_id = resp['_scroll_id']
        counter = 0

        while len(resp['hits']['hits']) > 0:
            # prepare meta
            meta = resp.copy()
            meta['hits'] = resp['hits'].copy()
            meta['hits'].pop('hits')

            # if expected chunked, then return chunks else return
            # every doc per iteration
            if chunked:
                if with_meta:
                    yield resp['hits']['hits'], meta
                else:
                    yield resp['hits']['hits']
            else:
                for doc in resp['hits']['hits']:
                    if with_meta:
                        yield doc, meta
                    else:
                        yield doc

            # increment the counter
            counter += len(resp['hits']['hits'])

            # get the next set of results
            scroll_id = resp['_scroll_id']
            resp = self.scroll(scroll_id=scroll_id, scroll=kwargs['scroll'])

        # check if all the documents were scrolled or not
        if counter != total:
            raise ElasticsearchException(
                'Failed to get all the documents while scrolling. Total '
                'documents that matched the query: %s\n'
                'Total documents that were retrieved while scrolling: %s\n'
                'Last scroll_id with documents: %s.\n'
                'Last scroll_id: %s ' % (
                    total,
                    counter,
                    scroll_id,
                    resp['_scroll_id']))

        # clear scroll
        self.clear_scroll(scroll_id=scroll_id)

    def create_bulk_operation(self, **kwargs):
        '''
        Creates a new native client like instance for performing bulk
        operations. For every bulk operation, a new bulk operation instance
        must be created.

        .. Usage::
        from superelasticsearch import SuperElasticsearch
        es = SuperElasticsearch(hosts=['localhost:9200'])
        bulk = es.create_bulk_operation(index='bulk_index')

        bulk.index(
            index='other_bulk_index', doc_type='docs', body=dict(key1=val1))
        bulk.create(doc_type='docs', body=dict(key2=val2))

        bulk.execute()

        :arg index: Default index for items which don't provide one
        :arg doc_type: Default document type for items which don't provide one
        :arg consistency: Explicit write consistency setting for the operation
        :arg refresh: Refresh the index after performing the operation
        :arg routing: Specific routing value
        :arg replication: Explicitly set the replication type (default: sync)
        :arg timeout: Explicit operation timeout
        :returns: an instance of :class:`BulkOperation`

        .. Note:: all the arguments passed at the time create a new bulk
                  operation can be overridden when
                  :meth:`BulkOperation.execute`: is called.
        '''

        return BulkOperation(self, **kwargs)


class _BulkAction(object):

    def __init__(self, type, params, body=None):
        if type not in BulkOperation.BULK_ACTIONS:
            raise Exception('%s action type is not a valid Elasticsearch bulk '
                            'action type.' % type)

        if BulkOperation.BULK_ACTIONS.get(type) and body is None:
            raise Exception('%s action type expects a body as well to be a '
                            'valid bulk operation.' % type)

        self.type = type
        self.params = params
        self.body = body

    @property
    def es_op(self):
        retval = ''

        retval += json.dumps({self.type: self.params})
        if BulkOperation.BULK_ACTIONS.get(self.type):
            retval += '\n' + json.dumps(self.body)

        return retval


class BulkOperation(object):
    '''
    Simple bulk operations manager for Elasticsearch's Bulk API. Exposes API
    similar to non-bulk counterparts of all supported Bulk Operations, manages
    every call to make Bulk API request and executes it. Basically, it takes
    away the pain of writing different code because of difference between
    non-bulk APIs and bulk API.
    '''

    # Map of valid Bulk Actions that Elasticsearch supports and which of those
    # actions expect a body that needs to be added in the line next to the line
    # of action specification
    BULK_ACTIONS = {
        'index': True,
        'create': True,
        'update': True,
        'delete': False,
    }

    @query_params('index', 'doc_type', 'consistency', 'refresh', 'routing',
                  'replication', 'timeout')
    def __init__(self, client, params=None, **kwargs):
        '''
        API for performing easy bulk operations in Elasticsearch.

        :arg client: instance of official Elasticsearch Python client.
        :arg index: Default index for items which don't provide one
        :arg doc_type: Default document type for items which don't provide one
        :arg consistency: Explicit write consistency setting for the operation
        :arg refresh: Refresh the index after performing the operation
        :arg routing: Specific routing value
        :arg replication: Explicitly set the replication type (default: sync)
        :arg timeout: Explicit operation timeout

        .. Note:: all the arguments passed at the time create a new bulk
                  operation can be overridden when
                  :meth:`BulkOperation.execute`: is called.
        '''

        self._client = client
        self._params = params
        self._actions = []

    @query_params('index', 'doc_type', 'consistency', 'refresh', 'routing',
                  'replication', 'timeout')
    def execute(self, params=None, **kwargs):
        '''
        Executes all recorded actions using Elasticsearch's Bulk Query.

        .. Note:: The arguments passed at the time of creating a bulk client
                  will be overridden with the arguments passed to this method.

        :arg index: Default index for items which don't provide one
        :arg doc_type: Default document type for items which don't provide one
        :arg consistency: Explicit write consistency setting for the operation
        :arg refresh: Refresh the index after performing the operation
        :arg routing: Specific routing value
        :arg replication: Explicitly set the replication type (default: sync)
        :arg timeout: Explicit operation timeout
        '''

        # TO DO: check if percolate, timeout and replication parameters are
        #        allowed for bulk index operation

        bulk_body = ''
        for action in self._actions:
            bulk_body += action.es_op + '\n'

        bulk_kwargs = {}
        bulk_kwargs.update(self._params)
        bulk_kwargs.update(params)

        resp = self._client.bulk(body=bulk_body, **bulk_kwargs)
        self._actions = []
        return resp

    @query_params('index', 'doc_type', 'consistency', 'parent', 'refresh',
                  'routing', 'timestamp', 'ttl',
                  'version', 'version_type')
    def _index_or_create(self, action_type, body, id=None, params=None):
        '''
        Implementation of Bulk Index and Bulk Create operations.

        :arg action_type: The type of action i.e. **create** or **index**
        :arg index: The name of the index
        :arg doc_type: The type of the document
        :arg body: The document
        :arg id: Document ID
        :arg consistency: Explicit write consistency setting for the operation
        :arg parent: ID of the parent document
        :arg refresh: Refresh the index after performing the operation
        :arg routing: Specific routing value
        :arg timestamp: Explicit timestamp for the document
        :arg ttl: Expiration time for the document
        :arg version: Explicit version number for concurrency control
        :arg version_type: Specific version type
        '''

        # TO DO: check if percolate, timeout and replication parameters are
        #        allowed for bulk index operation

        bulk_params = {}

        if params.get('index') is not None:
            bulk_params['_index'] = params['index']
            params.pop('index')
        if params.get('doc_type') is not None:
            bulk_params['_type'] = params['doc_type']
            params.pop('doc_type')
        if id is not None:
            bulk_params.update(_id=id)

        bulk_params.update(params)

        self._actions.append(_BulkAction(type=action_type, params=bulk_params,
                                         body=body))

    def index(self, body, id=None, **kwargs):
        '''
        Implementation of Bulk Index operation.

        :arg action_type: The type of action i.e. **create** or **index**
        :arg index: The name of the index
        :arg doc_type: The type of the document
        :arg body: The document
        :arg id: Document ID
        :arg consistency: Explicit write consistency setting for the operation
        :arg parent: ID of the parent document
        :arg refresh: Refresh the index after performing the operation
        :arg routing: Specific routing value
        :arg timestamp: Explicit timestamp for the document
        :arg ttl: Expiration time for the document
        :arg version: Explicit version number for concurrency control
        :arg version_type: Specific version type
        '''

        self._index_or_create('index', body, id, **kwargs)

    def create(self, body, id=None, **kwargs):
        '''
        Implementation of Bulk Create operation.

        :arg action_type: The type of action i.e. **create** or **index**
        :arg index: The name of the index
        :arg doc_type: The type of the document
        :arg body: The document
        :arg id: Document ID
        :arg consistency: Explicit write consistency setting for the operation
        :arg parent: ID of the parent document
        :arg refresh: Refresh the index after performing the operation
        :arg routing: Specific routing value
        :arg timestamp: Explicit timestamp for the document
        :arg ttl: Expiration time for the document
        :arg version: Explicit version number for concurrency control
        :arg version_type: Specific version type
        '''

        self._index_or_create('create', body, id, **kwargs)

    @query_params('index', 'doc_type', 'consistency', 'parent', 'replication',
                  'routing', 'ttl', 'version', 'version_type')
    def update(self, id, body, params=None, **kwargs):
        '''
        Implementation of Bulk Update operation.

        :arg index: The name of the index
        :arg doc_type: The type of the document
        :arg body: The document
        :arg id: Document ID
        :arg consistency: Explicit write consistency setting for the operation
        :arg parent: ID of the parent document
        :arg routing: Specific routing value
        :arg ttl: Expiration time for the document
        :arg version: Explicit version number for concurrency control
        :arg version_type: Specific version type
        '''

        bulk_params = {}

        if params.get('index') is not None:
            bulk_params['_index'] = params['index']
            params.pop('index')
        if params.get('doc_type') is not None:
            bulk_params['_type'] = params['doc_type']
            params.pop('doc_type')
        bulk_params.update(_id=id)

        bulk_params.update(params)

        self._actions.append(_BulkAction(type='update', params=bulk_params,
                                         body=body))

    @query_params('index', 'doc_type', 'consistency', 'parent', 'replication',
                  'routing', 'version', 'version_type')
    def delete(self, id, params=None, **kwargs):
        '''
        Implementation of Bulk Delete operation.

        :arg index: The name of the index
        :arg doc_type: The type of the document
        :arg id: Document ID
        :arg consistency: Explicit write consistency setting for the operation
        :arg parent: ID of the parent document
        :arg replication: Explicitly set the replication type (default: sync)
        :arg routing: Specific routing value
        :arg version: Explicit version number for concurrency control
        :arg version_type: Specific version type
        '''

        bulk_params = {}

        if params.get('index') is not None:
            bulk_params['_index'] = params['index']
            params.pop('index')
        if params.get('doc_type') is not None:
            bulk_params['_type'] = params['doc_type']
            params.pop('doc_type')
        bulk_params.update(_id=id)

        bulk_params.update(params)

        self._actions.append(_BulkAction(type='delete', params=bulk_params))
