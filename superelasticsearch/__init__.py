'''
    superelasticsearch
    ~~~~~~~~~~~~~~~~~~
'''

__all__ = ['SuperElasticsearch']

from elasticsearch import Elasticsearch, ElasticsearchException


class SuperElasticsearch(Elasticsearch):
    '''
    Subclass of elasticsearch.Elasticsearch to provide some useful utilities.
    '''

    def __init__(self, *args, **kwargs):
        super(SuperElasticsearch, self).__init__(*args, **kwargs)

        # presevery arguments and keyword arguments for bulk clients
        self._args = args
        self._kwargs = kwargs

    def itersearch(self, **iter_kwargs):
        '''
        Iterated search for making using Scroll API really simple to use.

        .. Usage::
        from superelasticsearch import SuperElasticsearch
        es = SuperElasticsearch(hosts=['localhost:9200'])
        for docs, _ in es.itersearch(index='tweets', doc_type='tweet'):
            for doc in docs:
                print doc['_id']
        '''

        # prepare kwargs for search
        kwargs = iter_kwargs.copy()
        if 'chunked' in kwargs:
            kwargs.pop('chunked')

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
            if iter_kwargs.get('chunked', True):
                yield resp['hits']['hits'], meta
            else:
                for doc in resp['hits']['hits']:
                    yield doc, meta

            # increment the counter
            counter += len(resp['hits']['hits'])

            # get the next set of results
            scroll_id = resp['_scroll_id']
            resp = self.scroll(scroll_id=scroll_id, scroll=kwargs['scroll'])

        # check if all the documents were scrolled or not
        if counter != total:
            raise ElasticsearchException('Failed to get all the documents '
                                         'while scrolling.\n'
                                         'Total documents that matched the '
                                         'query: %s\n'
                                         'Total documents that were retrieved '
                                         'while scrolling: %s\n'
                                         'Last scroll_id with documents: %s.\n'
                                         'Last scroll_id: %s ' % (
                                             total,
                                             counter,
                                             scroll_id,
                                             resp['_scroll_id']))

        # clear scroll
        self.clear_scroll(scroll_id=scroll_id)
