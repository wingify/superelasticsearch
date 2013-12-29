'''
    superelasticsearch
    ~~~~~~~~~~~~~~~~~~
'''

from elasticsearch import Elasticsearch

class SuperElasticsearch(Elasticsearch):
    '''
    Subclass of elasticsearch.Elasticsearch to provide some useful utilities.
    '''

    def itersearch(self, **kwargs):
        # check for scroll argument in kwargs
        if 'scroll' not in kwargs:
            raise TypeError('Value for scroll parameter must be provided.')

        resp = self.search(**kwargs)
        total = resp['hits']['total']
        scroll_id = resp['_scroll_id']
        counter = 0

        while len(resp['hits']['hits']) > 0:
            yield resp

            # increment the counter
            counter += len(resp['hits']['hits'])

            # get the next set of results
            scroll_id = resp['_scroll_id']
            resp = self.scroll(scroll_id=scroll_id, scroll=kwargs['scroll'])

        # check if all the documents were scrolled or not
        if counter != total:
            raise AssertionError('Failed to get all the documents while '
                                 'scrolling.\n'
                                 'Total documents that matched the query: %s\n'
                                 'Total documents that were retrieved while '
                                 'scrolling: %s\n'
                                 'Last scroll_id with documents: %s.\n'
                                 'Last scroll_id: %s '% (total, counter,
                                                         scroll_id,
                                                         resp['_scroll_id']))

