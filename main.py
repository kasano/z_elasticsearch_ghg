# write a class in Python that connects to an Elasticsearch database, 
# and also queries that database for the top 3 results. 
# The class should be able to be run from the command line,
# and should print the results to the console.

import sys
import json
from elasticsearch import Elasticsearch

class ElasticSearch:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.es = Elasticsearch([{'host': self.host, 'port': self.port}])

    def query(self, index, query):
        res = self.es.search(index=index, body=query)
        return res
    
    def print_results(self, res):
        for hit in res['hits']['hits']:
            print(json.dumps(hit['_source'], indent=4))
                
if __name__ == '__main__':
    host = sys.argv[1]
    port = sys.argv[2]
    index = sys.argv[3]
    query = sys.argv[4]
    es = ElasticSearch(host, port)
    res = es.query(index, query)
    es.print_results(res)
    
# python main.py localhost 9200 my_index '{"query": {"match_all": {}}}'