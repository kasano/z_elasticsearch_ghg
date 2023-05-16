# write a class in Python that connects to an Elasticsearch database, 
# and also queries that database for the top 3 results. 
# The class should be able to be run from the command line,
# and should print the results to the console.

import sys
import json
from elasticsearch import Elasticsearch

class ElasticSearch:
    def __init__(self, cloud_id, secret):
        self.cloud_id = cloud_id
        self.secret = secret
        # self.es = Elasticsearch([{'host': self.host, 'port': self.port}])
        self.es = Elasticsearch(
            cloud_id=self.cloud_id,
            basic_auth=("elastic", self.secret)
        )

    def query(self, index, query,n_return):
        # res = self.es.search(index=index, body=query)
        result = self.es.search(index=index, query={"fuzzy": {"Description": {"value": query}}} )
        result_length = len(result.body['hits']['hits'])
        return result.body['hits']['hits'][0:min( result_length,n_return )]
    
    def print_results(self, res):
        print( {'emission_factors': [ hit['_source']['Description'] for hit in res ] } )
        # print(json.dumps(hit['_source'], indent=4))
                
if __name__ == '__main__':
    cloud_id = sys.argv[1]
    secret = sys.argv[2]
    index = sys.argv[3]
    query = sys.argv[4]
    es = ElasticSearch(cloud_id, secret)
    res = es.query(index, query,3)
    es.print_results(res)
    
# python main.py <cloud_id> <pw> development mangrove
