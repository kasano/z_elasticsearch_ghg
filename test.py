# The test suite should test the following:
# 1. The class can connect to an Elasticsearch database
# 2. The class can query the database
# 3. The class can print the results of the query to the console

import unittest
from unittest.mock import patch
from main import ElasticSearch

class TestElasticSearch(unittest.TestCase):
    def setUp(self):
        self.es = ElasticSearch('localhost', '9200')
        self.index = 'my_index'
        self.query = '{"query": {"match_all": {}}}'
        
    def test_query(self):
        res = self.es.query(self.index, self.query)
        self.assertEqual(res['hits']['total']['value'], 3)
        
    def test_print_results(self):
        res = self.es.query(self.index, self.query)
        with patch('sys.stdout') as mock_stdout:
            self.es.print_results(res)
            self.assertTrue(mock_stdout.write.called)

if __name__ == '__main__':
    unittest.main()