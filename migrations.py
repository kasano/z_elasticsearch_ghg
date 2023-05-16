from pandas import read_csv
from elasticsearch import Elasticsearch
import sys

def prepare_es_data(index, doc_type, df):
  records = df.to_dict(orient="records")
  es_data = []
  for idx, record in enumerate(records):
    meta_dict = {
          "index": {
              "_index": index, 
              # "_type": doc_type, 
              "_id": idx
          }
      }
    es_data.append(meta_dict)
    es_data.append(record)
  return es_data

def index_es_data(index, es_data,es_client):
  if es_client.indices.exists(index=index):
      print("deleting the '{}' index.".format(index))
      res = es_client.indices.delete(index=index)
      print("Response from server: {}".format(res))

  print("creating the '{}' index.".format(index))
  res = es_client.indices.create(index=index)
  print("Response from server: {}".format(res))

  print("bulk index the data")
  res = es_client.bulk(index=index, body=es_data, refresh = True)
  print("Errors: {}, Num of records indexed: {}".format(res["errors"], len(res["items"])))
  

if __name__ == '__main__':
    ELASTIC_PASSWORD = sys.argv[1]
    CLOUD_ID = sys.argv[2]
    client = Elasticsearch(
      cloud_id=CLOUD_ID,
      basic_auth=("elastic", ELASTIC_PASSWORD)
    )
    df = read_csv('factors.csv',header='infer',index_col=False,low_memory=False)
    df_migrate = df[
       (df['Gas']=='CARBON DIOXIDE')
    ][['Description']].reset_index(drop=True)
    
    train_es_data = prepare_es_data(index="development", doc_type="ghg", df=df_migrate)
    
    index_es_data(index="development", es_data=train_es_data, es_client=client )
