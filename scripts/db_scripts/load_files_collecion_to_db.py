import argparse,os
from igf_data.utils.dbutils import read_dbconf_json,read_json_data
from igf_data.igfdb.collectionadaptor import CollectionAdaptor

'''
A script for loading file collections to the database

:param collection_file_data: A json file with following entries

                            name (required)
                            type (required)
                            table
                            file_path (required)
                            location
                            size
                            md5

:param dbconfig_path: A database configuration file
:param calculate_checksum: Toggle file checksum calculation
'''

parser=argparse.ArgumentParser()
parser.add_argument('-f','--collection_file_data', required=True, help='Collection file data json file')
parser.add_argument('-d','--dbconfig_path', required=True, help='Database configuration json file')
parser.add_argument('-s','--calculate_checksum', default=False, action='store_true', help='Toggle file checksum calculation')
args=parser.parse_args()

dbconfig_path=args.dbconfig_path
collection_file_data=args.collection_file_data
calculate_checksum=args.calculate_checksum

try:
  dbconnected=False
  if not os.path.exists(dbconfig_path):
    raise IOError('Dbconfig file {0} not found'.format(dbconfig_path))

  if not os.path.exists(collection_file_data):
    raise IOError('Collection data json file {0} not found'.format(collection_file_data))

  dbparam=read_dbconf_json(dbconfig_path)                                       # read db config
  collection_data=read_json_data(collection_file_data)                          # read collection data json
  ca=CollectionAdaptor(**dbparam)
  ca.start_session()                                                            # connect to database
  dbconnected=True
  ca.load_file_and_create_collection(data=collection_data,
                                     calculate_file_size_and_md5=calculate_checksum,
                                     autosave=True)                             # load data and commit changes
  ca.close_session()
  dbconnected=False
except Exception as e:
  if dbconnected:
    ca.rollback_session()
    ca.close_session()
  print('Error: {0}'.format(e))