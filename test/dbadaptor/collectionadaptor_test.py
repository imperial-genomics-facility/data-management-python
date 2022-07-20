import pandas as pd
import unittest, json, os, shutil, stat
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base, Collection, Collection_attribute
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import remove_dir

class CollectionAdaptor_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = None
    with open(self.dbconfig, 'r') as json_data:
      dbparam = json.load(json_data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.session_class

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_load_file_and_create_collection(self):
    file_path = \
      'data/collect_fastq_dir/1_16/IGFP0001_test_22-8-2017_rna/IGF00002/IGF00002-2_S1_L001_R1_001.fastq.gz'
    data = [{
      'name' : 'IGF00001_MISEQ_000000000-D0YLK_1',
      'type' : 'demultiplexed_fastq',
      'table' : 'run',
      'file_path' : file_path,
      'location' : 'HPC_PROJECT'}]
    ca = CollectionAdaptor(**{'session_class': self.session_class})
    ca.start_session()
    ca.load_file_and_create_collection(data)
    ca.close_session()
    fa = FileAdaptor(**{'session_class' : self.session_class})
    fa.start_session()
    file_data = fa.fetch_file_records_file_path(file_path)
    fa.close_session()
    file_md5 = file_data.md5
    self.assertEqual(file_md5, 'd5798564a14a09b9e80640ac2f42f47e')

  def test_fetch_collection_name_and_table_from_file_path(self):
    file_path = \
      'data/collect_fastq_dir/1_16/IGFP0001_test_22-8-2017_rna/IGF00002/IGF00002-2_S1_L001_R1_001.fastq.gz'
    data = [{
      'name' : 'IGF00001_MISEQ_000000000-D0YLK_1',
      'type' : 'demultiplexed_fastq',
      'table' : 'run',
      'file_path' : file_path,
      'location' : 'HPC_PROJECT'}]
    ca = CollectionAdaptor(**{'session_class' : self.session_class})
    ca.start_session()
    ca.load_file_and_create_collection(data)
    (name,_)=ca.fetch_collection_name_and_table_from_file_path(file_path)
    ca.close_session()
    self.assertEqual(name, 'IGF00001_MISEQ_000000000-D0YLK_1')

  def test_remove_collection_group_info(self):
    collection_data = [{
      'name' : 'IGF001_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'file_path' : 'a.cram' }, {
      'name' : 'IGF001_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'file_path' : 'a1.cram' }, {
      'name' : 'IGF003_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'file_path' : 'b.cram' }, {
      'name' : 'IGF003_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'file_path' : 'b1.cram'}]
    ca = CollectionAdaptor(**{'session_class' : self.session_class})
    ca.start_session()
    ca.load_file_and_create_collection(
      data=collection_data,
      calculate_file_size_and_md5=False)
    remove_data_list1 = [{
      'name' : 'IGF001_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'file_path' : 'a.cram'}]
    ca.remove_collection_group_info(
      data=remove_data_list1)
    cg_data = \
      ca.get_collection_files(
        collection_name='IGF001_MISEQ',
        collection_type='ALIGNMENT_CRAM',
        output_mode='dataframe')
    self.assertEqual(len(cg_data.index), 1)
    self.assertEqual(cg_data['file_path'].values[0], 'a1.cram')
    remove_data_list2 = [{
      'name' : 'IGF003_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment'}]
    ca.remove_collection_group_info(
      data=remove_data_list2)
    cg_data2 = \
      ca.get_collection_files(
        collection_name='IGF003_MISEQ',
        collection_type='ALIGNMENT_CRAM',
        output_mode='dataframe')
    self.assertEqual(len(cg_data2.index), 0)
    ca.close_session()

  def test_tag_existing_collection_data(self):
    collection_data = [{
      'name' : 'IGF001_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment'}, {
      'name' : 'IGF002_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table':'experiment'}]
    ca = CollectionAdaptor(**{'session_class':self.session_class})
    ca.start_session()
    ca.store_collection_and_attribute_data(
      data=collection_data,
      autosave=True)
    collection_data2 = [{
      'name' : 'IGF001_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment'}, {
      'name' : 'IGF003_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment'}]
    collection_data2 = \
      pd.DataFrame(collection_data2)
    collection_data2['data_exists'] = ''
    collection_data2 = \
      collection_data2.apply(
        lambda x: \
          ca._tag_existing_collection_data(
            data=x,
            tag='EXISTS',
            tag_column='data_exists'),
        axis=1,
        result_type=None)                                                       # tag existing collections
    ca_exists = \
      collection_data2[collection_data2['data_exists']=='EXISTS']
    ca_not_exists = \
      collection_data2[collection_data2['data_exists']!='EXISTS']
    self.assertEqual(ca_exists['name'].values[0], 'IGF001_MISEQ')
    self.assertEqual(ca_not_exists['name'].values[0], 'IGF003_MISEQ')
    ca.close_session()


class CollectionAdaptor_test2(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = None
    with open(self.dbconfig, 'r') as json_data:
      dbparam = json.load(json_data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.session_class

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_store_collection_and_attribute_data(self):
    collection_data = [{
      'name' : 'IGF001_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 1000}, {
      'name' : 'IGF002_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 2000}, {
      'name' : 'IGF003_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 3000}, {
      'name' : 'IGF004_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 4000}]
    ca = CollectionAdaptor(**{'session_class':self.session_class})
    ca.start_session()
    ca.store_collection_and_attribute_data(\
      data=collection_data,
      autosave=True)
    query = \
      ca.session.\
        query(
          Collection,
          Collection_attribute.attribute_name,
          Collection_attribute.attribute_value).\
        join(Collection_attribute).\
        filter(Collection.collection_id==Collection_attribute.collection_id).\
        filter(Collection.name=='IGF001_MISEQ').\
        filter(Collection.type=='ALIGNMENT_CRAM')
    results = \
      ca.fetch_records(
        query=query,
        output_mode='dataframe')
    self.assertEqual(results['attribute_value'].astype(int).values[0], 1000)
    self.assertEqual(results['attribute_name'].values[0], 'read_count')

  def test_check_collection_attribute(self):
    collection_data = [{
      'name' : 'IGF001_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 1000}, {
      'name' : 'IGF002_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 2000}, {
      'name' : 'IGF003_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 3000}, {
      'name' : 'IGF004_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 4000}]
    ca = \
      CollectionAdaptor(**{'session_class' : self.session_class})
    ca.start_session()
    ca.store_collection_and_attribute_data(
      data=collection_data,
      autosave=True)
    collection_exists = \
      ca.check_collection_attribute(
        collection_name='IGF004_MISEQ',
        collection_type='ALIGNMENT_CRAM',
        attribute_name='read_count')
    self.assertTrue(collection_exists)
    collection_exists = \
      ca.check_collection_attribute(
        collection_name='IGF004_MISEQ',
        collection_type='ALIGNMENT_CRAM',
        attribute_name='read_counts')
    self.assertFalse(collection_exists)

  def test_update_collection_attribute(self):
    collection_data = [{
      'name' : 'IGF001_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 1000}, {
      'name' : 'IGF002_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 2000}, {
      'name' : 'IGF003_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 3000}, {
      'name' : 'IGF004_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 4000}]
    ca = \
      CollectionAdaptor(**{'session_class' : self.session_class})
    ca.start_session()
    ca.store_collection_and_attribute_data(
      data=collection_data,
      autosave=True)
    query = \
      ca.session.\
        query(
          Collection,
          Collection_attribute.attribute_name,
          Collection_attribute.attribute_value).\
        join(Collection_attribute).\
        filter(Collection.collection_id==Collection_attribute.collection_id).\
        filter(Collection.name=='IGF001_MISEQ').\
        filter(Collection.type=='ALIGNMENT_CRAM')
    results = \
      ca.fetch_records(
        query=query,
        output_mode='dataframe')
    self.assertEqual(results['attribute_value'].astype(int).values[0], 1000)
    ca.update_collection_attribute(
      collection_name='IGF001_MISEQ',
      collection_type='ALIGNMENT_CRAM',
      attribute_name='read_count',
      attribute_value='2000',
      autosave=True)
    results = \
      ca.fetch_records(
        query=query,
        output_mode='dataframe')
    self.assertEqual(results['attribute_value'].astype(int).values[0], 2000)

  def test_create_or_update_collection_attributes(self):
    collection_data = [{
      'name' : 'IGF001_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 1000}, {
      'name' : 'IGF002_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 2000}, {
      'name' : 'IGF003_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 3000}, {
      'name' : 'IGF004_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'table' : 'experiment',
      'read_count' : 4000}]
    ca = \
      CollectionAdaptor(**{'session_class' : self.session_class})
    ca.start_session()
    ca.store_collection_and_attribute_data(
      data=collection_data,
      autosave=True)
    query = \
      ca.session.\
        query(
          Collection,
          Collection_attribute.attribute_name,
          Collection_attribute.attribute_value).\
        join(Collection_attribute).\
        filter(Collection.collection_id==Collection_attribute.collection_id).\
        filter(Collection.name=='IGF001_MISEQ').\
        filter(Collection.type=='ALIGNMENT_CRAM')
    results = \
      ca.fetch_records(
        query=query,
        output_mode='dataframe')
    self.assertEqual(results['attribute_value'].astype(int).values[0], 1000)
    data = [{
      'name' : 'IGF001_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'attribute_name' : 'read_count',
      'attribute_value' : '2000'}]
    ca.create_or_update_collection_attributes(
      data=data,
      autosave=True)
    results = \
      ca.fetch_records(
        query=query,
        output_mode='dataframe')
    self.assertEqual(results['attribute_value'].astype(int).values[0], 2000)
    self.assertFalse('read_count2' in results['attribute_name'].values)
    data = [{
      'name' : 'IGF001_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'attribute_name' : 'read_count2',
      'attribute_value' : '10000'}]
    ca.create_or_update_collection_attributes(
      data=data,
      autosave=True)
    query = \
      ca.session.\
        query(
          Collection,
          Collection_attribute.attribute_name,
          Collection_attribute.attribute_value).\
        join(Collection_attribute).\
        filter(Collection.collection_id==Collection_attribute.collection_id).\
        filter(Collection.name=='IGF001_MISEQ').\
        filter(Collection.type=='ALIGNMENT_CRAM')
    results = \
      ca.fetch_records(
        query=query,
        output_mode='dataframe')
    self.assertTrue('read_count2' in results['attribute_name'].values)
    data = [{
      'name' : 'IGF007_MISEQ',
      'type' : 'ALIGNMENT_CRAM',
      'attribute_name' : 'read_count2',
      'attribute_value' : '10000'}]
    with self.assertRaises(ValueError):
      ca.create_or_update_collection_attributes(
        data=data,
        autosave=True)
    query = \
      ca.session.\
        query(
          Collection,
          Collection_attribute.attribute_name,
          Collection_attribute.attribute_value).\
        join(Collection_attribute).\
        filter(Collection.collection_id==Collection_attribute.collection_id).\
        filter(Collection.name=='IGF007_MISEQ').\
        filter(Collection.type=='ALIGNMENT_CRAM')
    results = \
      ca.fetch_records(
        query=query,
        output_mode='dataframe')
    self.assertEqual(len(results.index),0)


class CollectionAdaptor_test3(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = None
    with open(self.dbconfig, 'r') as json_data:
      dbparam = json.load(json_data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.session_class
    self.temp_dir = get_temp_dir()
    file_list = [
      'sample1/sample1_S1_R1_001.fastq.gz',
      'sample1/sample1_S1_R2_001.fastq.gz',
      'sample1/sample1_S1_I1_001.fastq.gz',
      'sample1/sample1_S1_I2_001.fastq.gz']
    collection_list = list()
    for entry in file_list:
      file_path = \
        os.path.join(self.temp_dir, entry)
      os.makedirs(
        os.path.dirname(file_path),
        exist_ok=True)
      with open(file_path, 'w') as f:
        f.write('A')
      os.chmod(
        file_path,
        stat.S_IRUSR)
      collection_list.append({
        'name' : 'run1',
        'type' : 'demultiplexed_fastq',
        'table' : 'run',
        'file_path' : file_path,
        'location' : 'HPC_PROJECT'})
    file_list = [
      'sample2/sample2_S2_R1_001.fastq.gz']
    for entry in file_list:
      file_path = \
        os.path.join(self.temp_dir, entry)
      os.makedirs(
        os.path.dirname(file_path),
        exist_ok=True)
      with open(file_path, 'w') as f:
        f.write('A')
      os.chmod(
        file_path,
        stat.S_IRUSR)
      collection_list.append({
        'name' : 'run2',
        'type' : 'demultiplexed_fastq',
        'table' : 'run',
        'file_path' : file_path,
        'location' : 'HPC_PROJECT'})
    file_list = [
      'sample3/sample3_S2_R1_001.fastq.gz']
    for entry in file_list:
      file_path = \
        os.path.join(self.temp_dir, entry)
      os.makedirs(
        os.path.dirname(file_path),
        exist_ok=True)
      with open(file_path, 'w') as f:
        f.write('A')
      os.chmod(
        file_path,
        stat.S_IRUSR)
      collection_list.append({
        'name' : 'Prun1',
        'type' : 'demultiplexed_fastq',
        'table' : 'run',
        'file_path' : file_path,
        'location' : 'HPC_PROJECT'})
    ca = CollectionAdaptor(**{'session_class': self.session_class})
    ca.start_session()
    ca.load_file_and_create_collection(
      collection_list,
      calculate_file_size_and_md5=False)
    ca.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    if os.path.exists(self.temp_dir):
      remove_dir(self.temp_dir)

  def test_cleanup_collection_and_file_for_name_and_type(self):
    ca = CollectionAdaptor(**{'session_class': self.session_class})
    ca.start_session()
    ca.cleanup_collection_and_file_for_name_and_type(
      collection_name='run1',
      collection_type='demultiplexed_fastq',
      autosave=True,
      remove_files_on_disk=True)
    ca.close_session()
    ca.start_session()
    files = \
      ca.get_collection_files(
        collection_name='run1',
        collection_type='demultiplexed_fastq')
    self.assertEqual(len(files.index), 0)
    files = \
      ca.get_collection_files(
        collection_name='run2',
        collection_type='demultiplexed_fastq')
    self.assertEqual(len(files.index), 1)
    ca.close_session()
    self.assertFalse(
      os.path.exists(
        os.path.join(
          self.temp_dir,
          'sample1/sample1_S1_R1_001.fastq.gz')
      ))
    self.assertTrue(
      os.path.exists(
        os.path.join(
          self.temp_dir,
          'sample2/sample2_S2_R1_001.fastq.gz')
      ))
    ca.start_session()
    ca.cleanup_collection_and_file_for_name_and_type(
      collection_name='run2',
      collection_type='demultiplexed_fastq',
      autosave=True,
      remove_files_on_disk=False)
    ca.close_session()
    ca.start_session()
    files = \
      ca.get_collection_files(
        collection_name='run2',
        collection_type='demultiplexed_fastq')
    self.assertEqual(len(files.index), 0)
    ca.close_session()
    self.assertTrue(
      os.path.exists(
        os.path.join(
          self.temp_dir,
          'sample2/sample2_S2_R1_001.fastq.gz')
      ))

  def test_cleanup_collection_and_file_for_name_and_type2(self):
    ca = CollectionAdaptor(**{'session_class': self.session_class})
    ca.start_session()
    with self.assertRaises(Exception):
      ca.cleanup_collection_and_file_for_name_and_type(
        collection_name='run',
        collection_type='demultiplexed_fastq',
        partial_collection_name=True,
        min_collection_name_length=10,
        autosave=True,
        remove_files_on_disk=True)
    ca.cleanup_collection_and_file_for_name_and_type(
      collection_name='run',
      collection_type='demultiplexed_fastq',
      partial_collection_name=True,
      min_collection_name_length=3,
      autosave=True,
      remove_files_on_disk=True)
    ca.close_session()
    ca.start_session()
    files = \
      ca.get_collection_files(
        collection_name='Prun1',
        collection_type='demultiplexed_fastq')
    self.assertEqual(len(files.index), 1)
    ca.close_session()

if __name__=='__main__':
  unittest.main()