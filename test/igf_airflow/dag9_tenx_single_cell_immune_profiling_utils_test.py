import unittest,os
import pandas as pd
from igf_data.utils.fileutils import get_temp_dir,remove_dir
import sqlalchemy
from sqlalchemy import create_engine
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.igfTables import Base,File,Collection,Collection_group
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import _validate_analysis_description
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import _fetch_formatted_analysis_description
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import _add_reference_genome_path_for_analysis

class Dag9_tenx_single_cell_immune_profiling_utilstestA(unittest.TestCase):
  def setUp(self):
    self.workdir = get_temp_dir()
    self.feature_types = [
      "gene_expression",
      "vdj",
      "vdj-t",
      "vdj-b",
      "antibody_capture",
      "antigen_capture",
      "crisper"]
  def tearDown(self):
    remove_dir(self.workdir)
  def test_validate_analysis_description(self):
    feature_ref = \
      os.path.join(self.workdir,'r.csv')
    with open(feature_ref,'w') as fp:
      fp.write('a,b,c,d,e')
    analysis_description = [{
        'sample_igf_id':'IGF0001',
        'feature_type':'Gene Expression'
    },{
        'sample_igf_id':'IGF002',
        'feature_type':'VDJ-T'
    },{
        'sample_igf_id':'IGF003',
        'feature_type':'Antibody Capture',
        'reference':feature_ref
    },{
        'sample_igf_id':'IGF0004',
        'feature_type':'Gene Expression'
    },{
        'sample_igf_id':'IGF0005',
        'feature_type':'vdj-t'
    },{
        'sample_igf_id':'IGF0006',
        'feature_type':'vdj_t'
    },{
        'sample_igf_id':'IGF0007',
        'feature_type':'vdj-b',
        'reference':'/path/a.csv'
    }]
    _,feature_list,msgs = \
      _validate_analysis_description(analysis_description,self.feature_types)
    errors = 0
    self.assertTrue('gene_expression' in feature_list)
    self.assertTrue('vdj-t' in feature_list)
    self.assertTrue('vdj_t' in feature_list)
    self.assertEqual(len(feature_list),5)
    for m in msgs:
      if 'IGF0001' in m:
        self.assertIn('IGF0001,IGF0004',m)
        errors += 1
      if 'IGF0006' in m:
        self.assertIn('vdj_t',m)
        errors += 1
      if '/path/a.csv' in m:
        self.assertIn('reference',m)
        errors += 1
    self.assertEqual(errors,3)

  def test_fetch_formatted_analysis_description(self):
    analysis_description = [{
      'sample_igf_id':'IGF001',
      'feature_type':'gene expression'
    },{
      'sample_igf_id':'IGF002',
      'feature_type':'vdj'
    },{
      'sample_igf_id':'IGF003',
      'feature_type':'antibody capture',
      'reference':'a.csv'
    }]
    fastq_run_list = [{
      'sample_igf_id':'IGF001',
      'run_igf_id':'run1',
      'file_path':'/path/IGF001/run1/IGF001-GEX_S1_L001_R1_001.fastq.gz'
    },{
      'sample_igf_id':'IGF001',
      'run_igf_id':'run1',
      'file_path':'/path/IGF001/run1/IGF001-GEX_S1_L001_R2_001.fastq.gz'
    },{
      'sample_igf_id':'IGF002',
      'run_igf_id':'run2',
      'file_path':'/path/IGF002/run2/IGF002-VDJ_S1_L001_R1_001.fastq.gz'
    },{
      'sample_igf_id':'IGF002',
      'run_igf_id':'run2',
      'file_path':'/path/IGF002/run2/IGF002-VDJ_S1_L001_R2 _001.fastq.gz'
    },{
      'sample_igf_id':'IGF003',
      'run_igf_id':'run3',
      'file_path':'/path/IGF003/run3/IGF003-FB_S1_L001_R1_001.fastq.gz'
    },{
      'sample_igf_id':'IGF003',
      'run_igf_id':'run3',
      'file_path':'/path/IGF003/run3/IGF003-FB_S1_L001_R2_001.fastq.gz'
    }]
    os.environ['EPHEMERAL']='/tmp'
    formatted_analysis_description = \
      _fetch_formatted_analysis_description(
        analysis_description=analysis_description,
        fastq_run_list=fastq_run_list)
    self.assertTrue('gene_expression' in formatted_analysis_description)
    gex_entry = formatted_analysis_description.get('gene_expression')
    self.assertEqual(gex_entry.get('sample_igf_id'),'IGF001')
    self.assertEqual(gex_entry.get('sample_name'),'IGF001-GEX')
    self.assertEqual(gex_entry.get('run_count'),1)
    self.assertTrue('0' in gex_entry.get('runs'))
    self.assertEqual(gex_entry.get('runs').get('0').get('run_igf_id'),'run1')
    self.assertEqual(gex_entry.get('runs').get('0').get('fastq_dir'),'/path/IGF001/run1')

  def test_fetch_formatted_analysis_description2(self):
    analysis_description = [{
      'sample_igf_id':'IGF001',
      'feature_type':'gene expression'
    },{
      'sample_igf_id':'IGF002',
      'feature_type':'vdj'
    },{
      'sample_igf_id':'IGF003',
      'feature_type':'antibody capture',
      'reference':'a.csv'
    }]
    fastq_run_list = [{
      'sample_igf_id':'IGF001',
      'run_igf_id':'run1',
      'file_path':'/path/IGF001/run1/IGF001-GEX_S1_L001_R1_001.fastq.gz'
    },{
      'sample_igf_id':'IGF001',
      'run_igf_id':'run1',
      'file_path':'/path/IGF001/run1/IGF001-GEX_S1_L001_R2_001.fastq.gz'
    },{
      'sample_igf_id':'IGF002',
      'run_igf_id':'run2',
      'file_path':'/path/IGF002/run2/IGF002-VDJ_S1_L001_R1_001.fastq.gz'
    },{
      'sample_igf_id':'IGF002',
      'run_igf_id':'run2',
      'file_path':'/path/IGF002/run2/IGF002-VDJ_S1_L001_R2 _001.fastq.gz'
    }]
    os.environ['EPHEMERAL']='/tmp'
    with self.assertRaises(ValueError):
      _ = \
        _fetch_formatted_analysis_description(
          analysis_description=analysis_description,
          fastq_run_list=fastq_run_list)

class Dag9_tenx_single_cell_immune_profiling_utilstestB(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    self.species_name = 'HG38'
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    collection_data = [
      {'name':self.species_name,'type':'TRANSCRIPTOME_TENX'},
      {'name':self.species_name,'type':'VDJ_TENX'},
      {'name':self.species_name,'type':'GENOME_FASTA'}]
    file_data = [
      {'file_path':'/path/HG38/TenX'},
      {'file_path':'/path/HG38/TenX_vdj'},
      {'file_path':'/path/HG38/fasta'}]
    collection_group_data = [
      {'name':self.species_name,'type':'TRANSCRIPTOME_TENX','file_path':'/path/HG38/TenX'},
      {'name':self.species_name,'type':'VDJ_TENX','file_path':'/path/HG38/TenX_vdj'},
      {'name':self.species_name,'type':'GENOME_FASTA','file_path':'/path/HG38/fasta'}]
    base.start_session()
    ca = CollectionAdaptor(**{'session':base.session})
    fa = FileAdaptor(**{'session':base.session})
    ca.store_collection_and_attribute_data(data=collection_data)
    fa.store_file_and_attribute_data(data=file_data)
    ca.create_collection_group(data=collection_group_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_add_reference_genome_path_for_analysis(self):
    analysis_description = [{
      'sample_igf_id':'IGF001',
      'feature_type':'gene expression',
      'reference_type':'TRANSCRIPTOME_TENX',
      'genome_build':'HG38'
    },{
      'sample_igf_id':'IGF002',
      'feature_type':'vdj',
      'reference_type':'VDJ_TENX',
      'genome_build':'HG38'
    },{
      'sample_igf_id':'IGF003',
      'feature_type':'antibody capture',
      'reference':'a.csv'
    },{
      'sample_igf_id':'IGF004',
      'feature_type':'vdj-t',
      'reference':'a.csv'
    }]
    analysis_description = \
      _add_reference_genome_path_for_analysis(
        database_config_file=self.dbconfig,
        analysis_description=analysis_description,
        genome_required=False)
    df = pd.DataFrame(analysis_description)
    ref = \
      df[df['sample_igf_id']=='IGF001'][['reference']].values[0]
    self.assertEqual(ref,'/path/HG38/TenX')
    ref = \
      df[df['sample_igf_id']=='IGF002'][['reference']].values[0]
    self.assertEqual(ref,'/path/HG38/TenX_vdj')
    ref = \
      df[df['sample_igf_id']=='IGF003'][['reference']].values[0]
    self.assertEqual(ref,'a.csv')
    analysis_description = [{
      'sample_igf_id':'IGF003',
      'feature_type':'antibody capture',
      'reference':'a.csv',
      'genome_build':'HG38'
    },{
      'sample_igf_id':'IGF004',
      'feature_type':'vdj-t',
      'reference':'a.csv'
    }]
    with self.assertRaises(ValueError):
      analysis_description = \
        _add_reference_genome_path_for_analysis(
          database_config_file=self.dbconfig,
          analysis_description=analysis_description,
          genome_required=True)
  
if __name__=='__main__':
  unittest.main()