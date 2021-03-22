import unittest,os
import pandas as pd
from collections import defaultdict
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
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import _create_library_csv_for_cellranger_multi
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import _get_fastq_and_run_cutadapt_trim
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import _configure_and_run_multiqc


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
      "crisper_guide_capture"]
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

class Dag9_tenx_single_cell_immune_profiling_utilstestC(unittest.TestCase):
  def setUp(self):
    self.work_dir = get_temp_dir()
  def tearDown(self):
    remove_dir(self.work_dir)
  def test_create_library_csv_for_cellranger_multi(self):
    analysis_info = {
      'antibody_capture': {
        'sample_igf_id': 'IGF003', 
        'sample_name': 'IGF003-FB', 
        'run_count': 1, 
        'runs': {
          '0': {
            'run_igf_id': 'run3', 
            'fastq_dir': '/path/IGF003/run3', 
            'output_path': '/tmp/tempox6tw1kv/antibody_capture/IGF003/run3'}}}, 
      'gene_expression': {
        'sample_igf_id': 'IGF001', 
        'sample_name': 'IGF001-GEX', 
        'run_count': 1, 
        'runs': {
          '0': {
            'run_igf_id': 'run1', 
            'fastq_dir': '/path/IGF001/run1', 
            'output_path': '/tmp/tempox6tw1kv/gene_expression/IGF001/run1'}}}, 
      'vdj': {
        'sample_igf_id': 'IGF002', 
        'sample_name': 'IGF002-VDJ', 
        'run_count': 1, 
        'runs': {
          '0': {
            'run_igf_id': 'run2', 
            'fastq_dir': '/path/IGF002/run2', 
            'output_path': '/tmp/tempox6tw1kv/vdj/IGF002/run2'}}}}
    analysis_description = [{
      'sample_igf_id':'IGF001',
      'feature_type':'gene expression',
      'reference':'g.csv'
    },{
      'sample_igf_id':'IGF002',
      'feature_type':'vdj',
      'reference':'v.csv'
    },{
      'sample_igf_id':'IGF003',
      'feature_type':'antibody capture',
      'reference':'f.csv'
    }]
    csv_path = \
      _create_library_csv_for_cellranger_multi(
        analysis_description=analysis_description,
        analysis_info=analysis_info,
        work_dir=self.work_dir)
    self.assertEqual(csv_path,os.path.join(self.work_dir,'library.csv'))
    data = defaultdict(list)
    header = ''
    with open(csv_path, 'r') as f:
      for i in f:
        row = i.rstrip('\n')
        if row.startswith('['):
          header = row.split(',')[0].strip('[').strip(']')
        else:
          data[header].append(row)
    gex_data = data.get('gene-expression')
    self.assertEqual(gex_data[0],'reference,g.csv')
    vdj_data = data.get('vdj')
    self.assertEqual(vdj_data[0],'reference,v.csv')
    feature_data = data.get('feature')
    self.assertEqual(feature_data[0],'reference,f.csv')
    lib_data = data.get('libraries')
    self.assertEqual(lib_data[0].split(',')[0],'fastq_id')
    self.assertEqual(lib_data[0].split(',')[1],'fastqs')
    self.assertEqual(lib_data[1].split(',')[0],'IGF003-FB')
    self.assertEqual(lib_data[1].split(',')[1],'/tmp/tempox6tw1kv/antibody_capture/IGF003/run3')

class Dag9_tenx_single_cell_immune_profiling_utilstestD(unittest.TestCase):
  def setUp(self):
    self.work_dir = get_temp_dir()
    self.output_dir = get_temp_dir()
    self.analysis_description = [{
        'sample_igf_id':'IGF001',
        'feature_type':'gene expression',
        'reference':'g.csv'
      },{
        'sample_igf_id':'IGF002',
        'feature_type':'vdj',
        'reference':'v.csv'
      },{
        'sample_igf_id':'IGF003',
        'feature_type':'antibody capture',
        'reference':'f.csv'
      }]
    self.analysis_info = {
      'antibody_capture': {
        'sample_igf_id': 'IGF003',
        'sample_name': 'IGF003-FB',
        'run_count': 1,
        'runs': {
          '0': {
            'run_igf_id': 'run3',
            'fastq_dir': os.path.join(self.work_dir,'IGF003','run3'),
            'output_path': os.path.join(self.output_dir,'antibody_capture','IGF003','run3')}}},
      'gene_expression': {
        'sample_igf_id': 'IGF001',
        'sample_name': 'IGF001-GEX',
        'run_count': 1,
        'runs': {
          '0': {
            'run_igf_id': 'run1',
            'fastq_dir': os.path.join(self.work_dir,'IGF001','run1'),
            'output_path': os.path.join(self.output_dir,'gene_expression','IGF001','run1')}}},
      'vdj': {
        'sample_igf_id': 'IGF002',
        'sample_name': 'IGF002-VDJ',
        'run_count': 1,
        'runs': {
          '0': {
            'run_igf_id': 'run2',
            'fastq_dir': os.path.join(self.work_dir,'IGF002','run2'),
            'output_path': os.path.join(self.output_dir,'vdj','IGF002','run2')}}}}
    os.makedirs(os.path.join(self.output_dir,'antibody_capture','IGF003','run3'),exist_ok=True)
    os.makedirs(os.path.join(self.output_dir,'gene_expression','IGF001','run1'),exist_ok=True)
    os.makedirs(os.path.join(self.output_dir,'vdj','IGF002','run2'),exist_ok=True)
    for i in ('R1','R2','I1'):
      dir_path1 = os.path.join(self.work_dir,'IGF003','run3')
      os.makedirs(dir_path1,exist_ok=True)
      fastq1_path = \
        os.path.join(dir_path1,'IGF003_S1_L001_{0}_001.fastq.gz'.format(i))
      dir_path2 = os.path.join(self.work_dir,'IGF001','run1')
      os.makedirs(dir_path2,exist_ok=True)
      fastq2_path = \
        os.path.join(dir_path2,'IGF001_S1_L001_{0}_001.fastq.gz'.format(i))
      dir_path3 = os.path.join(self.work_dir,'IGF002','run2')
      os.makedirs(dir_path3,exist_ok=True)
      fastq3_path = \
        os.path.join(dir_path3,'IGF002_S1_L001_{0}_001.fastq.gz'.format(i))
      with open(fastq1_path,'w') as fp:
        fp.write('AAAA')
      with open(fastq2_path,'w') as fp:
        fp.write('AAAA')
      with open(fastq3_path,'w') as fp:
        fp.write('AAAA')

  def tearDown(self):
    remove_dir(self.work_dir)

  def test_get_fastq_and_run_cutadapt_trim1(self):
    analysis_name = 'gene_expression'
    fastq_input_dir_tag = 'fastq_dir'
    fastq_output_dir_tag = 'output_path'
    vdj_data = self.analysis_info.get(analysis_name)
    self.assertTrue(vdj_data is not None)
    _get_fastq_and_run_cutadapt_trim(
      analysis_info=self.analysis_info,
      analysis_name=analysis_name,
      analysis_description=self.analysis_description,
      run_id=0,
      r1_length=0,
      r2_length=0,
      dry_run=True,
      fastq_input_dir_tag=fastq_input_dir_tag,
      fastq_output_dir_tag=fastq_output_dir_tag,
      singularity_image=None)
    gex_output_path = \
      os.path.join(self.output_dir,'gene_expression','IGF001','run1')
    self.assertTrue('IGF001_S1_L001_R1_001.fastq.gz' in os.listdir(gex_output_path))
    self.assertTrue('IGF001_S1_L001_R2_001.fastq.gz' in os.listdir(gex_output_path))
    self.assertTrue('IGF001_S1_L001_I1_001.fastq.gz' in os.listdir(gex_output_path))

  def test_get_fastq_and_run_cutadapt_trim2(self):
    analysis_name = 'vdj'
    fastq_input_dir_tag = 'fastq_dir'
    fastq_output_dir_tag = 'output_path'
    vdj_data = self.analysis_info.get(analysis_name)
    self.assertTrue(vdj_data is not None)
    _get_fastq_and_run_cutadapt_trim(
      analysis_info=self.analysis_info,
      analysis_name=analysis_name,
      analysis_description=self.analysis_description,
      run_id=0,
      r1_length=26,
      r2_length=0,
      dry_run=True,
      fastq_input_dir_tag=fastq_input_dir_tag,
      fastq_output_dir_tag=fastq_output_dir_tag,
      singularity_image=None)
    vdj_output_path = \
      os.path.join(self.output_dir,'vdj','IGF002','run2')
    self.assertFalse('IGF002_S1_L001_R1_001.fastq.gz' in os.listdir(vdj_output_path))
    self.assertTrue('IGF002_S1_L001_R2_001.fastq.gz' in os.listdir(vdj_output_path))
    self.assertTrue('IGF002_S1_L001_I1_001.fastq.gz' in os.listdir(vdj_output_path))


class Dag9_tenx_single_cell_immune_profiling_utilstestE(unittest.TestCase):
  def setUp(self):
    self.work_dir = get_temp_dir()
    self.input_dir = get_temp_dir()
    self.singularity_image = \
      os.path.join(self.input_dir,'multiqc.sif')
    with open(self.singularity_image,'w') as fp:
      fp.write('A')
    self.analysis_paths_list = [
      os.path.join(self.input_dir,'A'),
      os.path.join(self.input_dir,'B'),
      os.path.join(self.input_dir,'C')]
    with open(os.path.join(self.input_dir,'A'),'w') as fp:
      fp.write('A')
    with open(os.path.join(self.input_dir,'B'),'w') as fp:
      fp.write('A')
    with open(os.path.join(self.input_dir,'C'),'w') as fp:
      fp.write('A')
    self.multiqc_template_file = \
      os.path.join('template','multiqc_report','multiqc_config.yaml')

  def tearDown(self):
    remove_dir(self.input_dir)
    remove_dir(self.work_dir)

  def test_configure_and_run_multiqc(self):
    multiqc_html,multiqc_data,cmd = \
      _configure_and_run_multiqc(
        analysis_paths_list=self.analysis_paths_list,
        project_igf_id='test_project',
        sample_igf_id='test_sample',
        work_dir=self.work_dir,
        genome_build='HG38',
        multiqc_template_file=self.multiqc_template_file,
        tool_order_list=[],
        singularity_mutiqc_image=self.singularity_image,
        multiqc_params=['--zip-data-dir'],
        dry_run=True)
    self.assertTrue(multiqc_html is None)
    self.assertTrue(multiqc_data is None)
    input_file_list = os.path.join(self.work_dir,'multiqc.txt')
    self.assertTrue('singularity exec --bind')
    self.assertTrue(self.singularity_image in cmd)
    self.assertTrue('--file-list {0}'.format(input_file_list) in cmd)
    self.assertTrue('--zip-data-dir' in cmd)


if __name__=='__main__':
  unittest.main()