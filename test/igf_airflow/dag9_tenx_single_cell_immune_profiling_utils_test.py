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
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import _check_bam_index_and_copy
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import _build_collection_attribute_data_for_cellranger

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
    with open(os.path.join(self.work_dir,'multiqc.txt'),'r') as fp:
      data = fp.read()
    data = data.split('\n')
    self.assertTrue(os.path.join(self.input_dir,'A') in data)
    self.assertTrue(os.path.join(self.input_dir,'B') in data)
    self.assertTrue(os.path.join(self.input_dir,'C') in data)


class Dag9_tenx_single_cell_immune_profiling_utilstestF(unittest.TestCase):
  def setUp(self):
    self.work_dir = get_temp_dir()
    self.input_dir = get_temp_dir()
    self.singularity_image = \
      os.path.join(self.input_dir,'samtools.sif')
    with open(self.singularity_image,'w') as fp:
      fp.write('A')
    self.bam_file = \
      os.path.join(self.input_dir,'samtools.sif')
    with open(self.bam_file,'w') as fp:
      fp.write('A')

  def tearDown(self):
    remove_dir(self.input_dir)
    remove_dir(self.work_dir)

  def test_check_bam_index_and_copy(self):
    list_of_tasks = [
      'taskA',
      'taskB']
    output_temp_dirs = \
      _check_bam_index_and_copy(
        samtools_exe='samtools',
        singularity_image=self.singularity_image,
        bam_file=self.bam_file,
        list_of_analysis=list_of_tasks,
        use_ephemeral_space=True,
        dry_run=True)
    self.assertEqual(len(output_temp_dirs.keys()),2)
    self.assertTrue('taskA' in output_temp_dirs.keys())
    self.assertTrue(os.path.exists(output_temp_dirs.get('taskA')))
    self.assertTrue('taskB' in output_temp_dirs.keys())
    self.assertTrue(os.path.exists(output_temp_dirs.get('taskB')))

class Dag9_tenx_single_cell_immune_profiling_utilstestG(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    vdj_data = [
      'Estimated Number of Cells,Mean Read Pairs per Cell,Number of Cells With Productive V-J Spanning Pair,Number of Read Pairs,Valid Barcodes,Q30 Bases in Barcode,Q30 Bases in RNA Read 1,Q30 Bases in UMI,Reads Mapped to Any V(D)J Gene,Reads Mapped to TRA,Reads Mapped to TRB,Mean Used Read Pairs per Cell,Fraction Reads in Cells,Median TRA UMIs per Cell,Median TRB UMIs per Cell,Cells With Productive V-J Spanning Pair,"Cells With Productive V-J Spanning (TRA, TRB) Pair",Paired Clonotype Diversity,Cells With TRA Contig,Cells With TRB Contig,Cells With CDR3-annotated TRA Contig,Cells With CDR3-annotated TRB Contig,Cells With V-J Spanning TRA Contig,Cells With V-J Spanning TRB Contig,Cells With Productive TRA Contig,Cells With Productive TRB Contig',
      '"2,561","16,056","1,824","41,120,098",93.8%,97.5%,80.1%,97.4%,85.3%,19.7%,65.4%,"11,443",81.1%,2.0,8.0,71.2%,71.2%,145.62,79.4%,98.6%,76.9%,97.9%,78.7%,97.9%,74.0%,97.2%']
    count_data = [
      'Estimated Number of Cells,Mean Reads per Cell,Median Genes per Cell,Number of Reads,Valid Barcodes,Sequencing Saturation,Q30 Bases in Barcode,Q30 Bases in RNA Read,Q30 Bases in UMI,Reads Mapped to Genome,Reads Mapped Confidently to Genome,Reads Mapped Confidently to Intergenic Regions,Reads Mapped Confidently to Intronic Regions,Reads Mapped Confidently to Exonic Regions,Reads Mapped Confidently to Transcriptome,Reads Mapped Antisense to Gene,Fraction Reads in Cells,Total Genes Detected,Median UMI Counts per Cell',
      '"5,819","24,988","1,186","145,406,860",91.1%,78.2%,97.6%,86.1%,97.5%,96.1%,88.0%,1.1%,7.8%,79.1%,69.6%,4.1%,93.7%,"22,037","2,977"']
    self.vdj_metrics_file = os.path.join(self.temp_dir,'vdj_metrics_summary.csv')
    self.count_metrics_file = os.path.join(self.temp_dir,'count_metrics_summary.csv')
    with open(self.vdj_metrics_file,'w') as fp:
      for i in vdj_data:
        fp.write(i+'\n')
    with open(self.count_metrics_file,'w') as fp:
      for i in count_data:
        fp.write(i+'\n')

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_build_collection_attribute_data_for_cellranger(self):
    vdj_t_attribute_data = \
      _build_collection_attribute_data_for_cellranger(
        metrics_file=self.vdj_metrics_file,
        collection_name='TEST',
        collection_type='TEST',
        attribute_name='attribute_name',
        attribute_value='attribute_value',
        attribute_prefix='VDJ_T')
    gex_attribute_data = \
      _build_collection_attribute_data_for_cellranger(
        metrics_file=self.count_metrics_file,
        collection_name='TEST',
        collection_type='TEST',
        attribute_name='attribute_name',
        attribute_value='attribute_value',
        attribute_prefix='COUNT')
    vdj_t_attribute_data = \
      pd.DataFrame(vdj_t_attribute_data)
    vdj_cell_count = \
      vdj_t_attribute_data[
        vdj_t_attribute_data['attribute_name']=='VDJ_T_Estimated_Number_of_Cells']\
        ['attribute_value'].values[0]
    self.assertEqual(vdj_cell_count,'2561')
    gex_attribute_data = \
      pd.DataFrame(gex_attribute_data)
    gex_cell_count = \
      gex_attribute_data[
        gex_attribute_data['attribute_name']=='COUNT_Estimated_Number_of_Cells']\
        ['attribute_value'].values[0]
    self.assertEqual(gex_cell_count,'5819')

if __name__=='__main__':
  unittest.main()