from email import header
from tempfile import tempdir
import unittest,os
import pandas as pd
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.igfTables import Pipeline
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.utils.dbutils import read_json_data
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import remove_dir
from igf_nextflow.nextflow_utils.nextflow_input_formatter import (
    parse_sample_metadata_and_fetch_fastq,
    _make_nfcore_smrnaseq_input,
    format_nextflow_conf,
    prepare_nfcore_smrnaseq_input,
    prepare_nfcore_rnaseq_input,
    _make_nfcore_rnaseq_input,
    _make_nfcore_methylseq_input,
    prepare_nfcore_methylseq_input,
    prepare_nfcore_sarek_input,
    _make_nfcore_sarek_input,
    _make_nfcore_ampliseq_input,
    prepare_nfcore_ampliseq_input,
    _make_nfcore_atacseq_input,
    prepare_nfcore_atacseq_input,
    _make_nfcore_chipseq_input,
    prepare_nfcore_chipseq_input,
    _make_nfcore_cutandrun_input,
    prepare_nfcore_cutandrun_input,
    prepare_input_for_multiple_nfcore_pipeline,
    _check_and_set_col_order_for_nf_samplesheet
  )

class Prepare_nfcore_input_testA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.dbconfig = 'data/dbconfig.json'
    self.config_template_file = 'template/nextflow_template/nextflow_generic_conf_v0.0.1.conf'
    self.runner_template_file = 'template/nextflow_template/nfcore_generic_runner_v0.0.1.sh'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    base.start_session()
    platform_data = [{
      "platform_igf_id" : "K00345",
      "model_name" : "HISEQ4000",
      "vendor_name" : "ILLUMINA",
      "software_name" : "RTA",
      "software_version" : "RTA2"
    }]
    flowcell_rule_data = [{
      "platform_igf_id": "K00345",
      "flowcell_type": "HiSeq 3000/4000 SR",
      "index_1": "NO_CHANGE",
      "index_2": "NO_CHANGE" }, {
      "platform_igf_id": "K00345",
      "flowcell_type": "HiSeq 3000/4000 PE",
      "index_1": "NO_CHANGE",
      "index_2": "REVCOMP"
    }]
    pl = PlatformAdaptor(**{'session': base.session})
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    seqrun_data = [{
      'seqrun_igf_id': 'seqrunA',
      'flowcell_id': '000000000',
      'platform_igf_id': 'K00345',
      'flowcell': 'HISEQ4000'
    }]
    sra = SeqrunAdaptor(**{'session': base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    project_data = [{'project_igf_id': 'projectA'}]
    pa = ProjectAdaptor(**{'session': base.session})
    pa.store_project_and_attribute_data(data=project_data)
    sample_data = [{
      'sample_igf_id': 'sampleA',
      'project_igf_id': 'projectA' }, {
      'sample_igf_id': 'sampleB',
      'project_igf_id': 'projectA' }, {
      'sample_igf_id': 'sampleC',
      'project_igf_id': 'projectA',
    }]
    sa = SampleAdaptor(**{'session': base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    experiment_data = [{
      'project_igf_id': 'projectA',
      'sample_igf_id': 'sampleA',
      'experiment_igf_id': 'sampleA_HISEQ4000',
      'library_name': 'sampleA',
      'library_layout': 'PAIRED',
      'platform_name': 'HISEQ4000'}, {
      'project_igf_id': 'projectA',
      'sample_igf_id': 'sampleB',
      'experiment_igf_id': 'sampleB_HISEQ4000',
      'library_name': 'sampleB',
      'library_layout': 'PAIRED',
      'platform_name': 'HISEQ4000'}, {
      'project_igf_id': 'projectA',
      'sample_igf_id': 'sampleC',
      'experiment_igf_id': 'sampleC_HISEQ4000',
      'library_name': 'sampleC',
      'library_layout': 'PAIRED',
      'platform_name': 'HISEQ4000'
    }]
    ea = ExperimentAdaptor(**{'session': base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    run_data = [{
      'experiment_igf_id': 'sampleA_HISEQ4000',
      'seqrun_igf_id': 'seqrunA',
      'run_igf_id': 'sampleA_HISEQ4000_000000000_1',
      'lane_number': '1' }, {
      'experiment_igf_id': 'sampleA_HISEQ4000',
      'seqrun_igf_id': 'seqrunA',
      'run_igf_id': 'sampleA_HISEQ4000_000000000_2',
      'lane_number': '2' }, {
      'experiment_igf_id': 'sampleA_HISEQ4000',
      'seqrun_igf_id': 'seqrunA',
      'run_igf_id': 'sampleA_HISEQ4000_000000000_3',
      'lane_number': '3' }, {
      'experiment_igf_id': 'sampleB_HISEQ4000',
      'seqrun_igf_id': 'seqrunA',
      'run_igf_id': 'sampleB_HISEQ4000_000000000_1',
      'lane_number': '1' }, {
      'experiment_igf_id': 'sampleC_HISEQ4000',
      'seqrun_igf_id': 'seqrunA',
      'run_igf_id': 'sampleC_HISEQ4000_000000000_1',
      'lane_number': '1'
    }]
    ra = RunAdaptor(**{'session': base.session})
    ra.store_run_and_attribute_data(data=run_data)
    file_data = [{
      'file_path': '/path/sampleA_S1_L001_R1_001.fastq.gz',
      'location': 'HPC_PROJECT' }, {
      'file_path': '/path/sampleA_S1_L001_R2_001.fastq.gz',
      'location': 'HPC_PROJECT' }, {
      'file_path': '/path/sampleA_S1_L002_R1_001.fastq.gz',
      'location': 'HPC_PROJECT' }, {
      'file_path': '/path/sampleA_S1_L002_R2_001.fastq.gz',
      'location': 'HPC_PROJECT' }, {
      'file_path': '/path/sampleA_S1_L003_R1_001.fastq.gz',
      'location': 'HPC_PROJECT' }, {
      'file_path': '/path/sampleA_S1_L003_R2_001.fastq.gz',
      'location': 'HPC_PROJECT' }, {
      'file_path': '/path/sampleB_S1_L001_R1_001.fastq.gz',
      'location': 'HPC_PROJECT' }, {
      'file_path': '/path/sampleB_S1_L001_R2_001.fastq.gz',
      'location': 'HPC_PROJECT' }, {
      'file_path': '/path/sampleC_S1_L001_R1_001.fastq.gz',
      'location': 'HPC_PROJECT'}, {
      'file_path': '/path/sampleC_S1_L001_R2_001.fastq.gz',
      'location': 'HPC_PROJECT'
    }]
    fa = FileAdaptor(**{'session': base.session})
    fa.store_file_and_attribute_data(data=file_data)
    collection_data = [{
      'name': 'sampleA_HISEQ4000_000000000_1',
      'type': 'demultiplexed_fastq',
      'table': 'run' }, {
      'name': 'sampleA_HISEQ4000_000000000_2',
      'type': 'demultiplexed_fastq',
      'table': 'run' }, {
      'name': 'sampleA_HISEQ4000_000000000_3',
      'type': 'demultiplexed_fastq',
      'table': 'run' }, {
      'name': 'sampleB_HISEQ4000_000000000_1',
      'type': 'demultiplexed_fastq',
      'table': 'run' }, {
      'name': 'sampleC_HISEQ4000_000000000_1',
      'type': 'demultiplexed_fastq',
      'table': 'run'
    }]
    collection_files_data = [{
      'name': 'sampleA_HISEQ4000_000000000_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleA_S1_L001_R1_001.fastq.gz'}, {
      'name': 'sampleA_HISEQ4000_000000000_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleA_S1_L001_R2_001.fastq.gz'}, {
      'name': 'sampleA_HISEQ4000_000000000_2',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleA_S1_L002_R1_001.fastq.gz'}, {
      'name': 'sampleA_HISEQ4000_000000000_2',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleA_S1_L002_R2_001.fastq.gz'}, {
      'name': 'sampleA_HISEQ4000_000000000_3',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleA_S1_L003_R1_001.fastq.gz'}, {
      'name': 'sampleA_HISEQ4000_000000000_3',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleA_S1_L003_R2_001.fastq.gz'}, {
      'name': 'sampleB_HISEQ4000_000000000_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleB_S1_L001_R1_001.fastq.gz'}, {
      'name': 'sampleB_HISEQ4000_000000000_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleB_S1_L001_R2_001.fastq.gz'}, {
      'name': 'sampleC_HISEQ4000_000000000_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleC_S1_L001_R1_001.fastq.gz'}, {
      'name': 'sampleC_HISEQ4000_000000000_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleC_S1_L001_R2_001.fastq.gz'
    }]
    ca = CollectionAdaptor(**{'session': base.session})
    ca.store_collection_and_attribute_data(data=collection_data)
    ca.create_collection_group(data=collection_files_data)
    base.close_session()

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_parse_sample_metadata_and_fetch_fastq(self):
    sample_metadata = {
      'sampleA': '',
      'sampleB': ''
    }
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=self.dbconfig)
    samples = fastq_df['sample_igf_id'].drop_duplicates().values.tolist()
    self.assertEqual(len(samples), 2)
    self.assertTrue('sampleA' in samples)
    self.assertTrue('sampleB' in samples)
    sampleb_fastq = fastq_df[fastq_df['sample_igf_id'] == 'sampleB']['file_path'].values.tolist()
    self.assertEqual(len(sampleb_fastq), 2)
    self.assertTrue('/path/sampleB_S1_L001_R1_001.fastq.gz' in sampleb_fastq)

  def test_make_nfcore_smrnaseq_input(self):
    sample_metadata = {
      'sampleA': '',
      'sampleB': ''
    }
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=self.dbconfig)
    input_csv_file = \
      _make_nfcore_smrnaseq_input(
        sample_metadata=sample_metadata,
        fastq_df=fastq_df,
        output_dir=self.temp_dir)
    self.assertTrue(os.path.exists(input_csv_file))
    csv_data = pd.read_csv(input_csv_file, sep=",", header=0)
    self.assertTrue('sample' in csv_data)
    self.assertTrue('fastq_1' in csv_data)
    sampleA_fastq = \
      csv_data[csv_data['sample']=='sampleA']['fastq_1'].\
        values.tolist()
    self.assertEqual(len(sampleA_fastq), 3)
    self.assertTrue('/path/sampleA_S1_L001_R1_001.fastq.gz' in sampleA_fastq)
    sampleB_fastq = \
      csv_data[csv_data['sample']=='sampleB']['fastq_1'].\
        values.tolist()
    self.assertEqual(len(sampleB_fastq), 1)
    self.assertTrue('/path/sampleB_S1_L001_R1_001.fastq.gz' in sampleB_fastq)


  def test_format_nextflow_conf(self):
    formatted_config_file = \
      format_nextflow_conf(
        config_template_file=self.config_template_file,
        singularity_bind_dir_list=['/path/input', '/path/output'],
        output_dir=self.temp_dir,
        input_paths=[['sampleA', ['sampleA_R1.fastq.gz', 'sampleA_R2.fastq.gz']]])
    self.assertTrue(os.path.exists(formatted_config_file))
    singularity_bind_line = ''
    input_paths_line = ''
    with open(formatted_config_file, 'r') as fp:
      for f in fp:
        if 'runOptions =' in f:
          singularity_bind_line = f.strip()
        if 'input_paths' in f and not 'schema_ignore_params' in f:
          input_paths_line = f.strip()
    self.assertTrue(singularity_bind_line != '')
    self.assertTrue('/path/input,' in singularity_bind_line)
    self.assertTrue('/path/output,' in singularity_bind_line)
    self.assertEqual("input_paths  = [['sampleA', ['sampleA_R1.fastq.gz', 'sampleA_R2.fastq.gz']]]", input_paths_line)
    formatted_config_file = \
      format_nextflow_conf(
        config_template_file=self.config_template_file,
        singularity_bind_dir_list=['/path/input', '/path/output'],
        output_dir=self.temp_dir
      )
    input_paths_line = ''
    with open(formatted_config_file, 'r') as fp:
      for f in fp:
        if 'input_paths' in f:
          input_paths_line = f.strip()
    self.assertEqual(input_paths_line, "")

  def test_prepare_nfcore_smrnaseq_input(self):
    sample_metadata = {
      'sampleA': '',
      'sampleB': ''
    }
    analysis_metadata = { 
      "NXF_VER": "x.y.z",
      "nextflow_params": [
        "-profile singularity",
        "-r 2.0.0",
        "--protocol custom",
        "--genome GRCm38",
        "--mirtrace_species mmu",
        "--three_prime_adapter AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC"]}
    hpc_data_dir = \
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir')
    os.makedirs(
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir',
        'projectA'))
    work_dir, runner_file = \
      prepare_nfcore_smrnaseq_input(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata)
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    check_extra_params = False
    extra_params = \
      "--three_prime_adapter AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if input_file_path in f.strip():
          check_input_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_input_file)
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)
    work_dir, runner_file = \
      prepare_input_for_multiple_nfcore_pipeline(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata,
        nfcore_pipeline_name='nf-core/smrnaseq')
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    check_extra_params = False
    extra_params = \
      "--three_prime_adapter AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if input_file_path in f.strip():
          check_input_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_input_file)
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)



  def test_make_nfcore_rnaseq_input(self):
    sample_metadata = {
      "sampleA": {
        "strandedness": "reverse"
      },
      "sampleB": {
        "strandedness": "reverse"
      }
    }
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=self.dbconfig)
    input_csv = \
      _make_nfcore_rnaseq_input(
      sample_metadata=sample_metadata,
      fastq_df=fastq_df,
      output_dir=self.temp_dir)
    self.assertTrue(os.path.exists(input_csv))
    csv_data = \
      pd.read_csv(input_csv, sep=",", header=0)
    self.assertTrue('sample' in csv_data.columns)
    self.assertTrue('fastq_1' in csv_data.columns)
    self.assertTrue('fastq_2' in csv_data.columns)
    self.assertTrue('strandedness' in csv_data.columns)
    sampleA = \
      csv_data[csv_data['sample']=='sampleA']
    fastq1_list = sampleA['fastq_1'].values.tolist()
    self.assertTrue('/path/sampleA_S1_L001_R1_001.fastq.gz' in fastq1_list)
    self.assertEqual(len(fastq1_list), 3)
    fastq2_list = sampleA['fastq_2'].values.tolist()
    self.assertTrue('/path/sampleA_S1_L001_R2_001.fastq.gz' in fastq2_list)
    self.assertEqual(len(fastq2_list), 3)
    sampleB = \
      csv_data[csv_data['sample']=='sampleB']
    fastq1_list = sampleB['fastq_1'].values.tolist()
    self.assertTrue('/path/sampleB_S1_L001_R1_001.fastq.gz' in fastq1_list)
    self.assertEqual(len(fastq1_list), 1)
    fastq2_list = sampleB['fastq_2'].values.tolist()
    self.assertTrue('/path/sampleB_S1_L001_R2_001.fastq.gz' in fastq2_list)
    self.assertEqual(len(fastq2_list), 1)


  def test_prepare_nfcore_rnaseq_input(self):
    sample_metadata = {
      "sampleA": {
        "strandedness": "reverse"
      },
      "sampleB": {
        "strandedness": "reverse"
      }
    }
    analysis_metadata = { 
      "NXF_VER": "x.y.z",
      "nextflow_params": [
        "-profile singularity",
        "-r 3.9",
        "--genome GRCm38",
        "--aligner star_rsem",
        "--seq_center IGF"
      ]}
    hpc_data_dir = \
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir')
    os.makedirs(
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir',
        'projectA'))
    work_dir, runner_file = \
      prepare_nfcore_rnaseq_input(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata)
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    check_extra_params = False
    extra_params = \
      "--aligner star_rsem"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if input_file_path in f.strip():
          check_input_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_input_file)
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)
    work_dir, runner_file = \
      prepare_input_for_multiple_nfcore_pipeline(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata,
        nfcore_pipeline_name='nf-core/rnaseq')
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    check_extra_params = False
    extra_params = \
      "--aligner star_rsem"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if input_file_path in f.strip():
          check_input_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_input_file)
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)


  def test_make_nfcore_methylseq_input(self):
    sample_metadata = {
      "sampleA": "",
      "sampleB": ""
    }
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=self.dbconfig)
    input_paths_list = \
      _make_nfcore_methylseq_input(
        sample_metadata=sample_metadata,
        fastq_df=fastq_df)
    self.assertEqual(len(input_paths_list), 4)
    sampleA_count = 0
    for i in input_paths_list:
      if i[0] == 'sampleA':
        sampleA_count += 1
    self.assertEqual(sampleA_count, 3)
    sampleB_count = 0
    sampleB_fastqs = []
    for i in input_paths_list:
      if i[0] == 'sampleB':
        sampleB_count += 1
        sampleB_fastqs = i[1]
    self.assertEqual(sampleB_count, 1)
    self.assertEqual(len(sampleB_fastqs), 2)
    self.assertEqual('/path/sampleB_S1_L001_R2_001.fastq.gz', sampleB_fastqs[1])

  def test_prepare_nfcore_methylseq_input(self):
    sample_metadata = {
      "sampleA": "",
      "sampleB": ""
    }
    analysis_metadata = { 
      "NXF_VER": "x.y.z",
      "nextflow_params": [
        "-profile singularity",
        "-r 1.6.0",
        "--aligner bismark",
        "--genome GRCm38"]}
    hpc_data_dir = \
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir')
    os.makedirs(
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir',
        'projectA'))
    work_dir, runner_file = \
      prepare_nfcore_methylseq_input(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata)
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_extra_params = False
    extra_params = \
      "--aligner bismark"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)
    work_dir, runner_file = \
      prepare_input_for_multiple_nfcore_pipeline(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata,
        nfcore_pipeline_name='nf-core/methylseq')
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_extra_params = False
    extra_params = \
      "--aligner bismark"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
        if input_file_path in f.strip():
          check_input_file = True
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)
    self.assertFalse(check_input_file)
    analysis_metadata = { 
      "NXF_VER": "x.y.z",
      "nextflow_params": [
        "-profile singularity",
        "-r 2.0.0",
        "--aligner bismark",
        "--genome GRCm38"]}
    work_dir, runner_file = \
      prepare_nfcore_methylseq_input(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata)
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    with open(runner_file, 'r') as fp:
      for f in fp:
        if input_file_path in f.strip():
          check_input_file = True
    self.assertTrue(check_input_file)
    df = pd.read_csv(os.path.join(work_dir, "input.csv"))
    self.assertEqual(len(df.columns), 3)
    self.assertTrue('sample' in df.columns)
    self.assertTrue('fastq_1' in df.columns)
    self.assertTrue('fastq_2' in df.columns)

  def test_make_nfcore_sarek_input(self):
    sample_metadata = {
      "sampleA": {
        "patient": 1,
        "sex": "XX",
        "status": 0
      },
      "sampleB": {
        "patient": 2,
        "sex": "XY",
        "status": 1
      }
    }
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=self.dbconfig)
    input_csv_file_path = \
      _make_nfcore_sarek_input(
        sample_metadata=sample_metadata,
        fastq_df=fastq_df,
        output_dir=self.temp_dir)
    self.assertTrue(os.path.exists(input_csv_file_path))
    csv_data = pd.read_csv(input_csv_file_path, sep=",", header=0)
    sampleA = \
      csv_data[csv_data['sample']=='sampleA']
    self.assertEqual(len(sampleA.index), 3)
    sampleA_fastq1s = \
      sampleA['fastq_1'].values.tolist()
    self.assertTrue( '/path/sampleA_S1_L001_R1_001.fastq.gz' in sampleA_fastq1s)
    sampleA_fastq2s = \
      sampleA['fastq_2'].values.tolist()
    self.assertTrue( '/path/sampleA_S1_L001_R2_001.fastq.gz' in sampleA_fastq2s)
    sampleA_sex = \
      sampleA['sex'].drop_duplicates().values.tolist()
    self.assertEqual(sampleA_sex[0], 'XX')
    sampleB = \
      csv_data[csv_data['sample']=='sampleB']
    self.assertEqual(len(sampleB.index), 1)
    sampleB_fastq1s = \
      sampleB['fastq_1'].values.tolist()
    self.assertTrue( '/path/sampleB_S1_L001_R1_001.fastq.gz' in sampleB_fastq1s)
    sampleB_fastq2s = \
      sampleB['fastq_2'].values.tolist()
    self.assertTrue( '/path/sampleB_S1_L001_R2_001.fastq.gz' in sampleB_fastq2s)

  def test_prepare_nfcore_sarek_input(self):
    sample_metadata = {
      "sampleA": {
        "patient": 1,
        "sex": "XX",
        "status": 0
      },
      "sampleB": {
        "patient": 2,
        "sex": "XY",
        "status": 1
      }
    }
    analysis_metadata = { 
      "NXF_VER": "x.y.z",
      "nextflow_params":  [
        "-profile singularity",
        "-r 3.0.2",
        "--step mapping"
        "--genome GRCh38"
      ]}
    hpc_data_dir = \
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir')
    os.makedirs(
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir',
        'projectA'))
    work_dir, runner_file = \
      prepare_nfcore_sarek_input(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata)
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    check_extra_params = False
    extra_params = \
      "--step mapping"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if input_file_path in f.strip():
          check_input_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_input_file)
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)
    work_dir, runner_file = \
      prepare_input_for_multiple_nfcore_pipeline(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata,
        nfcore_pipeline_name='nf-core/sarek')
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    check_extra_params = False
    extra_params = \
      "--step mapping"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if input_file_path in f.strip():
          check_input_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_input_file)
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)


  def test_make_nfcore_ampliseq_input(self):
    sample_metadata = {
      "sampleA": {"condition": "control"},
      "sampleB": {"condition": "treatment"}
    }
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=self.dbconfig)
    input_file, metadata_file = \
      _make_nfcore_ampliseq_input(
        sample_metadata=sample_metadata,
        fastq_df=fastq_df,
        output_dir=self.temp_dir)
    self.assertTrue(os.path.exists(input_file))
    self.assertTrue(os.path.exists(metadata_file))
    csv_data = \
      pd.read_csv(input_file, sep=",", header=0)
    self.assertTrue('sampleID' in csv_data.columns)
    self.assertTrue('forwardReads' in csv_data.columns)
    self.assertTrue('reverseReads' in csv_data.columns)
    self.assertTrue('run' in csv_data.columns)
    sampleA = \
      csv_data[csv_data['sampleID']=='sampleA']
    self.assertTrue('/path/sampleA_S1_L001_R1_001.fastq.gz' in sampleA['forwardReads'].values.tolist())
    self.assertEqual(len(sampleA['forwardReads'].values.tolist()), 3)
    self.assertTrue('/path/sampleA_S1_L001_R2_001.fastq.gz' in sampleA['reverseReads'].values.tolist())
    sampleB = \
      csv_data[csv_data['sampleID']=='sampleB']
    self.assertTrue('/path/sampleB_S1_L001_R1_001.fastq.gz' in sampleB['forwardReads'].values.tolist())
    self.assertEqual(len(sampleB['forwardReads'].values.tolist()), 1)
    csv_metadata = \
      pd.read_csv(metadata_file, sep=",", header=0)
    self.assertTrue('ID' in csv_metadata.columns)
    self.assertTrue('condition' in csv_metadata.columns)
    sampleA = \
      csv_metadata[csv_metadata['ID']=='sampleA']
    self.assertEqual(sampleA['condition'].values.tolist()[0], 'control')
    sampleB = \
      csv_metadata[csv_metadata['ID']=='sampleB']
    self.assertEqual(sampleB['condition'].values.tolist()[0], 'treatment')

  def test_prepare_nfcore_ampliseq_input(self):
    sample_metadata = {
      "sampleA": {"condition": "control"},
      "sampleB": {"condition": "treatment"}
    }
    analysis_metadata = { 
      "NXF_VER": "x.y.z",
      "nextflow_params":  [
        "-profile singularity",
        "-r 2.4.0",
        "--illumina_novaseq",
        "--FW_primer GTGYCAGCMGCCGCGGTAA",
        "--RV_primer GGACTACNVGGGTWTCTAAT"]}
    hpc_data_dir = \
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir')
    os.makedirs(
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir',
        'projectA'))
    work_dir, runner_file = \
      prepare_nfcore_ampliseq_input(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata)
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    check_metadata_file = False
    metadata_file_path = \
      f'--metadata {os.path.join(work_dir, "metadata.csv")}'
    check_extra_params = False
    extra_params = \
      "--FW_primer GTGYCAGCMGCCGCGGTAA"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if input_file_path in f.strip():
          check_input_file = True
        if metadata_file_path in f.strip():
          check_metadata_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_input_file)
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_metadata_file)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)
    work_dir, runner_file = \
      prepare_input_for_multiple_nfcore_pipeline(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata,
        nfcore_pipeline_name='nf-core/ampliseq')
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    check_metadata_file = False
    metadata_file_path = \
      f'--metadata {os.path.join(work_dir, "metadata.csv")}'
    check_extra_params = False
    extra_params = \
      "--FW_primer GTGYCAGCMGCCGCGGTAA"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if input_file_path in f.strip():
          check_input_file = True
        if metadata_file_path in f.strip():
          check_metadata_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_input_file)
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_metadata_file)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)


  def test_make_nfcore_atacseq_input(self):
    sample_metadata = {
      "sampleA": {
        "sample": "control1",
        "replicate": 1},
      "sampleB": {
        "sample": "control2",
        "replicate": 2}
    }
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=self.dbconfig)
    input_file = \
      _make_nfcore_atacseq_input(
        sample_metadata=sample_metadata,
        fastq_df=fastq_df,
        output_dir=self.temp_dir)
    self.assertTrue(os.path.exists(input_file))
    csv_data = \
      pd.read_csv(input_file, sep=",", header=0)
    self.assertTrue('fastq_1' in csv_data)
    self.assertTrue('fastq_2' in csv_data)
    self.assertTrue('sample' in csv_data)
    self.assertTrue('replicate' in csv_data)
    control1 = \
      csv_data[csv_data['sample']=='control1']
    self.assertEqual(len(control1.index), 3)
    self.assertTrue('/path/sampleA_S1_L001_R1_001.fastq.gz' in control1['fastq_1'].values.tolist())
    self.assertTrue('/path/sampleA_S1_L001_R2_001.fastq.gz' in control1['fastq_2'].values.tolist())

  def test_prepare_nfcore_atacseq_input(self):
    sample_metadata = {
      "sampleA": {
        "sample": "control1",
        "replicate": 1},
      "sampleB": {
        "sample": "control2",
        "replicate": 2}
    }
    analysis_metadata = { 
      "NXF_VER": "x.y.z",
      "nextflow_params":  [
        "-profile singularity",
        "-r 1.2.2",
        "--genome GRCh38"
      ]}
    hpc_data_dir = \
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir')
    os.makedirs(
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir',
        'projectA'))
    work_dir, runner_file = \
      prepare_nfcore_atacseq_input(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata)
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    check_extra_params = False
    extra_params = \
      "--genome GRCh38"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if input_file_path in f.strip():
          check_input_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_input_file)
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)
    work_dir, runner_file = \
      prepare_input_for_multiple_nfcore_pipeline(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata,
        nfcore_pipeline_name='nf-core/atacseq')
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    check_extra_params = False
    extra_params = \
      "--genome GRCh38"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if input_file_path in f.strip():
          check_input_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_input_file)
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)


  def test_make_nfcore_chipseq_input(self):
    sample_metadata = {
      "sampleA": {
        "antibody": "BCATENIN",
        "control": "sampleB"},
      "sampleB": {
        "antibody": "",
        "control": ""}
    }
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=self.dbconfig)
    input_file = \
      _make_nfcore_chipseq_input(
        sample_metadata=sample_metadata,
        fastq_df=fastq_df,
        output_dir=self.temp_dir)
    self.assertTrue(os.path.exists(input_file))
    csv_data = \
      pd.read_csv(input_file, sep=",", header=0)
    self.assertTrue('fastq_1' in csv_data)
    self.assertTrue('fastq_2' in csv_data)
    self.assertTrue('control' in csv_data)
    self.assertTrue('antibody' in csv_data)
    self.assertTrue('sample' in csv_data)
    control1 = \
      csv_data[csv_data['sample']=='sampleA']
    self.assertEqual(len(control1.index), 3)
    self.assertTrue('/path/sampleA_S1_L001_R1_001.fastq.gz' in control1['fastq_1'].values.tolist())
    self.assertTrue('/path/sampleA_S1_L001_R2_001.fastq.gz' in control1['fastq_2'].values.tolist())

  def test_prepare_nfcore_chipseq_input(self):
    sample_metadata = {
      "sampleA": {
        "antibody": "BCATENIN",
        "control": "sampleB"},
      "sampleB": {
        "antibody": "",
        "control": ""}
    }
    analysis_metadata = { 
      "NXF_VER": "x.y.z",
      "nextflow_params":  [
        "-profile singularity",
        "-r 2.0.0",
        "--genome GRCh38"
      ]}
    hpc_data_dir = \
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir')
    os.makedirs(
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir',
        'projectA'))
    work_dir, runner_file = \
      prepare_nfcore_chipseq_input(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata)
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    check_extra_params = False
    extra_params = \
      "--genome GRCh38"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if input_file_path in f.strip():
          check_input_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_input_file)
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)
    work_dir, runner_file = \
      prepare_input_for_multiple_nfcore_pipeline(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata,
        nfcore_pipeline_name='nf-core/chipseq')
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    check_extra_params = False
    extra_params = \
      "--genome GRCh38"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if input_file_path in f.strip():
          check_input_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_input_file)
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)


  def test_make_nfcore_cutandrun_input(self):
    sample_metadata = {
      "sampleA": {
        "group": "h3k27me3",
        "replicate": 1,
        "control_group": "igg_ctrl"},
      "sampleB": {
        "group": "igg_ctrl",
        "replicate": 1,
        "control_group": ""}
    }
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=self.dbconfig)
    input_file = \
      _make_nfcore_cutandrun_input(
        sample_metadata=sample_metadata,
        fastq_df=fastq_df,
        output_dir=self.temp_dir)
    self.assertTrue(os.path.exists(input_file))
    csv_data = \
      pd.read_csv(input_file, sep=",", header=0)
    self.assertTrue('fastq_1' in csv_data)
    self.assertTrue('fastq_2' in csv_data)
    self.assertTrue('group' in csv_data)
    self.assertTrue('replicate' in csv_data)
    self.assertTrue('control_group' in csv_data)
    control1 = \
      csv_data[csv_data['group']=='h3k27me3']
    self.assertEqual(len(control1.index), 3)
    self.assertTrue('/path/sampleA_S1_L001_R1_001.fastq.gz' in control1['fastq_1'].values.tolist())
    self.assertTrue('/path/sampleA_S1_L001_R2_001.fastq.gz' in control1['fastq_2'].values.tolist())


  def test_prepare_nfcore_cutandrun_input(self):
    sample_metadata = {
      "sampleA": {
        "group": "h3k27me3",
        "replicate": 1,
        "control_group": "igg_ctrl"},
      "sampleB": {
        "group": "igg_ctrl",
        "replicate": 1,
        "control_group": ""}
    }
    analysis_metadata = { 
      "NXF_VER": "x.y.z",
      "nextflow_params":  [
        "-profile singularity",
        "-r 3.0",
        "--genome GRCh38"
      ]}
    hpc_data_dir = \
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir')
    os.makedirs(
      os.path.join(
        self.temp_dir, 
        'hpc_data_dir',
        'projectA'))
    work_dir, runner_file = \
      prepare_nfcore_cutandrun_input(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata)
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    check_extra_params = False
    extra_params = \
      "--genome GRCh38"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if input_file_path in f.strip():
          check_input_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_input_file)
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)
    work_dir, runner_file = \
      prepare_input_for_multiple_nfcore_pipeline(
        runner_template_file=self.runner_template_file,
        config_template_file=self.config_template_file,
        project_name='projectA',
        hpc_data_dir=hpc_data_dir,
        dbconf_file=self.dbconfig,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata,
        nfcore_pipeline_name='nf-core/cutandrun')
    self.assertTrue(os.path.exists(runner_file))
    self.assertTrue(os.path.exists(work_dir))
    check_config_file = False
    config_file_path = \
      f'-c {work_dir}/{os.path.basename(self.config_template_file)}'
    check_input_file = False
    input_file_path = \
      f'--input {os.path.join(work_dir, "input.csv")}'
    check_extra_params = False
    extra_params = \
      "--genome GRCh38"
    check_work_dir = False
    work_dir_path = \
      f"-work-dir {work_dir}"
    check_output_dir = False
    output_dir = \
      f"--outdir {os.path.join(work_dir, 'results')}"
    check_report = False
    report_path = \
      f"-with-report {os.path.join(work_dir, 'results', 'report.html')}"
    with open(runner_file, 'r') as fp:
      for f in fp:
        if config_file_path in f.strip():
          check_config_file = True
        if input_file_path in f.strip():
          check_input_file = True
        if extra_params in f.strip():
          check_extra_params = True
        if work_dir_path in f.strip():
          check_work_dir = True
        if output_dir in f.strip():
          check_output_dir = True
        if report_path in f.strip():
          check_report = True
    self.assertTrue(check_input_file)
    self.assertTrue(check_config_file)
    self.assertTrue(check_extra_params)
    self.assertTrue(check_work_dir)
    self.assertTrue(check_output_dir)
    self.assertTrue(check_report)

if __name__=='__main__':
  unittest.main()