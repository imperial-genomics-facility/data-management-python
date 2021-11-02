import pandas as pd
from airflow.models import Variable
import logging, os, requests, subprocess, re, shutil, gzip
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.fileutils import check_file_path, get_temp_dir, copy_local_file
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor


GENOME_FASTA_TYPE = 'GENOME_FASTA'
GENOME_DICT_TYPE = 'GENOME_DICT'
GENE_REFFLAT_TYPE = 'GENE_REFFLAT'
RIBOSOMAL_INTERVAL_TYPE = 'RIBOSOMAL_INTERVAL'
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
SLACK_CONF = Variable.get('slack_conf', default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf', default_var=None)
GTF_REF_PATH = Variable.get('gtf_ref_path', default_var=None)
STAR_EXE = Variable.get('star_exe', default_var=None)
STAR_REF_PATH = Variable.get('star_ref_path', default_var=None)
RSEM_EXE_DIR = Variable.get('rsem_exe_dir', default_var=None)
RSEM_REF_PATH = Variable.get('rsem_ref_path', default_var=None)
UCSC_EXE_DIR = Variable.get('ucsc_exe_dir', default_var=None)
REFFLAT_REF_PATH = Variable.get('refflat_ref_path', default_var=None)
RIBOSOMAL_INTERVAL_REF_PATH = Variable.get('ribosomal_interval_ref_path', default_var=None)
CELLRANGER_EXE = Variable.get('cellranger_exe', default_var=None)
CELLRANGER_REF_PATH = Variable.get('cellranger_ref_path', default_var=None)

def collect_files_to_db(collection_list, dbconfig_file):
  try:
    dbparams = \
      read_dbconf_json(dbconfig_file)
    ca = CollectionAdaptor(**dbparams)
    ca.start_session()
    fa = FileAdaptor(**{'session': ca.session})
    try:
      for collection_name, collection_type, file_path in collection_list:
        collection_grp_df = \
          ca.get_collection_files(
            collection_name=collection_name,
            collection_type=collection_type)
        if len(collection_grp_df.index) > 0:
          file_exists = \
            fa.check_file_records_file_path(file_path)
          if file_exists:
            raise ValueError(
                    'File {0} already present in db'.\
                      format(file_path))
          remove_data = [{
            'name': collection_name,
            'type': collection_type}]
          ca.remove_collection_group_info(
            data=remove_data,
            autosave=False)
        collection_data = \
          pd.DataFrame([{
            'name': collection_name,
            'type': collection_type,
            'table': 'file',
            'location': 'HPC_PROJECT',
            'file_path': file_path}])
        ca.load_file_and_create_collection(
          data=collection_data,
          calculate_file_size_and_md5=False,
          autosave=False)
      ca.commit_session()
      ca.close_session()
    except:
      ca.rollback_session()
      ca.close_session()
      raise
  except Exception as e:
    raise ValueError(
            'Failed to collect files, error: {0}'.format(e))


def add_refs_to_db_collection_func(**context):
  try:
    task_list = \
      context['params'].get('task_list')
    if task_list is None:
      raise ValueError('No task list found')
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    if dag_run is not None and \
       dag_run.conf is not None:
      tag = dag_run.conf.get('tag')
      species_name = dag_run.conf.get('species_name')
      if tag is None or \
         species_name is None:
        raise ValueError('Tag or species_name not found')
      collection_list = list()
      for xcom_task, xcom_key, collection_type in task_list:
        file_path = \
          ti.xcom_pull(
            task_ids=xcom_task,
            key=xcom_key)
        if file_path is None:
          raise ValueError(
                  'File path for xcom task {0} and key {1} not found'.\
                    format(xcom_task, xcom_key))
        collection_list.append([
          species_name, collection_type, file_path])
      collect_files_to_db(
        collection_list=collection_list,
        dbconfig_file=DATABASE_CONFIG_FILE)
      message = \
        'Finish ref genome building for {0} - {1}'.\
          format(species_name, tag)
      send_log_to_channels(
        slack_conf=SLACK_CONF,
        ms_teams_conf=MS_TEAMS_CONF,
        task_id=context['task'].task_id,
        dag_id=context['task'].dag_id,
        comment=message,
        reaction='pass')
    else:
      raise ValueError('No dag_run.conf entry found')
  except Exception as e:
    logging.error(e)
    message = \
      'DB collection building error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def filter_gtf_row_for_cellranger_ref(x):
  try:
    gene_id = None
    biotype_pattern = \
      "(protein_coding|lncRNA|IG_C_gene|IG_D_gene|IG_J_gene|IG_LV_gene|IG_V_gene|IG_V_pseudogene|IG_J_pseudogene|IG_C_pseudogene|TR_C_gene|TR_D_gene|TR_J_gene|TR_V_gene|TR_V_pseudogene|TR_J_pseudogene)"
    gene_pattern =  \
      re.compile(r'.*gene_type \"{0}\"'.format(biotype_pattern.replace('\n', '')))
    tx_pattern = \
      re.compile(r'.*transcript_type \"{0}\"'.format(biotype_pattern))
    readthrough_pattern = \
      re.compile(r".*tag \"readthrough_transcript\"")
    par_pattern = \
      re.compile(r".*tag \"PAR\"")
    gene_id_pattern = \
      re.compile(r'.*gene_id \"(\S+)\"')
    if (re.match(gene_pattern, x) and \
        re.match(tx_pattern, x)):
      if re.match(gene_id_pattern, x):
        gene_id = \
          re.match(gene_id_pattern, x).group(1)
    if re.match(readthrough_pattern, x) or \
       re.match(par_pattern, x):
        gene_id = None
    return gene_id
  except Exception as e:
    raise ValueError(
            'Failed to filter gtf for cellranger, error: {0}'.\
              format(e))


def extract_gene_id_from_gtf(x):
  try:
    gene_id_pattern = re.compile(r'.*gene_id \"(\S+)\"')
    gene_id = None
    if re.match(gene_id_pattern, x):
      gene_id = re.match(gene_id_pattern, x).group(1)
    return gene_id
  except Exception as e:
    raise ValueError(
            'Failed to extract gene id, error: {0}'.\
              format(e))


def filter_gtf_file_for_cellranger(gtf_file, skip_gtf_rows=5):
  try:
    check_file_path(gtf_file)
    df = \
      pd.read_csv(
        gtf_file,
        sep='\t',
        skiprows=skip_gtf_rows,
        header=None,
        engine='c',
        low_memory=True)
    transcripts_df = \
      df[df[2]=='transcript']
    transcripts_df['gene_id'] = \
      transcripts_df[8].\
        map(lambda x: filter_gtf_row_for_cellranger_ref(x))
    gene_id_list = \
      transcripts_df['gene_id'].\
      dropna().\
      drop_duplicates().\
      values.tolist()
    df['gene_id'] = \
      df[8].\
      map(lambda x: extract_gene_id_from_gtf(x))
    df = \
      df[df['gene_id'].isin(gene_id_list)]
    df.\
      drop('gene_id', axis=1, inplace=True)
    temp_dir = get_temp_dir()
    filtered_gtf = \
      os.path.join(
        temp_dir,
        '{0}_filtered'.format(os.path.basename(gtf_file)))
    temp_filtered_gtf = \
      os.path.join(
        temp_dir,
        '{0}_filtered_temp'.format(os.path.basename(gtf_file)))
    header_file = \
      os.path.join(
        temp_dir,
        '{0}_header'.format(os.path.basename(gtf_file)))
    with open(header_file, 'w') as ofp:
      with open(gtf_file, 'r') as fp:
        for line in fp:
          if line.startswith('#'):
            ofp.write(line)
    df.\
      to_csv(
        temp_filtered_gtf,
        sep='\t',
        header=False,
        index=False,
        quotechar="'")
    with open(filtered_gtf, 'wb') as ofp:
      for i in [header_file, temp_filtered_gtf]:
        with open(i, 'rb') as fp:
          shutil.copyfileobj(fp, ofp)
    check_file_path(filtered_gtf)
    return filtered_gtf
  except Exception as e:
    raise ValueError(
            'Failed to filter gtf file {0} for cellranger, error: {1}'.\
              format(gtf_file, e))


def create_cellranger_ref_func(**context):
  try:
    gtf_xcom_key = \
      context['params'].get('gtf_xcom_key')
    gtf_xcom_task = \
      context['params'].get('gtf_xcom_task')
    cellranger_ref_xcom_key = \
      context['params'].get('cellranger_ref_xcom_key')
    skip_gtf_rows = \
      context['params'].get('skip_gtf_rows')
    threads = \
      context['params'].get('threads')
    memory = \
      context['params'].get('memory')
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    gtf_file = \
      ti.xcom_pull(
        task_ids=gtf_xcom_task,
        key=gtf_xcom_key)
    check_file_path(gtf_file)
    filtered_gtf = \
      filter_gtf_file_for_cellranger(
        gtf_file=gtf_file,
        skip_gtf_rows=skip_gtf_rows)
    temp_dir = get_temp_dir()
    if dag_run is not None and \
       dag_run.conf is not None:
      tag = dag_run.conf.get('tag')
      species_name = dag_run.conf.get('species_name')
      cellranger_version = dag_run.conf.get('cellranger_version')
      if species_name is None or \
         tag is None or \
         cellranger_version is None:
        raise ValueError(
                'Missing species name, cellranger version or tag')
      fasta_file = \
        fetch_ref_genome_from_db(
          dbconfig_file=DATABASE_CONFIG_FILE,
          genome_build=species_name,
          reference_type=GENOME_FASTA_TYPE)
      cellranger_ref_name = \
        '{0}_{1}_{2}'.\
          format(
            species_name,
            tag,
            cellranger_version)
      target_cellranger_ref = \
        os.path.join(
          CELLRANGER_REF_PATH,
          species_name,
          cellranger_ref_name)
      os.makedirs(
        os.path.join(
          CELLRANGER_REF_PATH,
          species_name),
        exist_ok=True)
      if os.path.exists(target_cellranger_ref):
        raise ValueError(
                'Cellranger ref {0} already present'.\
                  format(target_cellranger_ref))
      cellranger_command = [
        'cd', temp_dir,';',
        CELLRANGER_EXE,
        'mkref',
        '--ref-version={0}_{1}_{2}'.\
          format(species_name, tag, cellranger_version),
        '--genome={0}'.format(cellranger_ref_name),
        '--fasta={0}'.format(fasta_file),
        '--genes={0}'.format(filtered_gtf),
        '--memgb={0}'.format(memory),
        '--nthreads={0}'.format(threads)]
      cellranger_command = \
        ' '.join(cellranger_command)
      subprocess.\
        check_call(
          cellranger_command,
          shell=True)
      copy_local_file(
        os.path.join(temp_dir, cellranger_ref_name),
        target_cellranger_ref)
      ti.xcom_push(
        key=cellranger_ref_xcom_key,
        value=target_cellranger_ref)
    else:
      raise ValueError('No dag_run.conf entry found')
  except Exception as e:
    logging.error(e)
    message = \
      'Cellranger ref building error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def filter_rRNA_genes_in_gtf(x):
  try:
    pattern = re.compile(r'.*gene_type \"(rRNA|rRNA_pseudogene)\\"')
    if re.match(pattern, x):
        return 'rRNA'
    else:
        return None
  except Exception as e:
    raise ValueError(
            'Failed to get rRNA gene_type, error: {0}'.\
              format(e))



def extract_gene_name_from_gtf_info(x):
  try:
    pattern = re.compile(r'.*gene_name \"(\S+)\"')
    gene_name = None
    if re.match(pattern, x):
        gene_name = re.match(pattern, x).group(1)
    return gene_name
  except Exception as e:
    raise ValueError(
            'Failed to extract gene name, error; {0}'.\
              format(e))


def prepare_ribosomal_interval_from_gtf_and_dict(
      gtf_file, dict_file, ribosomal_interval, skip_gtf_rows=5):
  try:
    check_file_path(gtf_file)
    check_file_path(dict_file)
    temp_dir = \
        get_temp_dir()
    temp_file = \
      os.path.join(
        temp_dir,
        '{0}_temp'.format(
          os.path.basename(gtf_file)))
    df = \
        pd.read_csv(
          gtf_file,
          sep='\t',
          header=None,
          skiprows=skip_gtf_rows)
    df['rRNA'] = \
      df[8].\
        map(lambda x: filter_rRNA_genes_in_gtf(x))                              # mark rRNA genes
    df = \
      df[df['rRNA']=='rRNA']                                                    # filter rRNA genes
    df[8] = \
      df[8].\
        map(lambda x: extract_gene_name_from_gtf_info(x))
    df.iloc[:,[0, 3, 4, 6, 8]].\
      to_csv(
        temp_file,
        index=False,
        sep='\t',
        header=False)
    with open(ribosomal_interval ,'wb') as ofp:
      for i in (dict_file, temp_file):
        with open(i, 'rb') as fp:
          shutil.copyfileobj(fp, ofp)
  except Exception as e:
    raise ValueError(
            'Failed to prepare ribosomal interval file, error: {0}'.\
              format(e))


def create_ribosomal_interval_func(**context):
  try:
    gtf_xcom_key = \
      context['params'].get('gtf_xcom_key')
    gtf_xcom_task = \
      context['params'].get('gtf_xcom_task')
    ribosomal_ref_xcom_key = \
      context['params'].get('ribosomal_ref_xcom_key')
    skip_gtf_rows = \
      context['params'].get('skip_gtf_rows')
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    gtf_file = \
      ti.xcom_pull(
        task_ids=gtf_xcom_task,
        key=gtf_xcom_key)
    check_file_path(gtf_file)
    if dag_run is not None and \
       dag_run.conf is not None:
      tag = dag_run.conf.get('tag')
      species_name = dag_run.conf.get('species_name')
      if species_name is None or \
         tag is None:
        raise ValueError('Missing species name or tag')
      dict_file = \
        fetch_ref_genome_from_db(
          dbconfig_file=DATABASE_CONFIG_FILE,
          genome_build=species_name,
          reference_type=GENOME_DICT_TYPE)
      temp_dir = \
        get_temp_dir(use_ephemeral_space=True)
      ribosomal_ref_name = \
        '{0}_{1}_ribosomal.interval'.\
            format(species_name, tag)
      temp_ribosomal_interval_file_path = \
        os.path.join(
          temp_dir,
          ribosomal_ref_name)
      target_ribisomal_file = \
        os.path.join(
          RIBOSOMAL_INTERVAL_REF_PATH,
          species_name,
          ribosomal_ref_name)
      if os.path.exists(target_ribisomal_file):
        raise ValueError(
                'Ribosomal file {0} already present, remove it before continuing'.\
                  format(target_ribisomal_file))
      prepare_ribosomal_interval_from_gtf_and_dict(
        gtf_file=gtf_file,
        dict_file=dict_file,
        ribosomal_interval=temp_ribosomal_interval_file_path,
        skip_gtf_rows=skip_gtf_rows)
      copy_local_file(
        temp_ribosomal_interval_file_path,
        target_ribisomal_file)
      ti.xcom_push(
        key=ribosomal_ref_xcom_key,
        value=target_ribisomal_file)
    else:
      raise ValueError('No dag_run.conf entry found')
  except Exception as e:
    logging.error(e)
    message = \
      'Ribosomal ref building error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise

def create_genepred_to_refflat(genpred_file, refflat_file):
  try:
    check_file_path(genpred_file)
    df = \
      pd.read_csv(
        genpred_file,
        sep='\t',
         header=None)
    df.iloc[:,[11, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]].\
      to_csv(
        refflat_file,
        index=False,
        sep='\t',
        header=False)
  except Exception as e:
    raise ValueError(
            'Failed to convert genepred to refflat, error: {0}'.format(e))


def create_reflat_index_func(**context):
  try:
    gtf_xcom_key = \
      context['params'].get('gtf_xcom_key')
    gtf_xcom_task = \
      context['params'].get('gtf_xcom_task')
    refflat_ref_xcom_key = \
      context['params'].get('refflat_ref_xcom_key')
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    gtf_file = \
      ti.xcom_pull(
        task_ids=gtf_xcom_task,
        key=gtf_xcom_key)
    check_file_path(gtf_file)
    if dag_run is not None and \
       dag_run.conf is not None:
      tag = dag_run.conf.get('tag')
      species_name = dag_run.conf.get('species_name')
      if species_name is None or \
         tag is None:
        raise ValueError('Missing species name or tag')
      temp_dir = \
        get_temp_dir(use_ephemeral_space=True)
      refflat_ref_name = \
        '{0}_{1}.refflat'.\
            format(species_name, tag)
      temp_refflat_file = \
        os.path.join(
          temp_dir,
          refflat_ref_name)
      target_refflat_file = \
        os.path.join(
          REFFLAT_REF_PATH,
          species_name,
          refflat_ref_name)
      if os.path.exists(target_refflat_file):
        raise ValueError(
                'REFFLAT file {0} already present, remove it before continuing'.\
                  format(target_refflat_file))
      intermediate_temp_file = \
        '{0}_temp'.format(temp_refflat_file)
      gtfToGenePred_exe = \
        os.path.join(
          UCSC_EXE_DIR,
          'gtfToGenePred')
      check_file_path(gtfToGenePred_exe)
      gtf_to_genepred_cmd = [
        gtfToGenePred_exe,
        '-genePredExt',
        '-geneNameAsName2',
        gtf_file,
        intermediate_temp_file]
      subprocess.\
        check_call(
          ' '.join(gtf_to_genepred_cmd),
          shell=True)
      create_genepred_to_refflat(
        genpred_file=intermediate_temp_file,
        refflat_file=temp_refflat_file)
      copy_local_file(
        temp_refflat_file,
        target_refflat_file)
      ti.xcom_push(
        key=refflat_ref_xcom_key,
        value=target_refflat_file)
    else:
      raise ValueError('No dag_run.conf entry found')
  except Exception as e:
    logging.error(e)
    message = \
      'REFFLAT ref building error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def create_rsem_index_func(**context):
  try:
    gtf_xcom_key = \
      context['params'].get('gtf_xcom_key')
    gtf_xcom_task = \
      context['params'].get('gtf_xcom_task')
    threads = \
      context['params'].get('threads')
    rsem_ref_xcom_key = \
      context['params'].get('rsem_ref_xcom_key')
    rsem_options = \
      context['params'].get('rsem_options')
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    gtf_file = \
      ti.xcom_pull(
        task_ids=gtf_xcom_task,
        key=gtf_xcom_key)
    if dag_run is not None and \
       dag_run.conf is not None:
      tag = dag_run.conf.get('tag')
      species_name = dag_run.conf.get('species_name')
      if species_name is None or \
         tag is None:
        raise ValueError('Missing species name or tag')
      fasta_file = \
        fetch_ref_genome_from_db(
          dbconfig_file=DATABASE_CONFIG_FILE,
          genome_build=species_name,
          reference_type=GENOME_FASTA_TYPE)
      temp_dir = \
        get_temp_dir(use_ephemeral_space=True)
      rsem_ref_name = \
        '{0}_{1}_rsem'.\
            format(species_name, tag)
      temp_ref_dir = \
        os.path.join(
          temp_dir,
          rsem_ref_name)
      os.makedirs(
        temp_ref_dir,
        exist_ok=True)
      target_ref_dir = \
        os.path.join(
          RSEM_REF_PATH,
          species_name,
          rsem_ref_name)
      os.makedirs(
        os.path.join(
          RSEM_REF_PATH,
          species_name),
          exist_ok=True)
      if os.path.exists(target_ref_dir):
        raise ValueError(
                'RSEM REF {0} already present, remove it before continuing'.\
                  format(target_ref_dir))
      rsem_exe = \
        os.path.join(
          RSEM_EXE_DIR,
          'rsem-prepare-reference')
      check_file_path(rsem_exe)
      rsem_cmd = [
        rsem_exe,
        '--num-threads', str(threads),
        '--gtf', gtf_file]
      if rsem_options is not None and \
         isinstance(rsem_options, list) and \
         len(rsem_options) > 0:
        rsem_cmd.\
          extend(rsem_options)
      rsem_cmd.\
        append(fasta_file)
      rsem_cmd.\
        append(
          os.path.join(temp_ref_dir, rsem_ref_name))
      subprocess.\
        check_call(
          ' '.join(rsem_cmd),
          shell=True)
      copy_local_file(
        temp_ref_dir,
        target_ref_dir)
      ti.xcom_push(
        key=rsem_ref_xcom_key,
        value=os.path.join(target_ref_dir, rsem_ref_name))
    else:
      raise ValueError('No dag_run.conf entry found')
  except Exception as e:
    logging.error(e)
    message = \
      'RSEM ref building error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def unzip_file(gzip_file):
  '''
  Should move these functions to utils
  '''
  try:
    if not gzip_file.endswith('.gz'):
      raise IOError(
              "File {0} doesn't have .gz extension".\
                format(gzip_file))
    check_file_path(gzip_file)
    unzipped_file = \
      gzip_file.replace('.gz', '')
    unzipped_filename = \
      os.path.basename(unzipped_file)
    temp_dir = get_temp_dir()
    temp_unzipped_file = \
      os.path.join(
        temp_dir,
        unzipped_filename)
    with gzip.open(gzip_file, 'rb') as fp:
      with open(temp_unzipped_file, 'wb') as ofp:
        shutil.copyfileobj(fp, ofp)
    copy_local_file(
      temp_unzipped_file,
      unzipped_file)
    return unzipped_file
  except Exception as e:
    raise ValueError(
            'Failed to unzip file {0}, error: {1}'.\
              format(gzip_file, e))


def download_file(url, output_file):
  try:
    with requests.get(url, stream=True) as r:
      r.raise_for_status()
      with open(output_file, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
          f.write(chunk)
    if not os.path.exists(output_file):
      raise ValueError('Output file {0} is missing'.format(output_file))
  except Exception as e:
    raise ValueError('Failed to download file, error{0}'.format(e))


def download_gtf_file_func(**context):
  try:
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    gtf_xcom_key = \
      context['params'].get('gtf_xcom_key')
    if dag_run is not None and \
       dag_run.conf is not None:
      gtf_url = dag_run.conf.get('gtf_url')
      species_name = dag_run.conf.get('species_name')
      if gtf_url is None or \
         species_name is None:
        raise ValueError('No url or species info found for GTF')
      temp_dir = \
        get_temp_dir(use_ephemeral_space=True)
      filename = gtf_url.split('/')[-1]
      temp_file_path = \
        os.path.join(temp_dir, filename)
      download_file(
        url=gtf_url,
        output_file=temp_file_path)
      unzipped_file = \
        unzip_file(
          gzip_file=temp_file_path)
      target_gtf_path = \
        os.path.join(
          GTF_REF_PATH,
          species_name,
          os.path.basename(unzipped_file))
      # os.makedirs(
      #   os.path.join(
      #     GTF_REF_PATH,
      #     species_name),
      #   exist_ok=True)
      copy_local_file(
        unzipped_file,
        target_gtf_path)
      ti.xcom_push(
        key=gtf_xcom_key,
        value=target_gtf_path)
    else:
      raise ValueError('No dag_run.conf entry found')
  except Exception as e:
    logging.error(e)
    message = \
      'GTF file download error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def fetch_ref_genome_from_db(dbconfig_file, genome_build, reference_type):
  try:
    check_file_path(dbconfig_file)
    dbparams = \
      read_dbconf_json(dbconfig_file)
    base = BaseAdaptor(**dbparams)
    ref_tool = \
      Reference_genome_utils(
        dbsession_class=base.get_session_class(),
        genome_tag=genome_build)
    reference_file = \
      ref_tool.\
        _fetch_collection_files(
          collection_type=reference_type,
          check_missing=False)
    return reference_file
  except Exception as e:
    raise ValueError(
            'Failed to fetch file for {0} {1}, error: {2}'.\
              format(genome_build, reference_type, e))


def create_star_index_func(**context):
  try:
    gtf_xcom_key = \
      context['params'].get('gtf_xcom_key')
    gtf_xcom_task = \
      context['params'].get('gtf_xcom_task')
    threads = \
      context['params'].get('threads')
    star_options = \
      context['params'].get('star_options')
    star_ref_xcom_key = \
      context['params'].get('star_ref_xcom_key')
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    gtf_file = \
      ti.xcom_pull(
        task_ids=gtf_xcom_task,
        key=gtf_xcom_key)
    if dag_run is not None and \
       dag_run.conf is not None:
      tag = dag_run.conf.get('tag')
      species_name = dag_run.conf.get('species_name')
      if species_name is None or \
         tag is None:
        raise ValueError('Missing species name or tag')
      fasta_file = \
        fetch_ref_genome_from_db(
          dbconfig_file=DATABASE_CONFIG_FILE,
          genome_build=species_name,
          reference_type=GENOME_FASTA_TYPE)
      temp_dir = \
        get_temp_dir(use_ephemeral_space=True)
      star_ref_dirname = \
        '{0}_{1}_star'.\
            format(species_name, tag)
      star_target_dir = \
        os.path.join(
          STAR_REF_PATH,
          species_name,
          star_ref_dirname)
      os.makedirs(
        os.path.join(
          STAR_REF_PATH,
          species_name),
        exist_ok=True)
      if os.path.exists(star_target_dir):
        raise ValueError(
                'Target STAR ref path {0} already present, remove it before continuing'.\
                  format(star_target_dir))
      genome_dir = \
        os.path.join(
          temp_dir,
          star_ref_dirname)
      os.makedirs(genome_dir, exist_ok=True)
      star_cmd = [
        STAR_EXE,
        '--runThreadN', str(threads),
        '--runMode', 'genomeGenerate',
        '--genomeDir', genome_dir,
        '--genomeFastaFiles', fasta_file,
        '--sjdbGTFfile', gtf_file]
      if star_options is not None and \
         isinstance(star_options, list) and \
         len(star_options) > 0:
        star_options = [
          str(s) for s in star_options]
        star_cmd.\
          extend(star_options)
      subprocess.\
        check_call(
          ' '.join(star_cmd),
          shell=True)
      copy_local_file(
        genome_dir,
        star_target_dir)
      ti.xcom_push(
        key=star_ref_xcom_key,
        value=star_target_dir)
    else:
      raise ValueError('No dag_run.conf entry found')
  except Exception as e:
    logging.error(e)
    message = \
      'STAR ref building error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise