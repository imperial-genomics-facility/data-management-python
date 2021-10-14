import pandas as pd
import os, re, fnmatch, subprocess
from collections import defaultdict
from shlex import quote
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.igfdb.igfTables import Experiment, Run
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.utils.fileutils import calculate_file_checksum


class Collect_seqrun_fastq_to_db:
  '''
  A class for collecting raw fastq files after demultiplexing and storing them in database.
  Additionally this will also create relevant entries for the experiment and run tables in database

  :param fastq_dir: A directory path for file look up
  :param model_name: Sequencing platform information
  :param seqrun_igf_id: Sequencing run name
  :param session_class: A database session class
  :param flowcell_id: Flowcell information for the run
  :param samplesheet_file: Samplesheet filepath
  :param samplesheet_filename: Name of the samplesheet file, default SampleSheet.csv
  :param collection_type: Collection type information for new fastq files, default demultiplexed_fastq
  :param file_location: Fastq file location information, default HPC_PROJECT
  :param collection_table: Collection table information for fastq files, default run
  :param manifest_name: Name of the file manifest file, default file_manifest.csv
  :param singlecell_tag: Samplesheet description for singlecell samples, default 10X
  '''
  def __init__(
        self,fastq_dir,model_name,seqrun_igf_id,session_class,flowcell_id,
        samplesheet_file=None,samplesheet_filename='SampleSheet.csv',
        collection_type='demultiplexed_fastq',file_location='HPC_PROJECT',
        collection_table='run', manifest_name='file_manifest.csv',singlecell_tag='10X'):

    self.fastq_dir = fastq_dir
    self.samplesheet_file = samplesheet_file
    self.samplesheet_filename = samplesheet_filename
    self.seqrun_igf_id = seqrun_igf_id
    self.model_name = model_name
    self.session_class = session_class
    self.collection_type = collection_type
    self.file_location = file_location
    self.flowcell_id = flowcell_id
    self.collection_table = collection_table
    self.manifest_name = manifest_name
    self.singlecell_tag = singlecell_tag

  def find_fastq_and_build_db_collection(self):
    '''
    A method for finding fastq files and samplesheet under a run directory
    and loading the new files to db with their experiment and run information
    
    It calculates following entries
    
    * library_name
        Same as sample_id unless mentioned in 'Description' field of samplesheet
    * experiment_igf_id
        library_name combined with the platform name
        same library sequenced in different platform will be added as separate experiemnt
    * run_igf_id
        experiment_igf_id combined with sequencing flowcell_id and lane_id
        collection name: Same as run_igf_id, fastq files will be added to db collection
        using this id
    * collection type
        Default type for fastq file collections are 'demultiplexed_fastq'
    * file_location
        Default value is 'HPC_PROJECT'
    '''
    try:
      fastq_files_list = \
        self._collect_fastq_and_sample_info()
      self._build_and_store_exp_run_and_collection_in_db(
        fastq_files_list=fastq_files_list)
    except Exception as e:
      raise ValueError(
              'Failed to find fastq and build collection, error: {0}'.\
              format(e))


  def _get_fastq_and_samplesheet(self):
    try:
      fastq_dir = self.fastq_dir
      samplesheet_file = self.samplesheet_file
      samplesheet_filename = self.samplesheet_filename
      r1_fastq_regex = \
        re.compile(r'\S+_R1_\d+\.fastq(\.gz)?', re.IGNORECASE)
      r2_fastq_regex = \
        re.compile(r'\S+_R2_\d+\.fastq(\.gz)?', re.IGNORECASE)
      samplesheet_list = list()
      r1_fastq_list = list()
      r2_fastq_list = list()
      if os.path.isdir(fastq_dir):
        for root, _, files in os.walk(top=fastq_dir):
          if samplesheet_filename in files:
            samplesheet_list.append(
              os.path.join(root,samplesheet_filename))
          for file in files:
            if not fnmatch.fnmatch(file, 'Undetermined_'):
              if r1_fastq_regex.match(file):
                r1_fastq_list.\
                  append(os.path.join(root,file))
              elif r2_fastq_regex.match(file):
                r2_fastq_list.\
                  append(os.path.join(root,file))
        if len(r2_fastq_list) > 0 and \
           len(r1_fastq_list) != len(r2_fastq_list):
          raise ValueError(
                  'R1 {0} and R2 {1}'.format(
                    len(r1_fastq_list),
                    len(r2_fastq_list)))
        if samplesheet_file is None and \
           len(samplesheet_list)==1:
          self.samplesheet_file = samplesheet_list[0]                           # set samplesheet file name
        if len(samplesheet_list) > 1:
          raise ValueError(
                  'Found more than one samplesheet file for fastq dir {0}'.\
                  format(fastq_dir))
        if samplesheet_file is None and \
           len(samplesheet_list)==0:
          raise ValueError(
                  'No samplesheet file for fastq dir {0}'.\
                  format(fastq_dir))
      elif os.path.isfile(fastq_dir):
        if samplesheet_file is None:
          raise ValueError(
                  'Missing samplesheet file for fastq file {0}'.\
                  format(fastq_dir))
        if not fnmatch.fnmatch(file, 'Undetermined_'):
          if r1_fastq_regex.match(file):
            r1_fastq_list.\
              append(os.path.join(root,file))
          elif r2_fastq_regex.match(file):
            r2_fastq_list.\
              append(os.path.join(root,file))
      return r1_fastq_list, r2_fastq_list
    except Exception as e:
      raise ValueError(
              'Failed to get fastq and samplesheet, error: {0}'.\
              format(e))

  @staticmethod
  def _link_fastq_file_to_sample(sample_name,r1_fastq_list, r2_fastq_list):
    try:
      sample_files = \
        defaultdict(lambda: defaultdict(lambda: defaultdict()))
      r1_regex = \
        re.compile(
        sample_name+'_S\d+_L(\d+)_R1_\d+\.fastq(\.gz)?',
        re.IGNORECASE)
      for file1 in r1_fastq_list:
        if r1_regex.match(os.path.basename(file1)):
          m = r1_regex.match(os.path.basename(file1))
          lane_id = m.group(1).strip('0')
          sample_files[lane_id]['R1'] = file1
      if len(r2_fastq_list) > 0:
        r2_regex = \
          re.compile(
            sample_name+'_S\d+_L(\d+)_R2_\d+\.fastq(\.gz)?',
            re.IGNORECASE)
        for file2 in r2_fastq_list:
          if r2_regex.match(os.path.basename(file2)):
            m = r2_regex.match(os.path.basename(file2))
            lane_id = m.group(1).strip('0')
            sample_files[lane_id]['R2'] = file2
      return sample_files
    except Exception as e:
      raise ValueError(
              'Failed to link fastq to sample, error: {0}'.format(e))

  def _collect_fastq_and_sample_info(self):
    '''
    An internal method for collecting fastq and sample info
    '''
    try:
      seqrun_igf_id = self.seqrun_igf_id
      model_name = self.model_name
      flowcell_id = self.flowcell_id
      (r1_fastq_list, r2_fastq_list) = \
          self._get_fastq_and_samplesheet()
      samplesheet_file = self.samplesheet_file
      final_data = list()
      samplesheet_sc = \
        SampleSheet(infile=samplesheet_file)                                    # read samplesheet for single cell check
      samplesheet_sc.\
        filter_sample_data(
          condition_key='Description',
          condition_value=self.singlecell_tag,
          method='include')                                                     # keep only single cell samples
      if len(samplesheet_sc._data) >0:
        sc_new_data = \
          pd.DataFrame(samplesheet_sc._data).\
          drop(['Sample_ID','Sample_Name','index'],axis=1).\
          drop_duplicates().\
          to_dict(orient='records')                                              # remove duplicate entries from single cell samplesheet
        final_data.extend(sc_new_data)                                          # add single cell entries to the final dataset
      samplesheet_data = \
        SampleSheet(infile=samplesheet_file)
      samplesheet_data.\
        filter_sample_data(
          condition_key='Description',
          condition_value=self.singlecell_tag,
          method='exclude')                                                     # keep non single cell samples
      if len(samplesheet_data._data) > 0:
        final_data.\
          extend(samplesheet_data._data)                                        # add normal samples to final data

      fastq_files_list = list()
      for row in final_data:
        description = row['Description']
        if description==self.singlecell_tag:                                    # collect required values for single cell projects
          sample_name = row['Original_Sample_Name']
          sample_id = row['Original_Sample_ID']
        else:
          sample_name = row['Sample_Name']                                        # collect default values for normal projects
          sample_id = row['Sample_ID']
        project_name = row['Sample_Project']

        sample_files = \
          self._link_fastq_file_to_sample(
            sample_name,
            r1_fastq_list,
            r2_fastq_list)
        for lane, lane_files in sample_files.items():
          fastq_info = {
            'sample_igf_id':sample_id,
            'sample_name':sample_name,
            'project_igf_id':project_name,
            'lane_number':lane,
            'seqrun_igf_id':seqrun_igf_id,
            'platform_name':model_name,
            'flowcell_id':flowcell_id,
            'description':description }
          for read_type, filepath in lane_files.items():
            fastq_info.\
              update({read_type:filepath})                             # allowing only one file per lane per read type
            fastq_files_list.\
              append(fastq_info)                                       # adding entries per sample per lane
      return fastq_files_list
    except Exception as e:
      raise ValueError(
              'Failed to collect info, error: {0}'.format(e))

  @staticmethod
  def _count_fastq_reads(fastq_file):
    '''
    A static method for counting reads from the zipped and bzipped fastq files
    required params:
    fastq_file: A fastq file with absolute path
    '''
    try:
      if not os.path.exists(fastq_file):
        raise IOError(
                'fastq file {0} is not found'.format(fastq_file))
      if fnmatch.fnmatch(os.path.basename(fastq_file),'*.fastq.gz'):
        read_cmd = ['zcat',quote(fastq_file)]
      elif fnmatch.fnmatch(os.path.basename(fastq_file),'*.fastq.bz'):
        read_cmd = ['bzcat',quote(fastq_file)]
      elif fnmatch.fnmatch(os.path.basename(fastq_file),'*.fastq'):
        read_cmd = ['cat',quote(fastq_file)]
      else:
        raise ValueError(
                'file {0} is not recognised'.format(fastq_file))

      proc = \
        subprocess.\
          Popen(
            read_cmd,
            stdout=subprocess.PIPE)
      count_cmd = ['wc','-l']
      proc2 = \
        subprocess.\
          Popen(
            count_cmd,
            stdin=proc.stdout,
            stdout=subprocess.PIPE)
      proc.stdout.close()
      result = \
        int(proc2.communicate()[0].decode('UTF-8'))
      if result==0:
        raise ValueError(
                'Fastq file {0} has zero lines'.format(fastq_file))

      result = int(result/4)
      return result
    except Exception as e:
      raise ValueError(
              'Failed to count fastq reads, error: {0}'.format(e))


  def _calculate_experiment_run_and_file_info(self,data,restricted_list):
    try:
      if not isinstance(data, pd.Series):
        data = pd.Series(data)
      # set library id
      library_id = data.sample_igf_id
      # calcaulate experiment id
      experiment_id = \
        '{0}_{1}'.format(library_id,data.platform_name)
      data['library_name'] = library_id
      data['experiment_igf_id'] = experiment_id
      # calculate run id
      run_igf_id = \
        '{0}_{1}_{2}'.format(
          experiment_id,
          data.flowcell_id,
          data.lane_number)
      data['run_igf_id'] = run_igf_id
      # set collection name and type
      data['name'] = run_igf_id
      data['type'] = self.collection_type
      data['table'] = self.collection_table
      data['location'] = self.file_location
      # set file md5 and size
      if 'R1' in data:
        data['R1_md5'] = \
          calculate_file_checksum(
            filepath=data.R1,
            hasher='md5')
        data['R1_size'] = \
          os.path.getsize(data.R1)
        data['R1_READ_COUNT'] = \
          self._count_fastq_reads(
            fastq_file=data.R1)
      if 'R2' in data:
        data['R2_md5'] = \
          calculate_file_checksum(
            filepath=data.R2,
            hasher='md5')
        data['R2_size'] = \
          os.path.getsize(data.R2)
        data['R2_READ_COUNT'] = \
          self._count_fastq_reads(
            fastq_file=data.R2)
      # set library strategy
      library_layout = 'SINGLE'
      if 'R1' in data and 'R2' in data and \
        data.R1 is not None and data.R2 is not None:
        library_layout='PAIRED'
      data['library_layout'] = library_layout
      return data
    except Exception as e:
      raise ValueError(
              'Failed to calculate exp, run and file, error: {0}'.\
                format(e))


  @staticmethod
  def _reformat_file_group_data(data):
    try:
      if isinstance(data, pd.DataFrame):
        data = data.to_dict(orient='records')
      if not isinstance(data,list):
        raise ValueError(
                'Expecting list got {0}'.format(type(data)))
      reformatted_file_group_data = list()
      reformatted_file_data = list()
      for row in data:
        collection_name = None
        collection_type = None
        file_location = None
        if 'name' in row.keys():
          collection_name = row['name']
        if 'type' in row.keys():
          collection_type = row['type']
        if 'location' in row.keys():
          file_location = row['location']
        if 'R1' in row.keys():
          r1_file_path = row['R1']
          r1_file_size = \
            row['R1_size'] if 'R1_size' in row.keys() else None
          r1_file_md5 = \
            row['R1_md5'] if 'R1_md5' in row.keys() else None
          reformatted_file_data.\
            append({
              'file_path':r1_file_path,
              'md5':r1_file_md5,
              'location':file_location,
              'size':r1_file_size})
          reformatted_file_group_data.\
            append({
              'name':collection_name,
              'type':collection_type,
              'file_path':r1_file_path})
        if 'R2' in row.keys():
          r2_file_path = row['R2']
          r2_file_size = \
            row['R2_size'] if 'R2_size' in row.keys() else None
          r2_file_md5 = \
            row['R2_md5'] if 'R2_md5' in row.keys() else None
          reformatted_file_data.\
            append({
              'file_path':r2_file_path,
              'md5':r2_file_md5,
              'location':file_location,
              'size':r2_file_size})
          reformatted_file_group_data.\
            append({
              'name':collection_name,
              'type':collection_type,
              'file_path':r2_file_path})
      file_data = \
        pd.DataFrame(reformatted_file_data)
      file_data = file_data.dropna()                                              # removing rows witn None values
      file_group_data = \
        pd.DataFrame(reformatted_file_group_data)
      file_group_data = \
        file_group_data.dropna()                                                  # removing rows with None values
      return file_data, file_group_data
    except Exception as e:
      raise ValueError(
              'Failed to reformat file group data, error: {0}'.\
              format(e))


  def _write_manifest_file(self,file_data):
    '''
    An internal method for writing file data to the manifest file
    '''
    try:
      manifest_name = self.manifest_name
      fastq_dir = self.fastq_dir
      manifest_path = \
        os.path.join(fastq_dir,manifest_name)
      if os.path.exists(manifest_path):
        raise ValueError(
                'manifest file {0} already present'.\
                format(manifest_path))
      if isinstance(file_data, list):
        file_data = pd.DataFrame(file_data)                                     # convert file data to dataframe
      file_data['file_path'] = \
        file_data['file_path'].\
          map(
            lambda x: \
              os.path.relpath(x, start=fastq_dir))                              # replace filepath with relative path
      file_data = \
        file_data.drop(['location'],axis=1)                                     # remove file location info
      file_data.\
        to_csv(
          manifest_path,
          sep='\t',
          encoding='utf-8',
          index=False)                                                          # write data to manifest file
    except Exception as e:
      raise ValueError(
              'Failed to write manifest file, error: {0}'.\
              format(e))

  @staticmethod
  def _check_existing_data(data,dbsession,table_name,check_column='EXISTS'):
    try:
      if not isinstance(data, pd.Series):
        raise ValueError(
                'Expecting a data series and got {0}'.\
                format(type(data)))
      if table_name=='experiment':
        if 'experiment_igf_id' in data and \
           not pd.isnull(data['experiment_igf_id']):
          experiment_igf_id = data['experiment_igf_id']
          ea = \
            ExperimentAdaptor(**{'session':dbsession})
          experiment_exists = \
            ea.check_experiment_records_id(
              experiment_igf_id)
          if experiment_exists:                                                 # store data only if experiment is not existing
            data[check_column] = True
          else:
            data[check_column] = False
          return data
        else:
          raise ValueError(
                  'Missing or empty required column experiment_igf_id')
      elif table_name=='run':
        if 'run_igf_id' in data and \
           not pd.isnull(data['run_igf_id']):
          run_igf_id = data['run_igf_id']
          ra = RunAdaptor(**{'session':dbsession})
          run_exists = \
            ra.check_run_records_igf_id(run_igf_id)
          if run_exists:                                                        # store data only if run is not existing
            data[check_column] = True
          else:
            data[check_column] = False
          return data
        else:
          raise ValueError(
                  'Missing or empty required column run_igf_id')
      elif table_name=='collection':
        if 'name' in data and 'type' in data and \
           not pd.isnull(data['name']) and \
           not pd.isnull(data['type']):
          ca = CollectionAdaptor(**{'session':dbsession})
          collection_exists = \
            ca.check_collection_records_name_and_type(
              collection_name=data['name'],
              collection_type=data['type'])
          if collection_exists:
            data[check_column] = True
          else:
            data[check_column] = False
          return data
        else:
          raise ValueError(
                  'Missing or empty required column name or type')
      else:
        raise ValueError(
                'table {0} not supported yet'.format(table_name))
    except Exception as e:
      raise ValueError(
              'Failed to check existing data, error: {0}'.format(e))

  def _build_and_store_exp_run_and_collection_in_db(
        self,fastq_files_list,restricted_list=('10X')):
    '''
    An internal method for building db collections for the raw fastq files
    '''
    session_class = self.session_class
    db_connected = False
    try:
      restricted_list = list(restricted_list)
      dataframe = \
        pd.DataFrame(fastq_files_list)
      # calculate additional detail
      dataframe['library_name'] = None
      dataframe['experiment_igf_id'] = None
      dataframe['run_igf_id'] = None
      dataframe['name'] = None
      dataframe['type'] = None
      dataframe['table'] = None
      dataframe['location'] = None
      dataframe['R1_md5'] = None
      dataframe['R1_size'] = None
      dataframe['R1_READ_COUNT'] = None
      dataframe['R2_md5'] = None
      dataframe['R2_size'] = None
      dataframe['R2_READ_COUNT'] = None
      dataframe['library_layout'] = None
      dataframe = \
        dataframe.\
          apply(
            lambda data: \
              self._calculate_experiment_run_and_file_info(
                data,
                restricted_list),
            axis=1)
      # get file data
      file_group_columns = [
        'name',
        'type',
        'location',
        'R1',
        'R1_md5',
        'R1_size',
        'R2',
        'R2_md5',
        'R2_size']
      file_group_data = \
        dataframe.\
          loc[:,dataframe.columns.intersection(file_group_columns)]
      file_group_data = \
        file_group_data.drop_duplicates()
      (file_data,file_group_data) = \
        self._reformat_file_group_data(
          data=file_group_data)
      # get base session
      base = BaseAdaptor(**{'session_class':session_class})
      base.start_session()
      db_connected = True
      # get experiment data
      experiment_columns = \
        base.\
          get_table_columns(
            table_name=Experiment,
            excluded_columns=[
              'experiment_id',
              'project_id',
              'sample_id' ])
      experiment_columns.\
        extend([
          'project_igf_id',
          'sample_igf_id'])
      exp_data = \
        dataframe.\
          loc[:,dataframe.columns.intersection(experiment_columns)]
      exp_data = \
        exp_data.drop_duplicates()
      if exp_data.index.size > 0:
        exp_data['EXISTS'] = ''
        exp_data = \
          exp_data.apply(
            lambda x: \
              self._check_existing_data(
                data=x,
                dbsession=base.session,
                table_name='experiment',
                check_column='EXISTS'),
            axis=1)
        exp_data = \
          exp_data[exp_data['EXISTS']==False]                                   # filter existing experiments
        exp_data.\
          drop(
            'EXISTS',
            axis=1,
            inplace=True)                                                       # remove extra columns
        exp_data = \
          exp_data[pd.isnull(exp_data['experiment_igf_id'])==False]             # filter exp with null values
      # get run data
      run_columns = \
        base.\
          get_table_columns(
            table_name=Run,
            excluded_columns=[
              'run_id',
              'seqrun_id',
              'experiment_id',
              'date_created',
              'status'])
      run_columns.\
        extend([
          'seqrun_igf_id',
          'experiment_igf_id',
          'R1_READ_COUNT',
          'R2_READ_COUNT'])
      run_data = \
        dataframe.\
          loc[:,dataframe.columns.intersection(run_columns)]
      run_data = run_data.drop_duplicates()
      formatted_existing_run_data = list()
      if run_data.index.size > 0:
        run_data['EXISTS'] = ''
        run_data = \
          run_data.\
            apply(
              lambda x: \
                self._check_existing_data(\
                  data=x,
                  dbsession=base.session,
                  table_name='run',
                  check_column='EXISTS'),
              axis=1)
        run_data = \
          run_data[run_data['EXISTS']==False]                                   # filter existing runs
        existing_run_data = \
          run_data[run_data['EXISTS']==True]                                    # get existing runs
        if existing_run_data.index.size > 0:
          if 'R1_READ_COUNT' not in existing_run_data.columns or \
             'R2_READ_COUNT' not in existing_run_data.columns:
            raise ValueError('Missing required columns for run_attribute update')
          existing_data_columns = [
            'run_igf_id',
            'R1_READ_COUNT',
            'R2_READ_COUNT']
          existing_data_columns = [
            i for i in existing_data_columns
              if i in existing_run_data.columns]                                # reset data columns
          existing_run_data = \
            existing_run_data[existing_data_columns]                            # keep only required data
          existing_run_data = \
            existing_run_data.\
              to_dict(orient='records')
          for entry in existing_run_data:
            row = {
              'run_igf_id': entry.get('run_igf_id')}
            for col in existing_data_columns:
              if col != 'run_igf_id' and \
                 entry.get(col) is not None:
                row.\
                  update({
                    col: entry.get(col)})
            formatted_existing_run_data.\
              append(row)
        run_data.\
          drop(
            'EXISTS',
            axis=1,
            inplace=True)                                                       # remove extra columns
        run_data = \
          run_data[pd.isnull(run_data['run_igf_id'])==False]                    # filter run with null values
      # get collection data
      collection_columns = ['name', 'type', 'table']
      collection_data = \
        dataframe.\
          loc[:,dataframe.columns.intersection(collection_columns)]
      collection_data = \
        collection_data.drop_duplicates()
      if collection_data.index.size > 0:
        collection_data['EXISTS'] = ''
        collection_data = \
          collection_data.\
            apply(
              lambda x: \
                self._check_existing_data(
                  data=x,
                  dbsession=base.session,
                  table_name='collection',
                  check_column='EXISTS'),
              axis=1)
        collection_data = \
          collection_data[collection_data['EXISTS']==False]                     # filter existing collection
        collection_data.\
          drop(
            'EXISTS',
            axis=1,
            inplace=True)                                                       # remove extra columns
        collection_data = \
          collection_data[pd.isnull(collection_data['name'])==False]            # filter collection with null values
      # store experiment to db
      if exp_data.index.size > 0:
        ea = ExperimentAdaptor(**{'session': base.session})
        ea.store_project_and_attribute_data(
          data=exp_data,
          autosave=False)
        base.session.flush()
      # store run to db
      if run_data.index.size > 0:
        ra = RunAdaptor(**{'session': base.session})
        ra.store_run_and_attribute_data(
          data=run_data,
          autosave=False)
        base.session.flush()
      # update run_adapter records
      if len(formatted_existing_run_data) > 0:
        ra = RunAdaptor(**{'session': base.session})
        ra.update_run_attribute_records_by_igfid(
          formatted_existing_run_data,
          autosave=False)
        base.session.flush()
      # store file to db
      fa = FileAdaptor(**{'session': base.session})
      fa.store_file_and_attribute_data(
        data=file_data,
        autosave=False)
      base.session.flush()
      # store collection to db
      ca = CollectionAdaptor(**{'session': base.session})
      if collection_data.index.size > 0:
        ca.store_collection_and_attribute_data(
          data=collection_data,
          autosave=False)
        base.session.flush()
      ca.create_collection_group(
        data=file_group_data,
        autosave=False)
      base.commit_session()
      self._write_manifest_file(file_data)
    except Exception as e:
      if db_connected:
        base.rollback_session()
      raise ValueError(
              'Failed to build exp, run and collection, error: {0}'.\
              format(e))
    finally:
      if db_connected:
        base.close_session()
