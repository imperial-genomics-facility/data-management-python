import os, re, fnmatch
from collections import defaultdict
from igf_data.illumina.samplesheet import SampleSheet
import pandas as pd
from igf_data.igfdb.igfTables import Experiment, Run
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.utils.fileutils import calculate_file_checksum

class Collect_seqrun_fastq_to_db:
  def __init__(self,fastq_dir,model_name,seqrun_igf_id,session_class,flowcell_id,\
               samplesheet_file=None,samplesheet_filename='SampleSheet.csv',\
               collection_type='demultiplexed_fastq',file_location='HPC_PROJECT'):
    self.fastq_dir=fastq_dir
    self.samplesheet_file=samplesheet_file
    self.samplesheet_filename=samplesheet_filename
    self.seqrun_igf_id=seqrun_igf_id
    self.model_name=model_name
    self.session_class=session_class
    self.collection_type=collection_type
    self.file_location=file_location
    self.flowcell_id=flowcell_id
    
    
  def find_fastq_and_build_db_collection(self):
    '''
    A method for finding fastq files and samplesheet under a run directory
    and loading the new files to db with their experiment and run information
    It calculates following entries:
    library_name: Same as sample_id unless mentioned in 'Description' field of
                  samplesheet
    experiment_igf_id: library_name combined with the platform name
                       same library sequenced in different platform will be added
                       as separate experiemnt
    run_igf_id: experiment_igf_id combined with sequencing flowcell_id and lane_id
    collection name: Same as run_igf_id, fastq files will be added to db collection
                     using this id
    collection type: Default type for fastq file collections are 'demultiplexed_fastq'
    file_location: default value is 'HPC_PROJECT'  
    '''
    try:
      fastq_files_list=self._collect_fastq_and_sample_info()
      self._build_and_store_exp_run_and_collection_in_db(fastq_files_list=fastq_files_list)
    except:
      raise
    
    
  def _get_fastq_and_samplesheet(self):
    fastq_dir=self.fastq_dir
    samplesheet_file=self.samplesheet_file
    samplesheet_filename=self.samplesheet_filename
    r1_fastq_regex=re.compile(r'\S+_R1_\d+\.fastq(\.gz)?', re.IGNORECASE)
    r2_fastq_regex=re.compile(r'\S+_R2_\d+\.fastq(\.gz)?', re.IGNORECASE)
    samplesheet_list=list()
    r1_fastq_list=list()
    r2_fastq_list=list()
    for root, dirs, files in os.walk(top=fastq_dir):
      if samplesheet_filename in files:
        samplesheet_list.append(os.path.join(root,samplesheet_filename))
        
      for file in files:
        if not fnmatch.fnmatch(file, 'Undetermined_'):
          if r1_fastq_regex.match(file):
            r1_fastq_list.append(os.path.join(root,file))
          elif r2_fastq_regex.match(file):
            r2_fastq_list.append(os.path.join(root,file))
        
    if len(r2_fastq_list) > 0 and len(r1_fastq_list) != len(r2_fastq_list):
      raise ValueError('R1 {0} and R2 {1}'.format(len(r1_fastq_list),\
                                                  len(r2_fastq_list)))
  
    if samplesheet_file is None and len(samplesheet_list)==1:
        self.samplesheet_file=samplesheet_list[0]                               # set samplesheet file name
        
    if len(samplesheet_list) > 1:
      raise ValueError('Found more than one samplesheet file for fastq dir {0}'.\
                       format(fastq_dir))
         
    if samplesheet_file is None and len(samplesheet_list)==0:
      raise ValueError('No samplesheet file for fastq dir {0}'.\
                       format(fastq_dir))
  
    return r1_fastq_list, r2_fastq_list


  def _link_fastq_file_to_sample(self,sample_name,r1_fastq_list, r2_fastq_list):
    sample_files=defaultdict(lambda: defaultdict(lambda: defaultdict()))
    r1_regex=re.compile(sample_name+'_S\d+_L(\d+)_R1_\d+\.fastq(\.gz)?',\
                        re.IGNORECASE)
    for file1 in r1_fastq_list:
      if r1_regex.match(os.path.basename(file1)):
        m=r1_regex.match(os.path.basename(file1))
        lane_id=m.group(1).strip('0')
        sample_files[lane_id]['R1']=file1
            
    if len(r2_fastq_list) > 0:
      r2_regex=re.compile(sample_name+'_S\d+_L(\d+)_R2_\d+\.fastq(\.gz)?',\
                          re.IGNORECASE)
      for file2 in r2_fastq_list:
        if r2_regex.match(os.path.basename(file2)):
          m=r2_regex.match(os.path.basename(file2))
          lane_id=m.group(1).strip('0')
          sample_files[lane_id]['R2']=file2
    return sample_files


  def _collect_fastq_and_sample_info(self):
    fastq_dir=self.fastq_dir
    samplesheet_filename=self.samplesheet_filename
    seqrun_igf_id=self.seqrun_igf_id
    model_name=self.model_name
    flowcell_id=self.flowcell_id
    (r1_fastq_list, r2_fastq_list)=\
        self._get_fastq_and_samplesheet()
    samplesheet_file=self.samplesheet_file
    samplesheet_data=SampleSheet(infile=samplesheet_file)
    fastq_files_list=list()
    for row in samplesheet_data._data:
      sample_name=row['Sample_Name']
      sample_id=row['Sample_ID']
      project_name=row['Sample_Project']
      description=row['Description']
      sample_files=self._link_fastq_file_to_sample(sample_name,r1_fastq_list, \
                                                   r2_fastq_list)
      for lane, lane_files in sample_files.items():
        fastq_info={'sample_igf_id':sample_id,
                    'sample_name':sample_name,
                    'project_igf_id':project_name,
                    'lane_number':lane,
                    'seqrun_igf_id':seqrun_igf_id,
                    'platform_name':model_name,
                    'flowcell_id':flowcell_id,
                    'description':description
                   }
        for read_type, filepath in lane_files.items():
          fastq_info.update({read_type:filepath})                               # allowing only one file per lane per read type
          fastq_files_list.append(fastq_info)                                   # adding entries per sample per lane
    return fastq_files_list


  def _calculate_experiment_run_and_file_info(self,data,restricted_list):
    if not isinstance(data, pd.Series):
      data=pd.Series(data)
    # set library id
    library_id=None
    if data.description and data.description not in restricted_list:
      library_id=data.description                                    
    else:
      library_id=data.sample_igf_id                                      
  
    # calcaulate experiment id
    experiment_id='{0}_{1}'.format(library_id,data.platform_name)         
    data['library_name']=library_id
    data['experiment_igf_id']=experiment_id
    # calculate run id
    run_igf_id='{0}_{1}_{2}'.format(experiment_id, \
                                    data.flowcell_id, \
                                    data.lane_number)
    data['run_igf_id']=run_igf_id
    # set collection name and type
    data['name']=run_igf_id
    data['type']=self.collection_type
    data['location']=self.file_location
    # set file md5 and size
    if 'R1' in data:
      data['R1_md5']=calculate_file_checksum(filepath=data.R1, \
                                             hasher='md5')
      data['R1_size']=os.path.getsize(data.R1)
    if 'R2' in data:
      data['R2_md5']=calculate_file_checksum(filepath=data.R2, \
                                             hasher='md5')
      data['R2_size']=os.path.getsize(data.R2)
    # set library strategy
    library_layout='SINGLE'
    if 'R1' in data and 'R2' in data and \
      data.R1 is not None and data.R2 is not None:
      library_layout='PAIRED'
    
    data['library_layout']=library_layout
    return data


  def _reformat_file_group_data(self,data):
    if isinstance(data, pd.DataFrame):
      data=data.to_dict(orient='records')
        
    if not isinstance(data,list):
      raise ValueError('Expecting list got {0}'.format(type(data)))
        
    reformatted_file_group_data=list()
    reformatted_file_data=list()

    for row in data:
      collection_name=None
      collection_type=None
      file_location=None
      if 'name' in row.keys():
        collection_name=row['name']
      if 'type' in row.keys():
        collection_type=row['type']
      if 'location' in row.keys():
        file_location=row['location']
      if 'R1' in row.keys():
        r1_file_path=row['R1']
        r1_file_size=row['R1_size'] if 'R1_size' in row.keys() else None
        r1_file_md5=row['R1_md5'] if 'R1_md5' in row.keys() else None
        reformatted_file_data.append({'file_path':r1_file_path,
                                      'md5':r1_file_md5,
                                      'location':file_location,
                                      'size':r1_file_size})
        reformatted_file_group_data.append({'name':collection_name,
                                            'type':collection_type,
                                            'file_path':r1_file_path})
      if 'R2' in row.keys():
        r2_file_path=row['R2']
        r2_file_size=row['R2_size'] if 'R2_size' in row.keys() else None
        r2_file_md5=row['R2_md5'] if 'R2_md5' in row.keys() else None
        reformatted_file_data.append({'file_path':r2_file_path,
                                      'md5':r2_file_md5,
                                      'location':file_location,
                                      'size':r2_file_size})
        reformatted_file_group_data.append({'name':collection_name,
                                            'type':collection_type,
                                            'file_path':r2_file_path})
    return pd.DataFrame(reformatted_file_data), pd.DataFrame(reformatted_file_group_data)


  def _build_and_store_exp_run_and_collection_in_db(self,fastq_files_list, restricted_list=['10X']):
    session_class=self.session_class
    db_connected=False
    try:
      dataframe=pd.DataFrame(fastq_files_list)
      # calculate additional detail
      dataframe=dataframe.apply(lambda data: \
                                self._calculate_experiment_run_and_file_info(data, 
                                                               restricted_list),\
                                axis=1)
      # get file data
      file_group_columns=['name','type','location',
                          'R1','R1_md5','R1_size','R2',
                          'R2_md5','R2_size']
      file_group_data=dataframe.loc[:,file_group_columns]
      file_group_data=file_group_data.drop_duplicates().dropna()
      (file_data,file_group_data)=self._reformat_file_group_data(data=file_group_data)
      # get base session
      base=BaseAdaptor(**{'session_class':session_class})
      base.start_session()
      db_connected=True
      # get experiment data
      experiment_columns=base.get_table_columns(table_name=Experiment, \
                                                excluded_columns=['experiment_id', 
                                                                  'project_id', 
                                                                  'sample_id' ])
      experiment_columns.extend(['project_igf_id', 
                                 'sample_igf_id'])
      exp_data=dataframe.loc[:,experiment_columns]
      exp_data=exp_data.drop_duplicates()
      # get run data
      run_columns=base.get_table_columns(table_name=Run, \
                                         excluded_columns=['run_id', 
                                                           'seqrun_id', 
                                                           'experiment_id',
                                                           'date_created',
                                                           'status'
                                                          ])
      run_columns.extend(['seqrun_igf_id', 
                          'experiment_igf_id'])
      run_data=dataframe.loc[:,run_columns]
      run_data=run_data.drop_duplicates()
      # get collection data
      collection_columns=['name',
                          'type']
      collection_data=dataframe.loc[:,collection_columns]
      collection_data=collection_data.drop_duplicates()
      # store experiment to db
      ea=ExperimentAdaptor(**{'session':base.session})
      ea.store_project_and_attribute_data(data=exp_data,autosave=False)
      base.session.flush()
      # store run to db
      ra=RunAdaptor(**{'session':base.session})
      ra.store_run_and_attribute_data(data=run_data,autosave=False)
      base.session.flush()
      # store file to db
      fa=FileAdaptor(**{'session':base.session})
      fa.store_file_and_attribute_data(data=file_data,autosave=False)
      base.session.flush()
      # store collection to db
      ca=CollectionAdaptor(**{'session':base.session})
      ca.store_collection_and_attribute_data(data=collection_data,\
                                             autosave=False)
      base.session.flush()
      ca.create_collection_group(data=file_group_data,autosave=False)
      base.commit_session()
    except:
      if db_connected:
        base.rollback_session()
      raise
    finally:
      if db_connected:
        base.close_session()
      