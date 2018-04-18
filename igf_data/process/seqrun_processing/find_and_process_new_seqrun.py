import os, sys, hashlib, json, fnmatch
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Seqrun
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.illumina.runinfo_xml import RunInfo_xml
from igf_data.illumina.runparameters_xml import RunParameter_xml
from igf_data.utils.fileutils import calculate_file_checksum
from igf_data.utils.dbutils import read_dbconf_json


def find_new_seqrun_dir(path, dbconfig):
  '''
  A method for check and finding new sequencing run directory
  '''
  all_seqrun_dir=[f for f in os.listdir(path) if os.path.isdir(os.path.join(path,f))]    # list of all directories present under path
  new_seqrun_dir=check_seqrun_dir_in_db(all_seqrun_dir,dbconfig)                          
  valid_seqrun_dir=check_finished_seqrun_dir(seqrun_dir=new_seqrun_dir, seqrun_path=path)
  return valid_seqrun_dir

def validate_samplesheet_for_seqrun(seqrun_info,schema_json,output_dir,samplesheet_file='SampleSheet.csv'):
  '''
  A method for validating samplesheet and writing errors to a report file
  
  :param seqrun_info : A dictionary containing seqrun name and path as key and values
  :param schema_json : A json schema for samplesheet validation
  :param output_dir : A directory path for writing output report files
  :param samplesheet_file : Samplesheet filename, default 'SampleSheet.csv'
  
  :return new_seqrun_info : A new dictionary containing seqrun name and path as key and values
  :return error_file_list : A dictionary containing seqrun name and error file paths as key and values
  '''
  new_seqrun_info=dict()
  error_file_list=dict()
  error_file_suffix='samplesheet_validation_failed.txt'
  if not os.path.exists(output_dir):
    raise IOError('Report output directory {0} not found'.format(output_dir))   # check for existing dir path

  try:
    for seqrun_name, seqrun_path in seqrun_info.items():
      samplesheet=os.path.join(seqrun_path,samplesheet_file)                    # get samplesheet file
      samplesheet_data=SampleSheet(infile=samplesheet)                          # read samplesheet data
      errors=samplesheet_data.validate_samplesheet_data(schema_json)            # validate samplesheet data
      if len(errors)>0:
        error_file=os.path.join(output_dir,
                                '{0}_{1}'.format(seqrun_name,
                                                 error_file_suffix))            # get error file path
        errors=[e if isinstance(e,str) else '{0} :{1}'.format(e.message,e.path) \
                  for e in errors]
        errors_str='\n'.join(errors)
        with open(error_file,'w') as fh:
          fh.write(errors_str)                                                  # write error file
        error_file_list.update({seqrun_name :error_file})
      else:
        new_seqrun_info.update({seqrun_name : seqrun_path})
      return new_seqrun_info, error_file_list
  except:
    raise

def check_for_registered_project_and_sample(seqrun_info,dbconfig,samplesheet_file='SampleSheet.csv'):
  '''
  A method for fetching project and sample records from samplesheet and checking for
  registered samples in db
  
  required params:
  seqrun_info: A dictionary containing seqrun name and path as key and values
  dbconfig: A database configuration file
  samplesheet_file: Name of samplesheet file, default is SampleSheet.csv
  '''
  try:
    msg=''
    new_seqrun_info=dict()
    dbparams=read_dbconf_json(dbconfig)
    base=BaseAdaptor(**dbparams)
    base.start_session()                                                          # connect to db
    sa=SampleAdaptor(**{'session':base.session})
    pa=ProjectAdaptor(**{'session':base.session})
    for seqrun_name, seqrun_path in seqrun_info.items():
      samplesheet=os.path.join(seqrun_path,samplesheet_file)                    # get samplesheet file
      samplesheet_data=SampleSheet(infile=samplesheet)                          # read samplesheet data
      for row in samplesheet_data._data:
        sample_id=row['Sample_ID']
        project_id=row['Sample_Project']
        record_exists=sa.check_project_and_sample(project_igf_id=project_id,\
                                                  sample_igf_id=sample_id)      # check for record in db
        if not record_exists:
          if msg =='':
            msg='missing sample {0} and project {1} for run {2}'.\
                format(sample_id, project_id, seqrun_name)
          else:
            msg='{0} \n missing sample {1} and project {2} for run {3}'.\
                format(msg, sample_id, project_id, seqrun_name)
                
        project_authority_exists=pa.check_data_authority_for_project(project_igf_id=project_id)
        if not project_authority_exists:
          if msg =='':
            msg='missing user info for project {0} for run {1}'.\
                format(project_id, seqrun_name)
          else:
            msg='{0} \n missing user info for project {1} for run {2}'.\
                format(msg, project_id, seqrun_name)
        if record_exists and project_authority_exists:
          new_seqrun_info.update({seqrun_name : seqrun_path})
    base.close_session()
    return new_seqrun_info, msg
  except:
    raise


def check_finished_seqrun_dir(seqrun_dir, seqrun_path, required_files=['RTAComplete.txt','SampleSheet.csv','RunInfo.xml']):
  '''
  A method for checking complete sequencing run directory
  '''
  valid_seqruns=dict()
  for seqrun in seqrun_dir:
    skip=0
    for serun_file in required_files:
      required_path=os.path.join(seqrun_path,seqrun,serun_file)
      if not os.path.exists(required_path):
        skip=1
      elif int(os.path.getsize(required_path))==0:
        skip=1
    if skip==0:
      valid_seqruns[seqrun]=os.path.join(seqrun_path,seqrun)
  return valid_seqruns


def check_seqrun_dir_in_db(all_seqrun_dir,dbconfig):
  '''
  A method for checking existing seqrun dirs in database
  required params:
  all_seqrun_dir: list of seqrun dirs to check
  dbconfig: dbconfig
  '''
  dbparam=read_dbconf_json(dbconfig)
  sra=SeqrunAdaptor(**dbparam)
  sra.start_session()
  sra_data=sra.fetch_records(sra.session.query(Seqrun.seqrun_igf_id),output_mode='object')
  existing_runs=set(s[0] for s in sra_data)
  sra.close_session() 
  all_runs=set(all_seqrun_dir)
  new_runs=list(all_runs.difference(existing_runs))
  return new_runs


def calculate_file_md5(seqrun_info, md5_out, seqrun_path, file_suffix='md5.json', exclude_dir=[]):
  '''
  A method for file md5 calculation for all the sequencing run files
  Output is a dictionary of json files
  {seqrun_name: seqrun_md5_list_path}
  Format of the json file
  [{"seqrun_file_name":"file_path","file_md5":"md5_value"}]
  '''
  seqrun_and_md5=dict()
  for seqrun_name, seqrun_path in seqrun_info.items():
    file_list_with_md5=list()
    output_json_file=os.path.join(md5_out,'{0}.{1}'.format(seqrun_name, file_suffix))
    for root_path,dirs,files in os.walk(seqrun_path, topdown=True):
      dirs[:]=[ d for d in dirs if d not in exclude_dir ]                                     # exclude listed dires from search
      if len(files)>0:
        for file_name in files:
          if not fnmatch.fnmatch(file_name, '*.fastq.gz'):
            file_path=os.path.join(root_path,file_name)
            if os.path.exists(file_path):
              file_md5=calculate_file_checksum(filepath=file_path)                              # calculate file checksum
              file_rel_path=os.path.relpath(file_path, start=seqrun_path)                       # get relative filepath
              file_list_with_md5.append({"seqrun_file_name":file_rel_path,"file_md5":file_md5})

    with open(output_json_file, 'w') as output_json:
      json.dump(file_list_with_md5, output_json, indent=4)                                    # write json md5 list

    seqrun_and_md5[seqrun_name]=output_json_file
  return seqrun_and_md5


def prepare_seqrun_for_db(seqrun_name, seqrun_path, session_class):
  '''
  A method for preparing seqrun data for database
  '''
  try:
    runinfo_file=os.path.join(seqrun_path,'RunInfo.xml')
    runinfo_data=RunInfo_xml(xml_file=runinfo_file)
    platform_name=runinfo_data.get_platform_number()
    reads_stats=runinfo_data.get_reads_stats()
    flowcell_id=runinfo_data.get_flowcell_name()
  
    seqrun_data=dict()
    seqrun_data['seqrun_igf_id']=seqrun_name
    seqrun_data['platform_igf_id']=platform_name
    seqrun_data['flowcell_id']=flowcell_id

    read_count=1
    index_count=1
    for read_id in sorted(reads_stats.keys()):
      if reads_stats[read_id]['isindexedread'] == 'Y':
        # its index
        seqrun_data['index{0}'.format(index_count)]=reads_stats[read_id]['numcycles']
        index_count+=1
      elif  reads_stats[read_id]['isindexedread'] == 'N':
        # its read
        seqrun_data['read{0}'.format(read_count)]=reads_stats[read_id]['numcycles']
        read_count+=1
      else:
        raise ValueError('unknown value for isindexedread: {0}'.format(reads_stats[read_id]['isindexedread']))

    pl=PlatformAdaptor(**{'session_class':session_class})
    pl.start_session()
    pl_data=pl.fetch_platform_records_igf_id(platform_igf_id=platform_name)
    pl.close_session()

    if (pl_data.model_name=='HISEQ4000'):
      runparameters_file=os.path.join(seqrun_path,'runParameters.xml')
      runparameters_data=RunParameter_xml(xml_file=runparameters_file)
      flowcell_type=runparameters_data.get_hiseq_flowcell()
      # add flowcell information for hiseq runs
      seqrun_data['flowcell']=flowcell_type

      if flowcell_type is None:
        raise ValueError('unknown flowcell type for sequencing run model {0}'.format(pl_data.model_name))
    else:
      seqrun_data['flowcell']=pl_data.model_name  # adding flowcell info for remaining platforms
    return seqrun_data
  except:
    raise


def seed_pipeline_table_for_new_seqrun(pipeline_name, dbconfig):
  '''
  A method for seeding pipelines for the new seqruns
  required params:
  pipeline_name: A pipeline name
  dbconfig: A dbconfig file
  '''
  dbparam=None
  with open(dbconfig, 'r') as json_data:
    dbparam=json.load(json_data)

  try:
    pa=PipelineAdaptor(**dbparam)
    pa.start_session()
    pa.seed_new_seqruns(pipeline_name=pipeline_name)
  except:
    raise
  finally:
    pa.close_session()

def load_seqrun_files_to_db(seqrun_info, seqrun_md5_info, dbconfig, file_type='ILLUMINA_BCL_MD5'):
  '''
  A method for loading md5 lists to collection and files table
  '''
  dbparam=None
  with open(dbconfig, 'r') as json_data:
    dbparam=json.load(json_data)

  seqrun_data=list()
  seqrun_md5_collection_data=list()
  seqrun_md5_file_data=list()
  seqrun_file_collection=list()

  base=BaseAdaptor(**dbparam)
  session_class=base.get_session_class()

  for seqrun_name, seqrun_path in seqrun_info.items():
    seqrun_data.append(prepare_seqrun_for_db(seqrun_name, seqrun_path, session_class))
    seqrun_md5_collection_data.append({'name':seqrun_name, 'type':file_type,'table':'seqrun' })
    seqrun_md5_file=seqrun_md5_info[seqrun_name]
    file_md5=calculate_file_checksum(seqrun_md5_file)
    file_size=os.path.getsize(seqrun_md5_file)
    seqrun_md5_file_data.append({'file_path':seqrun_md5_file,'location':'ORWELL','md5':file_md5, 'size':file_size})
    seqrun_file_collection.append({'name':seqrun_name, 'type':file_type, 'file_path':seqrun_md5_file})
    
  try:
    base.start_session()
    # store seqrun info
    sra=SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data, autosave=False)
  
    # store collection
    ca=CollectionAdaptor(**{'session':base.session})
    ca.store_collection_and_attribute_data(data=seqrun_md5_collection_data, autosave=False)
    ca.session.flush()
    
    # store files
    fa=FileAdaptor(**{'session':base.session})
    fa.store_file_and_attribute_data(data=seqrun_md5_file_data, autosave=False)
    fa.session.flush()

    # store file collection
    ca.create_collection_group(data=seqrun_file_collection, autosave=False)
 
    base.commit_session()
  except:
    base.rollback_session()
    raise
  finally:
    base.close_session()

