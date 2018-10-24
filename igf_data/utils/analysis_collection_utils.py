import os
from igf_data.utils.fileutils import get_datestamp_label
from igf_data.utils.fileutils import preprocess_path_name
from igf_data.utils.fileutils import get_file_extension
from igf_data.utils.fileutils import move_file
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor

class Analysis_collection_utils:
  '''
  A class for dealing with analysis file collection. It has specific method for moving analysis files
  to a specific directory structure and rename the file using a uniform rule, if required.
  Example '<collection_name>_<analysis_name>_<tag>_<datestamp>.<original_suffix>'
    
  :param dbsession_class: A database session class
  :param collection_name: Collection name information for file, default None
  :param collection_type: Collection type information for file, default None
  :param collection_table: Collection table information for file, default None
  :param base_path: A base filepath to move file while loading, default 'None' for no file move
  :param rename_file: Rename file based on collection_table type while loading, default True
  :param add_datestamp: Add datestamp while loading the file
  :param analysis_name: Analysis name for the file, required for renaming while loading, default None
  :param tag_name: Additional tag for filename,default None
  :param allowed_collection: List of allowed collection tables
                                    
                                    sample,
                                    experiment,
                                    run,
                                    project
  '''
  def __init__(self,dbsession_class,base_path=None,collection_name=None,
               collection_type=None,collection_table=None,rename_file=True,
               add_datestamp=True,tag_name=None,analysis_name=None,
               allowed_collection=('sample','experiment','run','project')):
    try:
      self.dbsession_class=dbsession_class
      self.collection_name=collection_name
      self.collection_type=collection_type
      if collection_table not in allowed_collection:
        raise ValueError('Collection table {0} not supported').\
                         format(collection_table)                               # check collection table information

      self.collection_table=collection_table
      self.base_path=base_path
      self.rename_file=rename_file
      self.add_datestamp=add_datestamp
      self.analysis_name=analysis_name
      self.tag_name=tag_name
    except:
      raise


  def create_or_update_analysis_collection(self,file_path,dbsession,
                                           withdraw_exisitng_collection=True,
                                           autosave_db=True,force=True,
                                           remove_file=False):
    '''
    A method for create or update analysis file collection in db. Required elements will be
    collected from database if base_path element is given.
    
    :param file_path: file path to load as db collection
    :param dbsession: An active database session
    :param withdraw_exisitng_collection: Remove existing collection group
    :param autosave_db: Save changes to database, default True
    :param remove_file: A toggle for removing existing file from disk, default False
    :param force: Toggle for removing existing file collection, default True
    '''
    try:
      ca=CollectionAdaptor(**{'session':dbsession})
      
      collection_exists=ca.get_collection_files(collection_name=self.collection_name,
                                                collection_type=self.collection_type)
      if len(collection_exists.index) >0 and \
          withdraw_exisitng_collection:
        remove_data=[{'name':self.collection_name,
                      'type':self.collection_type,
                    }]
        ca.remove_collection_group_info(data=remove_data,
                                        autosave=autosave_db)                   # removing all existing collection groups for the collection name and type

      fa=FileAdaptor(**{'session':dbsession})
      file_exists=fa.check_file_records_file_path(file_path=file_path)          # check if file already present in db
      if file_exists and force:
        fa.remove_file_data_for_file_path(file_path=file_path,
                                          remove_file=remove_file,
                                          autosave=autosave_db)                 # remove entry from file table and disk

      collection_data=[{'name':self.collection_name,
                        'type':self.collection_type,
                        'table':self.collection_table,
                        'file_path':file_path}]
      ca.load_file_and_create_collection(data=collection_data,
                                         autosave=autosave_db)                  # load file, collection and create collection group
    except:
      raise


  def load_file_to_disk_and_db(self,input_file_list,withdraw_exisitng_collection=True,
                               autosave_db=True,file_suffix=None,force=True,
                               remove_file=False):
    '''
    A method for loading analysis results to disk and database. File will be moved to a new path if base_path is present.
    Directory structure of the final path is based on the collection_table information.
    
    Following will be the final directory structure if base_path is present
    
    project - base_path/project_igf_id/analysis_name
    sample - base_path/project_igf_id/sample_igf_id/analysis_name
    experiment - base_path/project_igf_id/sample_igf_id/experiment_igf_id/analysis_name
    run - base_path/project_igf_id/sample_igf_id/experiment_igf_id/run_igf_id/analysis_name
    
    :param input_file_list: A list of input file to load, all using the same collection info
    :param withdraw_exisitng_collection: Remove existing collection group, DO NOT use this while loading a list of files
    :param autosave_db: Save changes to database, default True
    :param file_suffix: Use a specific file suffix, use None if it should be same as original file
                        e.g. input.vcf.gz to  output.vcf.gz
    :param force: Toggle for removing existing file, default True
    :param remove_file: A toggle for removing existing file from disk, default False
    :returns: A list of final filepath
    '''
    try:
      project_igf_id=None
      sample_igf_id=None
      experiment_igf_id=None
      experiment_igf_id=None
      run_igf_id=None
      output_path_list=list()                                                   # define empty output list
      dbconnected=False
      if self.collection_name is None or \
         self.collection_type is None or \
         self.collection_table is None:
        raise ValueError('File collection information is incomplete')           # check for collection information

      base=BaseAdaptor(**{'session_class':self.dbsession_class})
      base.start_session()                                                      # connect to db
      dbconnected=True
      if self.base_path is not None:                                            
        if self.collection_table == 'sample':
          sa=SampleAdaptor(**{'session':base.session})
          sample_igf_id=self.collection_name
          sample_exists=sa.check_sample_records_igf_id(sample_igf_id=sample_igf_id)
          if not sample_exists:
            raise ValueError('Sample {0} not found in db'.\
                             format(sample_igf_id))

          project_igf_id=sa.fetch_sample_project(sample_igf_id=sample_igf_id)     # fetch project id for sample
        elif self.collection_table == 'experiment':
          ea=ExperimentAdaptor(**{'session':base.session})
          experiment_igf_id=self.collection_name
          experiment_exists=\
            ea.check_experiment_records_id(experiment_igf_id=experiment_igf_id)
          if not experiment_exists:
            raise ValueError('Experiment {0} not present in database'.\
                             format(experiment_igf_id))

          (project_igf_id,sample_igf_id)=\
              ea.fetch_project_and_sample_for_experiment(experiment_igf_id=experiment_igf_id) # fetch project and sample id for experiment
        elif self.collection_table == 'run':
          ra=RunAdaptor(**{'session':base.session})
          run_igf_id=self.collection_name
          run_exists=ra.check_run_records_igf_id(run_igf_id=run_igf_id)
          if not run_exists:
            raise ValueError('Run {0} not found in database'.\
                             format(run_igf_id))

          (project_igf_id,sample_igf_id,experiment_igf_id)=\
            ra.fetch_project_sample_and_experiment_for_run(run_igf_id=run_igf_id) # fetch project, sample and experiment id for run
        elif self.collection_table == 'project':
          pa=ProjectAdaptor(**{'session':base.session})
          project_igf_id=self.collection_name
          project_exists=\
            pa.check_project_records_igf_id(project_igf_id=project_igf_id)
          if not project_exists:
            raise ValueError('Project {0} not found in database'.\
                             format(project_igf_id))

      if self.rename_file and self.analysis_name is None:
        raise ValueError('Analysis name is required for renaming file')         # check analysis name

      for input_file in input_file_list:
        final_path=''
        if self.base_path is None:                                              # do not move file if base_path is absent
          final_path=os.path.dirname(input_file)
        else:                                                                   # move file path
          if self.collection_table == 'project':
            if project_igf_id is None:
              raise ValueError('Missing project id for collection {0}'.\
                               format(self.collection_name))

            final_path=os.path.join(self.base_path,
                                    project_igf_id,
                                    self.analysis_name)                         # final path for project
          elif self.collection_table == 'sample':
            if project_igf_id is None or \
               sample_igf_id is None:
              raise ValueError('Missing project and sample id for collection {0}'.\
                               format(self.collection_name))

            final_path=os.path.join(self.base_path,
                                    project_igf_id,
                                    sample_igf_id,
                                    self.analysis_name)                         # final path for sample
          elif self.collection_table == 'experiment':
            if project_igf_id is None or \
               sample_igf_id is None or \
               experiment_igf_id is None:
              raise ValueError('Missing project,sample and experiment id for collection {0}'.\
                               format(self.collection_name))

            final_path=os.path.join(self.base_path,
                                    project_igf_id,
                                    sample_igf_id,
                                    experiment_igf_id,
                                    self.analysis_name)                         # final path for experiment
          elif self.collection_table == 'run':
            if project_igf_id is None or \
               sample_igf_id is None or \
               experiment_igf_id is None or \
               run_igf_id is None:
              raise ValueError('Missing project,sample,experiment and run id for collection {0}'.\
                               format(self.collection_name))

            final_path=os.path.join(self.base_path,
                                    project_igf_id,
                                    sample_igf_id,
                                    experiment_igf_id,
                                    run_igf_id,
                                    self.analysis_name)                         # final path for run

        if self.rename_file:
          new_filename=self.get_new_file_name(input_file=input_file,
                                              file_suffix=file_suffix)
          final_path=os.path.join(final_path,
                                  new_filename)                                 # get new filepath
        else:
          final_path=os.path.join(final_path,
                                  os.path.basename(input_file))

        if final_path !=input_file:                                             # move file if its required
          final_path=preprocess_path_name(input_path=final_path)                # remove unexpected characters from file path
          move_file(source_path=input_file,
                    destinationa_path=final_path,
                    force=force)                                                # move or overwrite file to destination dir

        output_path_list.append(final_path)                                     # add final path to the output list
        self.create_or_update_analysis_collection(\
                 file_path=final_path,
                 dbsession=base.session,
                 withdraw_exisitng_collection=withdraw_exisitng_collection,
                 remove_file=remove_file,
                 autosave_db=autosave_db)                                       # load new file collection in db
        if autosave_db:
          base.commit_session()                                                 # save changes to db for each file

      base.commit_session()                                                     # save changes to db
      base.close_session()                                                      # close db connection
      return output_path_list
    except:
      if dbconnected:
        base.rollback_session()
        base.close_session()
      raise


  def get_new_file_name(self,input_file,file_suffix=None):
    '''
    A method for fetching new file name
    
    :param input_file: An input filepath
    :param file_suffix: A file suffix
    '''
    try:
      new_filename=self.collection_name                                         # use collection name to rename file
      if new_filename =='':
        raise ValueError('New filename not found for input file {0}'.\
                         format(input_file))

      new_filename='{0}_{1}'.format(new_filename,
                                    self.analysis_name)
      if self.tag_name is not None:
         new_filename='{0}_{1}'.format(new_filename,
                                       self.tag_name)                           # add tagname to filepath

      if self.add_datestamp:
        datestamp=get_datestamp_label()                                         # collect datestamp
        new_filename='{0}_{1}'.format(new_filename,
                                      datestamp)                                # add datestamp to filepath

      if file_suffix is None:
        file_suffix=get_file_extension(input_file=input_file)                   # collect file suffix

      if file_suffix =='':
        raise ValueError('Missing file extension for new file name of {0}'.\
                         format(input_file))                                    # raise error if not file suffix found

      new_filename='{0}.{1}'.format(new_filename,file_suffix)                   # add file suffix to the new name
      return new_filename
    except:
      raise

