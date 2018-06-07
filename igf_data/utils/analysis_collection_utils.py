import os
from igf_data.utils.fileutils import get_datestamp_label
from igf_data.utils.fileutils import preprocess_path_name
from igf_data.utils.fileutils import get_file_extension
from igf_data.utils.fileutils import move_file
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor

class Analysis_collection_utils:
  def __init__(self,project_igf_id,dbsession_class,base_path=None,sample_igf_id=None,experiment_igf_id=None,
               run_igf_id=None,collection_name=None,collection_type=None,collection_table=None,
               rename_file=True,add_datestamp=True,tag_name=None,
               analysis_name=None, allowed_collection=('sample','experiment','run','project')):
    '''
    A class for dealing with analysis file collection. It has specific method for moving analysis files
    to a specific directory structure and rename the file using a uniform rule, if required.
    E.g. "<collection_name>_<analysis_name>_<tag>_<datestamp>.<original_suffix>"
    
    :param project_igf_id: A project name string
    :param dbsession_class: A database session class
    :param sample_igf_id: A sample name string, default None
    :param experiment_igf_id: An experiment name string, default None
    :param run_igf_id: A run name string, default None 
    :param collection_name: Collection name information for file, default None
    :param collection_type: Collection type information for file, default None
    :param collection_table: Collection table information for file, default None
    :param base_path: A base filepath to move file while loading, default None
    :param rename_file: Rename file based on collection_table type while loading, default True
    :param add_datestamp: Add datestamp while loading the file
    :param analysis_name: Analysis name for the file, required for renaming while loading, default None
    :param tag_name: Additional tag for filename,default None
    :param allowed_collection: List of allowed collection tables
                                 sample
                                 experiment
                                 run
                                 project
    '''
    try:
      self.project_igf_id=project_igf_id
      self.dbsession=dbsession
      self.sample_igf_id=sample_igf_id
      self.experiment_igf_id=experiment_igf_id
      self.run_igf_id=run_igf_id
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
                                           autosave_db=True,force=True):
    '''
    A method for create or update analysis file collection in db
    
    :param file_path: file path to load as db collection
    :param dbsession: An active database session
    :param withdraw_exisitng_collection: Remove existing collection group
    :param autosave_db: Save changes to database, default True
    :param force: Toggle for removing existing file collection, default True
    '''
    try:
      ca=CollectionAdaptor(**{'session':dbsession})
      fa=FileAdaptor(**{'session':dbsession})
      file_exists=fa.check_file_records_file_path(file_path=file_path)          # check if file already present in db
      if file_exists and force:
        fa.remove_file_data_for_file_path(file_path=file_path,
                                          autosave=autosave_db)                 # remove entry from file table

      collection_exists=ca.get_collection_files(collection_name=self.collection_name,
                                                collection_type=self.collection_type)
      if len(collection_exists.index) >0:
         if withdraw_exisitng_collection:
           remove_data=[{'name':self.collection_name,
                         'type':self.collection_type,
                       }]
           ca.remove_collection_group_info(data=remove_data,
                                           autosave=autosave_db)                # removing all existing collection groups for the collection name and type
         else:
           collection_data=[{'name':self.collection_name,
                             'type':self.collection_type,
                             'table':self.collection_table,
                             'file_path':file_path}]
           ca.load_file_and_create_collection(data=collection_data,
                                              autosave=autosave_db)             # load file, collection and create collection group
    except:
      raise


  def load_file_to_disk_and_db(self,input_file_list,withdraw_exisitng_collection=True,
                               autosave_db=True,file_suffix=None,force=True):
    '''
    A method for loading analysis results to disk and database. File will be moved to a new path if base_path is present.
    Directory structure of the final path is based on the collection_table information.
    
    project - base_path/project_igf_id/analysis_name
    sample - base_path/project_igf_id/sample_igf_id/analysis_name
    experiment - base_path/project_igf_id/sample_igf_id/experiment_igf_id/analysis_name
    run - base_path/project_igf_id/sample_igf_id/experiment_igf_id/run_igf_id/analysis_name
    
    :param input_file_list: A list of input file to load, all using the same collection info
    :param withdraw_exisitng_collection: Remove existing collection group
    :param autosave_db: Save changes to database, default True
    :param file_suffix: Use a specific file suffix, use None if it should be same as original file
                        e.g. input.vcf.gz to  output.vcf.gz
    :param force: Toggle for removing existing file, default True
    '''
    try:
      dbconnected=False
      if self.collection_name is None or \
         self.collection_type is None or \
         self.collection_table is None:
        raise ValueError('File collection information is incomplete')           # check for collection information

      if self.collection_type not in allowed_collection:
        raise ValueError('collection type {0} not allowed'.\
                         format(self.collection_type))                          # check for collection table information

      if self.collection_type is 'sample' and \
         self.sample_igf_id is None:
        raise ValueError('Missing sample name for building sample collection')  # check for sample info

      if self.collection_type is 'experiment' and \
         ( self.sample_igf_id is None or \
           self.experiment_igf_id is None):
        raise ValueError('Missing sample or experiment name for building experiment collection') # check for experiment info

      if self.collection_type is 'run' and \
        ( self.sample_igf_id is None or \
          self.experiment_igf_id is None or \
          self.run_igf_id is None):
        raise ValueError('Missing sample, experiment or run name for building run collection') # check for run info

      if self.rename_file and self.analysis_name is None:
        raise ValueError('Analysis name is required for renaming file')         # check analysis name

      base=BaseAdaptor(**{'session_class':self.dbsession})
      base.start_session()                                                      # connect to db
      dbconnected=True
      for input_file in input_file_list:
        final_path=''
        if self.base_path is None:                                              # do not move file if base_path is absent
          final_path=os.path.dirname(input_file)
        else:                                                                   # move file path
          if self.collection_type == 'project':
            final_path=os.path.join(base_path,
                                    self.project_igf_id,
                                    self.analysis_name)                         # final path for project
          elif self.collection_type == 'sample':
            final_path=os.path.join(base_path,
                                    self.project_igf_id,
                                    self.sample_igf_id,
                                    self.analysis_name)                         # final path for sample
          elif self.collection_type == 'experiment':
            final_path=os.path.join(base_path,
                                    self.project_igf_id,
                                    self.sample_igf_id,
                                    self.experiment_igf_id,
                                    self.analysis_name)                         # final path for experiment
          elif self.collection_type == 'run':
            final_path=os.path.join(base_path,
                                    self.project_igf_id,
                                    self.sample_igf_id,
                                    self.experiment_igf_id,
                                    self.run_igf_id,
                                    self.analysis_name)                         # final path for run

        if self.rename_file:
          new_filename=''
          if self.collection_type == 'project':
            new_filename=self.project_igf_id
          elif self.collection_type == 'sample':
            new_filename=self.sample_igf_id
          elif self.collection_type == 'experiment':
            new_filename=self.experiment_igf_id
          elif self.collection_type == 'run':
            new_filename=self.run_igf_id

          if new_filename =='':
            raise ValueError('New filename not found for input file {0}'.\
                             format(input_file))

          new_filename='{0}_{1}'.format(new_filename,
                                        self.analysis_name)
          if self.add_datestamp:
            datestamp=get_datestamp_label()                                     # collect datestamp
            new_filename='{0}_[1}'.format(new_filename,
                                          datestamp)                            # add datestamp to filepath

          file_suffix=get_file_extension(input_file=input_file)                 # collect file suffix
          if file_suffix =='':
            raise ValueError('Missing file extension for new file name of {0}'.\
                             format(input_file))                                # raise error if not file suffix found

          new_filename='{0}.{1}'.format(new_filename,file_suffix)               # add file suffix to the new name
          final_path=os.path.join(final_path,
                                  new_filename)                                 # get new filepath
          self.create_or_update_analysis_collection(file_path=final_path,
                                                    dbsession=base.session,
                                                    autosave_db=autosave_db)    # load new file collection in db
          move_file(source_path=input_file,
                    destinationa_path=final_path,
                    force=force)                                                # move or overwrite file to destination dir
          if autosave_db:
            base.commit_session()                                               # save changes to db for each file

      base.commit_session()                                                     # save changes to db
      base.close_session()                                                      # close db connection
    except:
      if dbconnected:
        base.rollback_session()
        base.close_session()
      raise