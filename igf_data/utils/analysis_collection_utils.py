import os
from igf_data.utils.fileutils import get_datestamp_label

class Analysis_collection_utils:
  def __init__(self,project_igf_id,dbsession,sample_igf_id=None,experiment_igf_id=None,
               run_igf_id=None,collection_name=None,collection_type=None,collection_table=None,
               rename_file=True,add_datestamp=True,tag_name=None,
               analysis_name=None, allowed_collection=('sample','experiment','run','project')):
    '''
    A class for dealing with analysis file collection
    
    :param project_igf_id: A project name string
    :param dbsession: An active database session
    :param sample_igf_id: A sample name string, default None
    :param experiment_igf_id: An experiment name string, default None
    :param run_igf_id: A run name string, default None 
    :param collection_name: Collection name information for file, default None
    :param collection_type: Collection type information for file, default None
    :param collection_table: Collection table information for file, default None
    :param rename_file: Rename file based on collection_table type while loading, default True
    :param add_datestamp: Add datestamp while loading the file
    :param analysis_name: Analysis name for the file, required for renaming while loading, default None
    :param tag_name: Additional tag for filename,default None
    :param allowed_collection: List of allowed collection tables,
     
                                 'sample'
                                 'experiment'
                                 'run'
                                 'project'
    '''
    try:
      project_igf_id=self.project_igf_id
      sample_igf_id=self.sample_igf_id
      experiment_igf_id=self.experiment_igf_id
      run_igf_id=self.run_igf_id
      collection_name=self.collection_name
      collection_type=self.collection_type
      collection_table=self.collection_table
      rename_file=self.rename_file
      add_datestamp=self.add_datestamp
      analysis_name=self.analysis_name
      tag_name=self.tag_name
    except:
      raise

  def load_file_to_disk_and_db(self,withdraw_exisitng_collection=True,
                               autosave_db=True):
    '''
    A method for loading analysis results to disk and database
    
    :param withdraw_exisitng_collection: Remove existing collection group
    :param autosave_db: Save changes to database, default True
    '''
    try:
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


    except:
      raise