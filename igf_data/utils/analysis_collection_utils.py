import os
from igf_data.utils.fileutils import get_datestamp_label
from igf_data.utils.fileutils import preprocess_path_name
from igf_data.utils.fileutils import get_file_extension

class Analysis_collection_utils:
  def __init__(self,project_igf_id,dbsession,base_path=None,sample_igf_id=None,experiment_igf_id=None,
               run_igf_id=None,collection_name=None,collection_type=None,collection_table=None,
               rename_file=True,add_datestamp=True,tag_name=None,
               analysis_name=None, allowed_collection=('sample','experiment','run','project')):
    '''
    A class for dealing with analysis file collection.
    
    :param project_igf_id: A project name string
    :param dbsession: An active database session
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
    :param allowed_collection: List of allowed collection tables,
     
                                 'sample'
                                 'experiment'
                                 'run'
                                 'project'
    '''
    try:
      self.project_igf_id=project_igf_id
      self.sample_igf_id=sample_igf_id
      self.experiment_igf_id=experiment_igf_id
      self.run_igf_id=run_igf_id
      self.collection_name=collection_name
      self.collection_type=collection_type
      self.collection_table=collection_table
      self.base_path=base_path
      self.rename_file=rename_file
      self.add_datestamp=add_datestamp
      self.analysis_name=analysis_name
      self.tag_name=tag_name
    except:
      raise

  def load_file_to_disk_and_db(self,input_file,withdraw_exisitng_collection=True,
                               autosave_db=True,file_suffix=None):
    '''
    A method for loading analysis results to disk and database. File will be moved to a new path if base_path is present.
    Directory structure of the final path is based on the collection_table information.
    
    project - base_path/project_igf_id/analysis_name
    sample - base_path/project_igf_id/sample_igf_id/analysis_name
    experiment - base_path/project_igf_id/sample_igf_id/experiment_igf_id/analysis_name
    run - base_path/project_igf_id/sample_igf_id/experiment_igf_id/run_igf_id/analysis_name
    
    :param input_file: Input file to load
    :param withdraw_exisitng_collection: Remove existing collection group
    :param autosave_db: Save changes to database, default True
    :param file_suffix: Use a specific file suffix, use None if it should be same as original file
                        e.g. input.vcf.gz to  output.vcf.gz
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

      final_path=''
      if self.base_path is None:                                                # do not move file if base_path is absent
        final_path=os.path.dirname(input_file)
      else:                                                                     # move file path
        if self.collection_type == 'project':
          final_path=os.path.join(base_path,
                                  self.project_igf_id,
                                  self.analysis_name)                           # final path for project
        elif self.collection_type == 'sample':
          final_path=os.path.join(base_path,
                                  self.project_igf_id,
                                  self.sample_igf_id,
                                  self.analysis_name)                           # final path for sample
        elif self.collection_type == 'experiment':
          final_path=os.path.join(base_path,
                                  self.project_igf_id,
                                  self.sample_igf_id,
                                  self.experiment_igf_id,
                                  self.analysis_name)                           # final path for experiment
        elif self.collection_type == 'run':
          final_path=os.path.join(base_path,
                                  self.project_igf_id,
                                  self.sample_igf_id,
                                  self.experiment_igf_id,
                                  self.run_igf_id,
                                  self.analysis_name)                           # final path for run
    except:
      raise