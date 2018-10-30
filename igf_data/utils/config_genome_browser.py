import re,os
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.igfTables import Base,Project,Sample,Experiment,Collection,Collection_group,File,Pipeline,Pipeline_seed

class config_genome_browser:
  '''
  A class for configuring genome browser input files for analysis track visualization
  
  :param dbsession_class: A database session class
  :param project_igf_id: A project igf id
  :param collection_type_list: A list of collection types to include in the track
  :param pipeline_name: Name of the analysis pipeline for status checking
  :param collection_table: Name of file collection table name
  :param species_name: Species name for ref genome fetching
  :param ref_genome_type: Reference genome type for remote tracks
  :param track_file_type: Additional track file collection types
  :param analysis_path_prefix: Top level dir name for analysis files, default 'analysis'
  :param analysis_dir_structure_list: List of keywords for sub directory paths, default ['sample_igf_id']
  '''
  def __init__(self,dbsession_class,project_igf_id,collection_type_list,
               pipeline_name,collection_table,species_name,ref_genome_type,
               track_file_type=None,analysis_path_prefix='analysis',
               analysis_dir_structure_list=['sample_igf_id']):
    self.dbsession_class=dbsession_class
    self.project_igf_id=project_igf_id
    self.collection_type_list=collection_type_list
    self.pipeline_name=pipeline_name
    self.collection_table=collection_table
    self.species_name=species_name
    self.ref_genome_type=ref_genome_type
    self.track_file_type=track_file_type
    self.analysis_path_prefix=analysis_path_prefix
    self.analysis_dir_structure_list=analysis_dir_structure_list

  def _fetch_track_files_with_metadata(self,level='experiment'):
    '''
    An internal method for fetching track files with the metadata information
    
    :param level: Specific level for fetching metadata information, default 'experiment'
    :returns: A pandas dataframe object
    '''
    try:
      if level == 'experiment':
        base=BaseAdaptor(**{'session_class':self.dbsession_class})
        base.start_session()
        query=base.session.\
              query(Project.project_igf_id,
                    Sample.sample_igf_id,
                    Experiment.experiment_igf_id,
                    Experiment.library_source,
                    Experiment.library_strategy,
                    Experiment.experiment_type,
                    Collection.name,
                    Collection.type,
                    File.file_path,
                    Pipeline.pipeline_name,
                    Pipeline_seed.status
                   ).\
              join(Sample).\
              join(Experiment).\
              join(Collection,Collection.name==Experiment.experiment_igf_id).\
              join(Collection_group).\
              join(File).\
              join(Pipeline_seed,Pipeline_seed.seed_id==Experiment.experiment_id).\
              join(Pipeline).\
              filter(Project.project_id==Sample.project_id).\
              filter(Sample.sample_id==Experiment.sample_id).\
              filter(Sample.status=='ACTIVE').\
              filter(Experiment.status=='ACTIVE').\
              filter(Collection.type.in_(self.collection_type_list)).\
              filter(Collection.table==self.collection_table).\
              filter(Collection.collection_id==Collection_group.collection_id).\
              filter(File.file_id==Collection_group.file_id).\
              filter(File.status=='ACTIVE').\
              filter(Pipeline_seed.status=='FINISHED').\
              filter(Pipeline.pipeline_id==Pipeline_seed.pipeline_id).\
              filter(Pipeline.pipeline_name==self.pipeline_name).\
              filter(Project.project_igf_id==self.project_igf_id)
        records=base.fetch_records(query=query,
                                   output_mode='dataframe')
        base.close_session()
        return records
      else:
        raise ValueError('No support for {0} tracks'.format(level))

    except:
      raise