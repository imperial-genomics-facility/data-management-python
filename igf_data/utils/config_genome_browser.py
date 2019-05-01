import re,os
import pandas as pd
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir,remove_dir,copy_local_file
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils
from jinja2 import Template,Environment, FileSystemLoader, select_autoescape
from igf_data.igfdb.igfTables import Base,Project,Sample,Experiment,Collection,Collection_group,File,Pipeline,Pipeline_seed

class Config_genome_browser:
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
               analysis_dir_structure_list=('sample_igf_id')):
    self.dbsession_class=dbsession_class
    self.project_igf_id=project_igf_id
    self.collection_type_list=collection_type_list
    self.pipeline_name=pipeline_name
    self.collection_table=collection_table
    self.species_name=species_name
    self.ref_genome_type=ref_genome_type
    self.track_file_type=track_file_type
    self.analysis_path_prefix=analysis_path_prefix
    self.analysis_dir_structure_list=list(analysis_dir_structure_list)

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
              join(Sample,Project.project_id==Sample.project_id).\
              join(Experiment,Sample.sample_id==Experiment.sample_id).\
              join(Collection,Collection.name==Experiment.experiment_igf_id).\
              join(Collection_group,Collection.collection_id==Collection_group.collection_id).\
              join(File,File.file_id==Collection_group.file_id).\
              join(Pipeline_seed,Pipeline_seed.seed_id==Experiment.experiment_id).\
              join(Pipeline,Pipeline.pipeline_id==Pipeline_seed.pipeline_id).\
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

  def build_biodalliance_config(self,template_file,output_file):
    '''
    A method for building biodalliance specific config file
    :param template_file: A template file path
    :param output_file: An output filepath
    '''
    try:
      if not os.path.exists(template_file):
        raise IOError('Template file {0} not found'.\
                      format(template_file))

      species_specific_data=\
        self._get_species_specific_info_for_track(species_name=self.species_name) # get species specific data
      ref_genome=Reference_genome_utils(\
                   genome_tag=self.species_name,
                   dbsession_class=self.dbsession_class,
                   genome_twobit_uri_type=self.ref_genome_type)                 # setup ref genome utils
      ref_genome_url=ref_genome.get_twobit_genome_url()
      if self.track_file_type is not None:
        track_file_urls=\
          ref_genome.get_generic_ref_files(collection_type=self.track_file_type,
                                           check_missing=True)                  # fetch additional track files

      records=self._fetch_track_files_with_metadata(level='experiment')
      if len(records.index)>0:
        records=records.\
                apply(lambda x: self._reformat_track_info(data=x),
                      axis=1)                                                   # reformat data for track config
        template_env=\
          Environment(loader=FileSystemLoader(\
                               searchpath=os.path.dirname(template_file)),
                      autoescape=select_autoescape(['html', 'xml','ipynb']))
        template=template_env.\
                 get_template(os.path.basename(template_file))

        temp_dir=get_temp_dir()
        temp_output=os.path.join(temp_dir,
                                 os.path.basename(output_file))                 # get temp output file
        template.\
        stream(
          chrname=species_specific_data.get('chrname'),
          startPosition=species_specific_data.get('startPosition'),
          endPosition=species_specific_data.get('endPosition'),
          speciesCommonName=species_specific_data.get('speciesCommonName'),
          speciesTaxon=species_specific_data.get('speciesTaxon'),
          refAuthority=species_specific_data.get('refAuthority'),
          refVersion=species_specific_data.get('refVersion'),
          speciesUcscName=species_specific_data.get('speciesUcscName'),
          ensemblSpecies=species_specific_data.get('ensemblSpecies'),
          twoBitRefRemoteUrl=ref_genome_url,
          bwdata=records.to_dict(orient='records')
        ).\
        dump(temp_output)
        if not os.path.exists(temp_output):
          raise IOError('Failed to write temp output file')

        copy_local_file(source_path=temp_output,
                        destinationa_path=output_file,
                        force=True)                                             # copy output config file
    except:
      raise

  @staticmethod
  def _get_species_specific_info_for_track(species_name):
    '''
    A species specific method for populating track config file
    
    :param species_name: A species name tag
    :returns: A dictionary with required species specific arguments
    '''
    try:
      ref_data=dict()
      if species_name.upper() == 'HG38':
        ref_data={'chrname':1,
                  'startPosition':30700000,
                  'endPosition':30900000,
                  'speciesCommonName':'Human',
                  'ensemblSpecies':'human',
                  'speciesTaxon':9606,
                  'refAuthority':'GRC',
                  'refVersion':38,
                  'speciesUcscName':'hg38'}
      else:
        raise ValueError('No support for species {0}'.\
                         format(species_name))
      return ref_data
    except:
      raise


  def _reformat_track_info(self,data):
    '''
    An internal method for reformatting data for track
    
    :param data: A pandas data series
    :returns: A pandas data series with updated entries
    '''
    try:
      tag_name=None
      if not isinstance(data,pd.Series):
        raise AttributeError('Expecting a pandas data series and got {0}'.\
                             format(type(data)))

      sample_igf_id=data.get('sample_igf_id')
      experiment_igf_id=data.get('experiment_igf_id')
      experiment_type=data.get('experiment_type')
      collection_type=data.get('type')
      file_path=data.get('file_path')

      if file_path is None:
        raise ValueError('No filepath found:{0}'.\
                         format(data.to_dict()))

      file_path=os.path.basename(file_path)
      if collection_type=='STAR_BIGWIG':                                        # adding collection type specific codes
        str1_pattern=re.compile(r'\S+\.str1\.')
        str2_pattern=re.compile(r'\S+\.str2\.')
        if re.match(str1_pattern,file_path):
          tag_name='str1'
        elif re.match(str2_pattern,file_path):
          tag_name='str2'

      if sample_igf_id is None or \
         experiment_type is None or \
         collection_type is None:
        raise ValueError('Required metadata not found: {0}'.\
                         format(data.to_dict()))

      track_name='{0}.{1}.{2}'.\
                 format(sample_igf_id,
                        experiment_type,
                        collection_type)
      if tag_name is not None:
        track_name='{0}.{1}'.format(track_name,
                                    tag_name)

      track_path_list=[self.analysis_path_prefix]
      for item in self.analysis_dir_structure_list:
        if data.get(item) is None:
          raise ValueError('path item {0} not found: {0}'.\
                           format(data.to_dict()))
        track_path_list.append(data.get(item))

      track_path_list.append(file_path)
      track_path=os.path.join(*track_path_list)
      data['track_name']=track_name
      data['track_desc']=track_name
      data['track_path']=track_path
      return data
    except:
      raise

