#!/usr/bin/env python
import os, subprocess
from shutil import copytree
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.utils.tools.cellranger.cellranger_count_utils import get_cellranger_count_input_list,check_cellranger_count_outrput

class RunBcl2Fastq(IGFBaseProcess):
  '''
  A ehive process class for running cellranger count pipeline
  '''
  def param_defaults(self):
    params_dict=super(RunBcl2Fastq,self).param_defaults()
    params_dict.update({
        'force_overwrite':True,
        'cellranger_exe':None,
        'reference_type':'cellranger_reference',
        'cellranger_options':'{ "--jobmode":"pbspro","--localcores":"1","--localmem":"4","--mempercore":"4","--maxjobs":"20"}',
      })
    return params_dict

  def run(self):
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      igf_session_class=self.param_required('igf_session_class')
      species_name=self.param('species_name')
      reference_type=self.param('reference_type')
      
      ca=CollectionAdaptor(**{'session_class':igf_session_class})
      ca.start_session()
      reference_genome=ca.get_collection_files(collection_name=species_name,
                                               collection_type=reference_type,
                                               output_mode='one_or_none')       # get reference genome info from database
      ca.close_session()
      if reference_genome is None:
        raise ValueError('No unique reference genome found for sample {0}'.\
                         format(sample_igf_id))
    except:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise