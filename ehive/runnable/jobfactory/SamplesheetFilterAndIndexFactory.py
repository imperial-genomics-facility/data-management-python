#!/usr/bin/env python
import os, copy
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory
from igf_data.illumina.samplesheet import SampleSheet

class SamplesheetFilterAndIndexFactory(IGFBaseJobFactory):
  '''
  A class for filtering samplesheet based on project name and lane id and index length
  It creates a job factory and pass on the output samplesheet names
  '''
  def param_defaults(self):
    params_dict=super(IGFBaseJobFactory,self).param_defaults()
    params_dict.update({
        'samplesheet_filename':'SampleSheet.csv',
      })
    return params_dict
  
  
  def run(self):
    try:
      samplesheet_file=self.param_required('samplesheet')
      project_name=self.param_required('project_name')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      base_work_dir=self.param_required('base_work_dir')
      samplesheet_filename=self.param('samplesheet_filename')
      
      job_name=self.job_name()
      work_dir=os.path.join(base_work_dir,seqrun_igf_id,job_name)               # get work directory name
      if not os.path.exists(work_dir):
        os.mkdir(work_dir)                                                      # create work directory
        
      if not os.path.exists(samplesheet_file):
        raise IOError('Samplesheet file {0} not found'.format(samplesheet_file))
      
      samplesheet=SampleSheet(infile=samplesheet_file)                          # read samplesheet
      samplesheet.filter_sample_data(condition_key='Sample_Project', 
                                     condition_value=project_name, 
                                     method='include')                          # keep only selected project
      lanes=samplesheet.get_lane_count()                                        # get samplesheet lanes
      data_group=dict()
      
      if not len(lanes)>0:
        raise ValueError('project {0} is not present in the samplesheet {1}'.\
                         format(project_name,samplesheet_file))
        
      if len(lanes)>1:
        for lane_id in lanes:
          samplesheet_project_data=copy.deepcopy(samplesheet)                   # deep copy samplesheet object
          samplesheet_project_data.filter_sample_data(condition_key='Lane', \
                                                      condition_value=lane_id, \
                                                      method='include')         # keep only selected lane
          data_group[lane_id]=samplesheet_project_data.\
                              group_data_by_index_length()                      # group data by lane
      else:
        data_group[1]=samplesheet.group_data_by_index_length()                  # For MiSeq and NextSeq
        
      sub_tasks=list()                                                          # create empty sub_tasks data structure
      for lane_id in data_group.keys():
        for index_length in data_group[lane_id].keys():
          output_file=os.path.join(work_dir, '{0}_{1}_{2}_{3}'.\
                                   format(samplesheet_filename,
                                          project_name,
                                          lane_id,
                                          index_length))                        # get output file name
          data_group[lane_id][index_length].\
           print_sampleSheet(outfile=output_file)                               # write output file
          sub_tasks.append({'project_name':project_name,
                            'flowcell_lane':lane_id,
                            'index_length':index_length,
                            'samplesheet':output_file})                         # append sub_tasks
      self.param('sub_tasks',sub_tasks)                                         # send sub_tasks to the dataflow
      message='seqrun: {0}, project:{1}, lanes:{2}'.format(seqrun_igf_id,\
                                                           project_name,\
                                                           ','.join(lanes))
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.comment_asana_task(task_name=seqrun_igf_id, comment=message)         # send log to asana
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise