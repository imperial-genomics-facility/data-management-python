#!/usr/bin/env python
import os, subprocess
from shutil import copytree, copy2
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.process.moveBclFilesForDemultiplexing import moveBclFilesForDemultiplexing


class RunBcl2Fastq(IGFBaseProcess):
  '''
  A process class for running tool bcl2fastq
  '''
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    params_dict.update({
        'runinfo_filename':'RunInfo.xml',
        'samplesheet_filename':'SampleSheet.csv',
        'fastq_dir_label':'fastq',
        'force_overwrite':True,
        'bcl2fastq_exe':'bcl2fastq',
        'bcl2fastq_options':{'-r':'1','-w':'1','-p':'1','--barcode-mismatches':'1', '--auto-set-to-zero-barcode-mismatches':''},
      })
    return params_dict
  
  def run(self):
    try:
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      seqrun_date=self.param_required('seqrun_date')
      flowcell_id=self.param_required('flowcell_id')
      flowcell_lane=self.param_required('flowcell_lane')
      project_name=self.param_required('project_name')
      index_length=self.param_required('index_length')
      seqrun_local_dir=self.param_required('seqrun_local_dir')
      bases_mask=self.param_required('basesmask')
      base_work_dir=self.param_required('base_work_dir')
      base_fastq_dir=self.param_required('base_fastq_dir')
      samplesheet_file=self.param_required('samplesheet')
      runinfo_filename=self.param('runinfo_filename')
      bcl2fastq_exe=self.param('bcl2fastq_exe')
      bcl2fastq_options=self.param('bcl2fastq_options')
      force_overwrite=self.param('force_overwrite')
      fastq_dir_label=self.param('fastq_dir_label')
      samplesheet_filename=self.param('samplesheet_filename')
      
      seqrun_dir=os.path.join(seqrun_local_dir,seqrun_igf_id)                   # local seqrun dir
      runinfo_file=os.path.join(seqrun_dir,runinfo_filename)                    # seqrun runinfo file
      if not os.path.exists(samplesheet_file):
        raise IOError('samplesheet file {0} not found'.format(samplesheet_file))
      
      if not os.path.exists(runinfo_file):
        raise IOError('Runinfo file {0} not found'.format(runinfo_file))
      
      lane_index='{0}_{1}'.format(flowcell_lane,index_length)                   # get label for lane and index length
      output_dir_label=os.path.join(project_name,\
                                    fastq_dir_label,\
                                    seqrun_date,\
                                    flowcell_id,\
                                    lane_index)                                 # output dir label
      output_fastq_dir=os.path.join(base_fastq_dir,output_dir_label)             # output fastq dir
      
      if os.path.exists(output_fastq_dir) and force_overwrite:
        remove_dir(output_fastq_dir)                                            # remove fastq directory if its already present
              
      seqrun_temp_dir=get_temp_dir()                                            # create a new input directory in TMPDIR
      move_file=moveBclFilesForDemultiplexing(input_dir=seqrun_dir,
                                              output_dir=seqrun_temp_dir,
                                              samplesheet=samplesheet_file,
                                              run_info_xml=runinfo_file)        # get lists of files to move to TMPDIR
      move_file.copy_bcl_files()                                                # move files to TMPDIR
      
      output_temp_dir=get_temp_dir()                                            # create a new output directory in TMPDIR
      #bcl2fastq_param=[[param,value] if value else [param] 
      #                 for param, value in bcl2fastq_options.items()]           # remove empty values
      #bcl2fastq_param=[col for row in bcl2fastq_param for col in row]           # flatten sub lists
      bcl2fastq_cmd=[bcl2fastq_exe,
                     '--runfolder-dir',seqrun_temp_dir,
                     '--sample-sheet',samplesheet_file,
                     '--output-dir',output_temp_dir,
                     ]                                                          # bcl2fastq base parameters
      bcl2fastq_param=self.format_tool_options(bcl2fastq_options)               # format bcl2fastq params
      bcl2fastq_cmd.extend(bcl2fastq_param)                                     # add additional parameters
      subprocess.check_call(bcl2fastq_cmd)                                      # run bcl2fastq
      
      copytree(output_temp_dir,output_fastq_dir)                                # copy output from TMPDIR
      copy2(samplesheet_file,os.path.join(output_fastq_dir,\
                                          samplesheet_filename))                # add samplesheet to output dir
      self.param('dataflow_params',{'fastq_dir':output_fastq_dir})              # set dataflow params
      message='seqrun: {0}, project: {1}, lane: {2}, index: {3}, fastq: {4}'.\
              format(seqrun_igf_id,project_name,flowcell_id,\
                     index_length,output_fastq_dir)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.comment_asana_task(task_name=seqrun_igf_id, comment=message)         # send log to asana
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise