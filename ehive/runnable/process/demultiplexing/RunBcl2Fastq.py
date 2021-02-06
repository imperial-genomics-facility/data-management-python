#!/usr/bin/env python
import os, subprocess,re
from shutil import copytree,copy2,move
from shlex import quote
from igf_data.utils.fileutils import get_temp_dir,remove_dir,copy_local_file,check_file_path
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.process.moveBclFilesForDemultiplexing import moveBclFilesForDemultiplexing


class RunBcl2Fastq(IGFBaseProcess):
  '''
  A process class for running tool bcl2fastq
  '''
  def param_defaults(self):
    params_dict=super(RunBcl2Fastq,self).param_defaults()
    params_dict.update({
        'runinfo_filename':'RunInfo.xml',
        'samplesheet_filename':'SampleSheet.csv',
        'fastq_dir_label':'fastq',
        'force_overwrite':True,
        'bcl2fastq_exe':None,
        'model_name':None,
        'bcl2fastq_options':'{"-r" : "1","-w" : "1","-p" : "2","--barcode-mismatches" : "1","--auto-set-to-zero-barcode-mismatches":"","--create-fastq-for-index-reads":""}',
        'singlecell_options':'{"--minimum-trimmed-read-length=8":"","--mask-short-adapter-reads=8":""}',
        'singlecell_tag':'10X',
        'reset_mask_short_adapter_reads':False,
        'use_ephemeral_space':0,
      })
    return params_dict

  def run(self):
    try:
      seqrun_igf_id = self.param_required('seqrun_igf_id')
      seqrun_date = self.param_required('seqrun_date')
      flowcell_id = self.param_required('flowcell_id')
      flowcell_lane = self.param_required('flowcell_lane')
      project_name = self.param_required('project_name')
      index_length = self.param_required('index_length')
      seqrun_local_dir = self.param_required('seqrun_local_dir')
      bases_mask = self.param_required('basesmask')
      base_work_dir = self.param_required('base_work_dir')
      base_fastq_dir = self.param_required('base_fastq_dir')
      samplesheet_file = self.param_required('samplesheet')
      bcl2fastq_exe = self.param_required('bcl2fastq_exe')
      runinfo_filename = self.param('runinfo_filename')
      bcl2fastq_options = self.param('bcl2fastq_options')
      singlecell_options = self.param_required('singlecell_options')
      singlecell_tag = self.param('singlecell_tag')
      force_overwrite = self.param('force_overwrite')
      fastq_dir_label = self.param('fastq_dir_label')
      samplesheet_filename = self.param('samplesheet_filename')
      use_ephemeral_space = self.param('use_ephemeral_space')
      model_name = self.param('model_name')
      reset_mask_short_adapter_reads = self.param('reset_mask_short_adapter_reads')

      project_type = ''                                                         # default single cell status is empty
      seqrun_dir = os.path.join(seqrun_local_dir,seqrun_igf_id)                 # local seqrun dir
      runinfo_file = os.path.join(seqrun_dir,runinfo_filename)                  # seqrun runinfo file
      if not os.path.exists(samplesheet_file):
        raise IOError('samplesheet file {0} not found'.\
                      format(samplesheet_file))


      samplesheet_sc = SampleSheet(infile=samplesheet_file)                     # read samplesheet for single cell check
      samplesheet_sc.\
        filter_sample_data(\
          condition_key='Description', 
          condition_value=singlecell_tag, 
          method='include')
      if len(samplesheet_sc._data) > 0:
        project_type = singlecell_tag                                           # set single cell status as true if its present in samplesheet

      if not os.path.exists(runinfo_file):
        raise IOError('Runinfo file {0} not found'.\
                      format(runinfo_file))

      lane_index = '{0}_{1}'.format(flowcell_lane,index_length)                 # get label for lane and index length
      output_dir_label = \
        os.path.join(
          project_name,
          fastq_dir_label,
          seqrun_date,
          flowcell_id,
          lane_index)                                                           # output dir label
      output_fastq_dir = \
        os.path.join(base_fastq_dir,output_dir_label)                           # output fastq dir

      if os.path.exists(output_fastq_dir) and force_overwrite:
        remove_dir(output_fastq_dir)                                            # remove fastq directory if its already present

      message = \
        'started fastq conversion for {0}, {1} : {2}_{3}'.\
          format(
            seqrun_igf_id,
            project_name,
            flowcell_lane,
            index_length)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.post_message_to_ms_team(
          message=message,
          reaction='pass')
      seqrun_temp_dir = \
        get_temp_dir(use_ephemeral_space=use_ephemeral_space)                   # create a new input directory in TMPDIR
      move_file = \
        moveBclFilesForDemultiplexing(\
          input_dir=seqrun_dir,
          output_dir=seqrun_temp_dir,
          samplesheet=samplesheet_file,
          run_info_xml=runinfo_file,
          platform_model=model_name)                                            # get lists of files to move to TMPDIR
      move_file.copy_bcl_files()                                                # move files to TMPDIR
      job_name = self.job_name()
      output_temp_dir = \
        get_temp_dir(use_ephemeral_space=use_ephemeral_space)                   # create tmp directory in TMPDIR for cluster
      report_dir = \
        os.path.join(\
          base_work_dir,
          seqrun_igf_id,
          job_name,
          'Reports')                                                            # creating report directory in main storage
      if not os.path.exists(report_dir):
        os.makedirs(report_dir,mode=0o770)

      stats_dir = \
        os.path.join(\
          base_work_dir,
          seqrun_igf_id,
          job_name,
          'Stats')                                                              # create stats directory in main storage
      if not os.path.exists(stats_dir):
        os.makedirs(stats_dir,mode=0o770)

      bcl2fastq_cmd = \
        [quote(bcl2fastq_exe),
         '--runfolder-dir',quote(seqrun_temp_dir),
         '--sample-sheet',quote(samplesheet_file),
         '--output-dir',quote(output_temp_dir),
         '--reports-dir',quote(report_dir),
         '--use-bases-mask',quote(bases_mask),
         '--stats-dir',quote(stats_dir)]                                        # bcl2fastq base parameters

      bcl2fastq_param = \
        self.format_tool_options(bcl2fastq_options)                             # format bcl2fastq params
      bcl2fastq_cmd.extend(bcl2fastq_param)                                     # add additional parameters
      if reset_mask_short_adapter_reads and \
         '--mask-short-adapter-reads' not in bcl2fastq_options:
        read_pattern = re.compile(r'^y(\d+)n?\d?')
        read_values = [int(re.match(read_pattern,i).group(1))
                         for i in bases_mask.split(',')
                           if i.startswith('y') and re.match(read_pattern,i)
                             if int(re.match(read_pattern,i).group(1)) < 22 ]   # hack for checking if reads are lower than the Illumina threasholds
        if len(read_values) > 0 and \
            min(read_values) > 5:
          bcl2fastq_cmd.\
            append("--mask-short-adapter-reads={0}".\
                   format(quote(str(min(read_values)))))
          message = \
            'Setting masked bases length for {0},{1}:{2}_{3}, value: {4}'.\
              format(
                seqrun_igf_id,
                project_name,
                flowcell_lane,
                index_length,
                min(read_values))
          self.post_message_to_slack(message,reaction='pass')                   # send log to slack
          self.comment_asana_task(\
            task_name=seqrun_igf_id,
            comment=message)                                                    # send log to asana

      if project_type==singlecell_tag:
        sc_bcl2fastq_param = self.format_tool_options(singlecell_options)       # format singlecell bcl2fastq params
        bcl2fastq_cmd.extend(sc_bcl2fastq_param)                                # add additional parameters

      message = ' '.join(bcl2fastq_cmd)
      self.post_message_to_slack(message,reaction='pass')                       # send bcl2fastq command to Slack
      self.comment_asana_task(task_name=seqrun_igf_id, comment=message)         # send bcl2fastq command to Asana
      self.post_message_to_ms_team(
          message=message,
          reaction='pass')
      subprocess.check_call(' '.join(bcl2fastq_cmd),shell=True)                 # run bcl2fastq
      if os.path.exists(output_fastq_dir):
        remove_dir(output_fastq_dir)
      copy_local_file(output_temp_dir,output_fastq_dir)                                # copy output from TMPDIR
      copy_local_file(\
        samplesheet_file,
        os.path.join(\
          output_fastq_dir,
          samplesheet_filename))                                                # add samplesheet to output dir
      copy_local_file(
        report_dir,
        os.path.join(output_fastq_dir,"Reports"))                                        # move report directory to project dir
      check_file_path(
        os.path.join(
          output_fastq_dir,
          'Reports',
          'html',
          flowcell_id,
          'all',
          'all',
          'all',
          'laneBarcode.html'
        )
      )
      copy_local_file(
        stats_dir,
        os.path.join(output_fastq_dir,'Stats'))                                          # move stats directory to project dir
      check_file_path(
        os.path.join(
          output_fastq_dir,
          'Stats',
          'Stats.json'
        )
      )
      self.param('dataflow_params',
                 {'fastq_dir':output_fastq_dir,
                  'bcl2fq_project_type':project_type})                          # set dataflow params
      message = \
        'Fastq conversion done for {0},{1}:{2}_{3}, fastq: {4}'.\
          format(
            seqrun_igf_id,
            project_name,
            flowcell_lane,
            index_length,
            output_fastq_dir)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.comment_asana_task(\
        task_name=seqrun_igf_id,
        comment=message)                                                        # send log to asana
      self.post_message_to_ms_team(
          message=message,
          reaction='pass')
      remove_dir(seqrun_temp_dir)
      remove_dir(output_temp_dir)                                               # remove temp dirs
    except Exception as e:
      message = \
        'seqrun: {2}, Error in {0}: {1}'.\
          format(
            self.__class__.__name__,
            e,
            seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      self.post_message_to_ms_team(
          message=message,
          reaction='fail')
      raise