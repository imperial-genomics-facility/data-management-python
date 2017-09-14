#!/usr/bin/env python
import os, subprocess
from shutil import copytree
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.process.moveBclFilesForDemultiplexing import moveBclFilesForDemultiplexing


class RunBcl2Fastq(IGFBaseProcess):
  '''
  A process class for running tool bcl2fastq
  '''
  def param_defaults(self):
    params_dict=IGFBaseProcess.param_defaults()
    params_dict.update({
        'runinfo_filename':'RunInfo.xml',
        'fastq_dir_label':'fastq',
        'bcl2fastq_module':'bcl2fastq/2.18',
        'bcl2fastq_exe':'bcl2fastq',
        'bcl2fastq_options':{'-r':'1','-w':'1','-p':'1','--barcode-mismatches':'1', '--auto-set-to-zero-barcode-mismatches':''},
      })
    return params_dict
  
  def run(self):
    try:
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      seqrun_local_dir=self.param_required('seqrun_local_dir')
      bases_mask=self.param_required('bases_mask')
      base_work_dir=self.param_required('base_work_dir')
      samplesheet_file=self.param_required('samplesheet')
      runinfo_filename=self.param('runinfo_filename')
      bcl2fastq_module=self.param('bcl2fastq_module')
      bcl2fastq_exe=self.param('bcl2fastq_exe')
      bcl2fastq_options=self.param('bcl2fastq_options')
      fastq_dir_label=self.param('fastq_dir_label')
      seqrun_dir=os.path.join(seqrun_local_dir,seqrun_igf_id)
      runinfo_file=os.path.join(seqrun_dir,runinfo_filename)
      if not os.path.exists(samplesheet_file):
        raise IOError('samplesheet file {0} not found'.format(samplesheet_file))
      
      if not os.path.exists(runinfo_file):
        raise IOError('Runinfo file {0} not found'.format(runinfo_file))
      
      job_name=self.job_name()
      output_dir=os.path.join(base_work_dir,seqrun_igf_id,job_name)
      if not os.path.exists(output_dir):
        os.mkdir(output_dir)                                                    # create output dir if its not present
      
      output_dir=os.path.join(output_dir,fastq_dir_label)                       # reset output dir
      temp_dir=get_temp_dir()                                                   # create a new directory in TMPDIR
      move_file=moveBclFilesForDemultiplexing(input_dir=seqrun_dir,
                                              output_dir=temp_dir,
                                              samplesheet=samplesheet_file,
                                              run_info_xml=runinfo_file)        # get lists of files to move to TMPDIR
      move_file.copy_bcl_files()                                                # move files to TMPDIR
      
      output_temp_dir=os,path.join(temp_dir,fastq_dir_label)                    # get output temp dir
      os.mkdir(output_temp_dir)                                                 # create output temp dir
      subprocess.check_call(['module','load',bcl2fastq_module])                 # load module for bcl2fastq
      bcl2fastq_param=[[param,value] if value else [param] 
                       for param, value in bcl2fastq_options.items()]           # remove empty values
      bcl2fastq_param=[col for row in param for col in bcl2fastq_param]         # flatten sub lists
      bcl2fastq_cmd=[bcl2fastq_exe,
                     '--runfolder-dir',seqrun_dir,
                     '--sample-sheet',samplesheet_file,
                     '--output-dir',output_temp_dir,
                     ]                                                          # bcl2fastq base parameters
      bcl2fastq_cmd.extend(bcl2fastq_param)                                     # add additional parameters
      subprocess.check_call(bcl2fastq_cmd)                                      # run bcl2fastq
      
      if os.path.exists(output_dir):
        remove_dir(output_dir)                                                  # remove fastq directory if its already present
      
      copytree(output_temp_dir,output_dir)                                      # copy output from TMPDIR
      self.param('dataflow_params',{'fastq_dir':output_dir})                    # set dataflow params
    except:
      raise