import os, subprocess,fnmatch
from shutil import copy2
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor


class RunFastqc(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    params_dict.update({
      'required_collection_table':'run',
      'force_overwrite':True,
      'fastqc_dir_label':'fastqc',
      'fastqc_exe':'fastqc',
      'fastqc_options':{'-q':'','--noextract':'','-f':'fastq','-k':'7','-t':'1'},
      'tag':None,
      'sample_name':None,
      'hpc_location':'HPC_PROJECT',
      'fastqc_collection_type':'FASTQC',
      })
    return params_dict
  
  def run(self):
    try:
      fastq_file=self.param_required('fastq_file')
      fastq_dir=self.param_required('fastq_dir')
      igf_session_class=self.param_required('igf_session_class')
      fastqc_exe=self.param_required('fastqc_exe')
      tag=self.param_required('tag')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      seqrun_date=self.param_required('seqrun_date')
      flowcell_id=self.param_required('flowcell_id')
      fastqc_options=self.param('fastqc_options')
      base_results_dir=self.param_required('base_results_dir')
      project_name=self.param_required('project_name')
      force_overwrite=self.param('force_overwrite')
      fastqc_dir_label=self.param('fastqc_dir_label')
      required_collection_table=self.param('required_collection_table')
      sample_name=self.param('sample_name')
      hpc_location=self.param('hpc_location')
      fastqc_collection_type=self.param('fastqc_collection_type')
      
      lane_index_info=os.path.basename(fastq_dir)                               # get the lane and index length info
      fastq_file_label=os.path.basename(fastq_file).replace('.fastq.gz','')
      
      if tag=='known':                                  # fetch sample name for known fastq, if its not defined
        base=BaseAdaptor(**{'session_class':igf_session_class})
        base.start_session()                                                    # connect to db
      
        ca=CollectionAdaptor(**{'session':base.session})
        (collection_name,collection_table)=\
        ca.fetch_collection_name_and_table_from_file_path(file_path=fastq_file) # fetch collection name and table info
        
        if collection_table != required_collection_table:
          raise ValueError('Expected collection table {0} and got {1}, {2}'.\
                           format(required_collection_table,collection_table,fastq_file))
          
        ra=RunAdaptor(**{'session':base.session})
        sample=ra.fetch_sample_info_for_run(run_igf_id=collection_name)
        sample_name=sample['sample_igf_id']
        base.close_session()
        
      fastqc_result_dir=os.path.join(base_results_dir, \
                                     project_name, \
                                     seqrun_date, \
                                     flowcell_id, \
                                     lane_index_info,\
                                     tag)                                       # result dir path is generic
      if sample_name is not None:
        fastqc_result_dir=os.path.join(fastqc_result_dir, \
                                       sample_name)                             # add sample name to dir path if its available
      
      fastqc_result_dir=os.path.join(fastqc_result_dir, \
                                     fastq_file_label,\
                                     fastqc_dir_label)                          # keep multiple files under same dir
      
      if os.path.exists(fastqc_result_dir) and force_overwrite:
        remove_dir(fastqc_result_dir)                                           # remove existing output dir if force_overwrite is true
        
      if not os.path.exists(fastqc_result_dir):
        os.makedirs(fastqc_result_dir,mode=0o775)                               # create output dir if its not present
        
      temp_work_dir=get_temp_dir()                                              # get a temp work dir
      if not os.path.exists(fastq_file):
        raise IOError('fastq file {0} not readable'.format(fastq_file))         # raise if fastq file path is not readable
      
      fastqc_output=os.path.join(temp_work_dir,fastq_file_label)
      os.mkdir(fastqc_output)                                                   # create fastqc output dir
      fastqc_param=self.format_tool_options(fastqc_options)                     # format fastqc params
      fastqc_cmd=[fastqc_exe, '-o',fastqc_output, '-d',temp_work_dir ]          # fastqc base parameters
      fastqc_cmd.extend(fastqc_param)                                           # add additional parameters
      fastqc_cmd.append(fastq_file)                                             # fastqc input file
      subprocess.check_call(fastqc_cmd)                                         # run fastqc
      
      fastqc_zip=None
      fastqc_html=None
      
      for root, dirs, files in os.walk(top=fastqc_output):
        for file in files:
          if fnmatch.fnmatch(file, '*.zip'):
            input_fastqc_zip=os.path.join(root,file)
            copy2(input_fastqc_zip,fastqc_result_dir)
            fastqc_zip=os.path.join(fastqc_result_dir,file)
          
          if fnmatch.fnmatch(file, '*.html'):
            input_fastqc_html=os.path.join(root,file)
            copy2(input_fastqc_html,fastqc_result_dir)
            fastqc_html=os.path.join(fastqc_result_dir,file)
          
      if fastqc_html is None or fastqc_zip is None:
        raise ValueError('Missing required values, fastqc zip: {0}, fastqc html: {1}'.\
                         format(fastqc_zip,fastqc_html))
      
      if tag=='known':
        fastqc_files=[{'name':collection_name,\
                       'type':fastqc_collection_type,\
                       'table':required_collection_table,\
                       'file_path':fastqc_zip,\
                       'location':hpc_location},
                      {'name':collection_name,\
                       'type':fastqc_collection_type,\
                       'table':required_collection_table,\
                       'file_path':fastqc_html,\
                       'location':hpc_location},
                     ]
        ca=CollectionAdaptor(**{'session_class':igf_session_class})
        ca.start_session()
        ca.load_file_and_create_collection(data=fastqc_files)                   # store fastqc files to db
        ca.close_session()
      
      self.param('dataflow_params',{'fastqc_html':fastqc_html, \
                                    'lane_index_info':lane_index_info,\
                                    'sample_name':sample_name,\
                                    'fastqc':{'fastqc_path':fastqc_result_dir,
                                              'fastqc_zip':fastqc_zip,
                                              'fastqc_html':fastqc_html}})      # set dataflow params
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise