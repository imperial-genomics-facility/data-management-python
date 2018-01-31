import os, subprocess,fnmatch
from shutil import copy2
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor

class RunFastqscreen(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(RunFastqscreen,self).param_defaults()
    params_dict.update({
      'force_overwrite':True,
      'tag':None,
      'sample_name':None,
      'lane_index_info':None,
      'fastqscreen_dir_label':'fastqscreen',
      'fastqscreen_exe':'fastqscreen',
      'fastqs_collection_type':'FASTQSCREEN',
      'hpc_location':'HPC_PROJECT',
      'store_file':False,
      'fastqscreen_conf':None,
      'fastqscreen_options':{'--aligner':'bowtie2', \
                             '--force':'', \
                             '--quiet':'', \
                             '--subset':'100000', \
                             '--threads':'1'},
      })
    return params_dict
  
  def run(self):
    try:
      fastq_file=self.param_required('fastq_file')
      fastq_dir=self.param_required('fastq_dir')
      igf_session_class=self.param_required('igf_session_class')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      base_results_dir=self.param_required('base_results_dir')
      project_name=self.param_required('project_name')
      seqrun_date=self.param_required('seqrun_date')
      flowcell_id=self.param_required('flowcell_id')
      fastqscreen_exe=self.param_required('fastqscreen_exe')
      fastqscreen_conf=self.param_required('fastqscreen_conf')
      tag=self.param_required('tag')
      lane_index_info=self.param_required('lane_index_info')
      sample_name=self.param('sample_name')
      fastqscreen_options=self.param('fastqscreen_options')
      force_overwrite=self.param('force_overwrite')
      fastqscreen_dir_label=self.param('fastqscreen_dir_label')
      fastqs_collection_type=self.param('fastqs_collection_type')
      hpc_location=self.param('hpc_location')
      store_file=self.param('store_file')
      
      if lane_index_info is None:
        lane_index_info=os.path.basename(fastq_dir)                             # get the lane and index length info
        
      fastq_file_label=os.path.basename(fastq_file).replace('.fastq.gz','')
      
      if tag=='known' and store_file:                                  # fetch sample name for known fastq, if its not defined
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
        
      fastqscreen_result_dir=os.path.join(base_results_dir, \
                                          project_name, \
                                          seqrun_date, \
                                          flowcell_id, \
                                          lane_index_info,\
                                          tag)                                  # result dir path is generic
      
      if sample_name is not None:
        fastqscreen_result_dir=os.path.join(fastqscreen_result_dir, \
                                            sample_name)                        # add sample name to dir path only if its available
      
      fastqscreen_result_dir=os.path.join(fastqscreen_result_dir, \
                                          fastq_file_label, \
                                          fastqscreen_dir_label)                # keep multiple files under same dir
      
      if os.path.exists(fastqscreen_result_dir) and force_overwrite:
        remove_dir(fastqscreen_result_dir)                                      # remove existing output dir if force_overwrite is true
      
      if not os.path.exists(fastqscreen_result_dir):
        os.makedirs(fastqscreen_result_dir,mode=0o775)                          # create output dir if its not present
      
      temp_work_dir=get_temp_dir()                                              # get a temp work dir
      if not os.path.exists(fastq_file):
        raise IOError('fastq file {0} not readable'.format(fastq_file))         # raise if fastq file path is not readable
      
      fastqscreen_output=os.path.join(temp_work_dir,fastq_file_label)
      os.mkdir(fastqscreen_output)                                              # create fastqc output dir
      
      fastqscreen_param=self.format_tool_options(fastqscreen_options)           # format fastqc params
      fastqscreen_cmd=[fastqscreen_exe,
                       '-conf',fastqscreen_conf,
                       '--outdir',fastqscreen_output,
                      ]                                                         # fastqscreen base parameters
      fastqscreen_cmd.extend(fastqscreen_param)                                 # add additional parameters
      fastqscreen_cmd.append(fastq_file)                                        # fastqscreen input file
      subprocess.check_call(fastqscreen_cmd)                                    # run fastqscreen
      
      fastqscreen_stat=None
      fastqscreen_html=None
      fastqscreen_png=None
      
      for root,dirs,files in os.walk(top=fastqscreen_output):
        for file in files:
          if fnmatch.fnmatch(file, '*.txt'):
            input_fastqs_txt=os.path.join(root,file)
            copy2(input_fastqs_txt,fastqscreen_result_dir)
            fastqscreen_stat=os.path.join(fastqscreen_result_dir,file)
            
          if fnmatch.fnmatch(file, '*.html'):
            input_fastqs_html=os.path.join(root,file)
            copy2(input_fastqs_html,fastqscreen_result_dir)
            fastqscreen_html=os.path.join(fastqscreen_result_dir,file)
          
          if fnmatch.fnmatch(file, '*.png'):
            input_fastqs_png=os.path.join(root,file)
            copy2(input_fastqs_png,fastqscreen_result_dir)
            fastqscreen_png=os.path.join(fastqscreen_result_dir,file)
            
      if fastqscreen_stat is None or fastqscreen_html is None or \
         fastqscreen_png is None:
        raise ValueError('Missing required file, stat: {0}, html: {1}, png: {2}'.\
                         format(fastqscreen_stat, \
                                fastqscreen_html, \
                                fastqscreen_png))
      
      if tag=='known' and store_file:
        fastqs_files=[{'name':collection_name,\
                       'type':fastqs_collection_type,\
                       'table':required_collection_table,\
                       'file_path':fastqscreen_stat,\
                       'location':hpc_location},
                      {'name':collection_name,\
                       'type':fastqs_collection_type,\
                       'table':required_collection_table,\
                       'file_path':fastqscreen_html,\
                       'location':hpc_location},
                      {'name':collection_name,\
                       'type':fastqs_collection_type,\
                       'table':required_collection_table,\
                       'file_path':fastqscreen_png,\
                       'location':hpc_location},
                     ]
        ca=CollectionAdaptor(**{'session_class':igf_session_class})
        ca.start_session()
        ca.load_file_and_create_collection(data=fastqs_files)                   # store fastqs files to db
        ca.close_session()
        
      self.param('dataflow_params',{'fastqscreen_html':fastqscreen_html, \
                                    'lane_index_info':lane_index_info,\
                                    'sample_name':sample_name,\
                                    'fastqscreen': \
                                    {'fastq_dir':fastq_dir,
                                     'fastqscreen_stat':fastqscreen_stat,
                                     'fastqscreen_html':fastqscreen_html,
                                    }})                                         # set dataflow params
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise