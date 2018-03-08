import os,fnmatch,re,subprocess,shutil
from collections import defaultdict
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.fileutils import get_temp_dir,remove_dir

class MergeSingleCellFastq:
  '''
  A class for merging single cell fastq files per lane per sample

  required params:
  fastq_dir: A directory path containing fastq files
  samplesheet: A samplesheet file used demultiplexing of bcl files
  platform_name: A sequencing platform name
  singlecell_tag: A single cell keyword for description field, default '10X'
  sampleid_col: A keyword for sample id column of samplesheet, default 'Sample_ID'
  samplename_col: A keyword for sample name column of samplesheet, default 'Sample_Name'
  orig_sampleid_col: A keyword for original sample id column, default 'Original_Sample_ID'
  orig_samplename_col: A keyword for original sample name column, default 'Original_Sample_Name'
  description_col: A keyword for description column, default 'Description'
  project_col: A keyword for project column, default 'Sample_Project'
  pseudo_lane_col: A keyword for pseudo lane column, default 'PseudoLane'
  lane_col: A keyword for lane column, default 'Lane'

  SampleSheet file should contain following columns:
  Sample_ID: A single cell sample id in the following format, SampleId_{digit}
  Sample_Name: A single cell sample name in the following format, SampleName_{digit}
  Original_Sample_ID: An IGF sample id
  Original_Sample_Name: A sample name provided by user
  Description: A single cell label, default 10X
  '''
  def __init__(self, fastq_dir,samplesheet,platform_name,singlecell_tag='10X', 
               sampleid_col='Sample_ID', samplename_col='Sample_Name', 
               orig_sampleid_col='Original_Sample_ID', description_col='Description', 
               orig_samplename_col='Original_Sample_Name',project_col='Sample_Project',
               lane_col='Lane', pseudo_lane_col='PseudoLane'):
    self.fastq_dir=fastq_dir
    self.samplesheet=samplesheet
    self.platform_name=platform_name
    self.singlecell_tag=singlecell_tag
    self.sampleid_col=sampleid_col
    self.samplename_col=samplename_col
    self.orig_sampleid_col=orig_sampleid_col
    self.description_col=description_col
    self.orig_samplename_col=orig_samplename_col
    self.project_col=project_col
    self.lane_col=lane_col
    self.pseudo_lane_col=pseudo_lane_col

  def _fetch_lane_and_sample_info_from_samplesheet(self):
    '''
    A internal method for grouping samples per lane based on the samplesheet
    returns a list containing sample and lane information per row
    '''
    try:
      samplesheet_data=SampleSheet(infile=self.samplesheet)                     # read samplesheet file
      if (self.orig_sampleid_col not in samplesheet_data._data_header) or \
         (self.orig_samplename_col not in samplesheet_data._data_header):
        raise ValueError('Samplesheet {0} does not have {1} or {2} column'.\
                         format(self.samplesheet,
                                self.orig_sampleid_col,
                                self.orig_samplename_col))                      # check for required columns in the samplesheet
      samplesheet_data.\
      filter_sample_data(condition_key=self.description_col,
                         condition_value=self.singlecell_tag,
                         method='include')                                      # filter samplesheet for single cell data
      sample_lane_data=list()
      if platform_name=='NEXTSEQ':                                              # hack for nextseq
        samplesheet_data.add_pseudo_lane_for_nextseq()
        for group_tag,_ in pd.DataFrame(samplesheet_data._data).\
                              groupby([self.pseudo_lane_col,
                                       self.orig_sampleid_col,
                                       self.orig_samplename_col,
                                       self.project_col]):
          sample_data.append({'lane_id':group_tag[0],
                              'sample_id':group_tag[1],
                              'sample_name':group_tag[2],
                              'project_igf_id':group_tag[3]})
      elif platform_name=='MISEQ':                                              # hack for miseq
        samplesheet_data.add_pseudo_lane_for_miseq()
        for group_tag,_ in pd.DataFrame(samplesheet_data._data).\
                              groupby([self.pseudo_lane_col,
                                       self.orig_sampleid_col,
                                       self.orig_samplename_col,
                                       self.project_col]):
          sample_data.append({'lane_id':group_tag[0],
                              'sample_id':group_tag[1],
                              'sample_name':group_tag[2],
                              'project_igf_id':group_tag[3]})
      elif platform_name=='HISEQ4000':                                          # check for hiseq4k
        for group_tag,_ in pd.DataFrame(samplesheet_data._data).\
                              groupby([self.lane_col,
                                       self.orig_sampleid_col,
                                       self.orig_samplename_col,
                                       self.project_col]):
          sample_data.append({'lane_id':group_tag[0],
                              'sample_id':group_tag[1],
                              'sample_name':group_tag[2],
                              'project_igf_id':group_tag[3]})
      else:
        raise ValueError('platform {0} not supported'.format(platform_name))
      return sample_data
    except:
      raise

  @staticmethod
  def _group_singlecell_fastq(sample_data,fastq_dir):
    '''
    A static method for grouping single cell fastq files
    
    required params:
    sample_data: A list of sample entries from samplesheet
                It should contain following keys for each row:
                lane_id, sample_id, sample_name, project_id
    fastq_dir: A directory path containing fastq files
    
    returns two dictionary of fastq group, one for single cell samples and 
    another for sample information
    '''
    try:
      samples_info=defaultdict(dict)
      sample_files_list=defaultdict(lambda: \
                                    defaultdict(lambda: \
                                                defaultdict(lambda: \
                                                            defaultdict(list)))) # output data structure
      for sample_record in sample_data:
        sample_lane=sample_record['lane_id']
        sample_id=sample_record['sample_id']
        sample_name=sample_record['sample_name']
        project_id=sample_record['project_id']
        samples_info[sample_id]['sample_name']=sample_name
        samples_info[sample_id]['project_id']=project_id
        sample_id_regex=re.compile('^{0}_\d$'.format(sample_id))                # regexp for sample id match
        file_name_regex=re.compile('^{0}_(\d)_S\d+_L00{1}_([R,I][1,2])_\d+\.fastq(\.gz)?$'.\
                                  format(sample_name,sample_lane))              # regexp for fastq file match
        for root,dir_name,files in os.walk(fastq_dir):
          for file in files:
            if fnmatch.fnmatch(file, "*.fastq.gz") and \
               not fnmatch.fnmatch(file, "Undetermined_*"):                     # skip undetermined reads
              if re.search(sample_id_regex,os.path.basename(root)) and \
                 re.search(file_name_regex,file): 
                sm=re.match(file_name_regex,file)
                if len(sm.groups())>=2:
                  fragment_id=sm.group(1)
                  read_type=sm.group(2)
                  sample_files_list[sample_lane][sample_id][read_type][fragment_id].\
                  append(os.path.join(root,file))                               # add fastqs to samples list
                else:
                  raise ValueError('Failed to determined sample info:{0}, {1}'.\
                                   format(sample_id,file))
      return sample_files_list, samples_info
    except:
      raise


  def merge_fastq_per_lane_per_sample(self,output_dir):
    '''
    A method for merging single cell fastq files present in input fastq_dir
    per lane per sample basis
    
    required params:
    output_dir: A directory path for writing output fastq files
    '''
    try:
      sample_data=self._fetch_lane_and_sample_info_from_samplesheet()           # get sample and lane information from samplesheet
      sample_files, samples_info=self._group_singlecell_fastq(sample_data,\
                                                                    fastq_dir)  # get file groups
      s_count=0                                                                 # initial count for fastq S value
      for lane_id in sorted(sample_files.keys()):
        if self.platform_type=='NEXTSEQ':
            s_count=0                                                           # nextseq is weird, reset counter for each lane
        for sample_id in sorted(sample_files[lane_id].keys()):
          s_count+=1                                                            # assign new S value for fastq files
          sample_name=sample_info.get(sample_id)['sample_name']
          project_id=sample_info.get(sample_id)['project_id']                   # get sample and project info
          output_path=os.path.join(self.fastq_dir,
                                   project_id,
                                   sample_id)                                   # output location is under input fastq_dir
          if not os.path.exists(output_path):
            os.makedirs(output_path,mode=0o770)                                 # create outout directory

          for read_type in sample_files[lane_id][sample_id].keys():             # merge per read type
            output_filename='{0}_S{1}_L00{2}_{3}_001.fastq.gz'.\
                            format(sample_name,s_count,lane_id, read_type)      # assign new output filename
            final_path=os.path.join(output_path,output_filename)                # assign final output path
            input_list=list()
            for sc_fragment, file_path in sorted(sample_files[lane_id][sample_id][read_type].items()):
              input_list.extend(file_path)                                      # create list of input fastqs for merge
            temp_dir=get_temp_dir()                                             # get a temp dir
            temp_file=os.path.join(temp_dir,output_filename)                    # assign temp filename
            cmd=["cat"]+input_list+[">",temp_file]                              # shell command for merging fastq.gz files
            subprocess.check_call(" ".join(cmd),shell=True)                     # exact same command for fastq merge as 10x pipeline
            shutil.copy(temp_file,final_path)                                   # copy file to final location
            remove_dir(temp_dir)                                                # remove temp dir
            for file_path in input_list:
              os.remove(file_path)                                              # remove fastq chunks
    except:
      raise