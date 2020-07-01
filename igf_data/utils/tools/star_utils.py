import os,subprocess,re,fnmatch
from shlex import quote
from igf_data.utils.fastq_utils import detect_non_fastq_in_file_list,identify_fastq_pair
from igf_data.utils.fileutils import check_file_path,get_temp_dir,remove_dir,copy_local_file

class Star_utils:
  '''
  A wrapper python class for running STAR alignment

  :param star_exe: STAR executable path
  :param input_files: List of input files for running alignment
  :param genome_dir: STAR reference transcriptome path
  :param reference_gtf: Reference GTF file for gene annotation
  :param output_dir: Path for output alignment and results
  :param output_prefix: File output prefix
  :param threads: No. of threads for STAR run, default 1
  :param use_ephemeral_space: A toggle for temp dir settings, default 0
  '''
  def __init__(self,star_exe,input_files,genome_dir,reference_gtf,
               output_dir,output_prefix,threads=1,use_ephemeral_space=0):
    self.star_exe = star_exe
    self.input_files = input_files
    self.genome_dir = genome_dir
    self.reference_gtf = reference_gtf
    self.output_dir = output_dir
    self.output_prefix = output_prefix
    self.threads = threads
    self.use_ephemeral_space = use_ephemeral_space

  def _run_checks(self):
    '''
    An internal method for running initial checks before star run
    '''
    try:
      check_file_path(self.star_exe)                                            # checking star exe
      if not isinstance(self.input_files, list) or \
         len(self.input_files)==0:
        raise ValueError('No input file list found for star')

      for file in self.input_files:
        check_file_path(file_path=file)                                         # checking input file paths

      check_file_path(file_path=self.reference_gtf)                             # checking input gtf filepath
    except:
      raise

  def generate_aligned_bams(self,two_pass_mode=True,dry_run=False,
                            star_patameters=(
                              "--outFilterMultimapNmax",20,
                              "--alignSJoverhangMin",8,
                              "--alignSJDBoverhangMin",1,
                              "--outFilterMismatchNmax",999,
                              "--outFilterMismatchNoverReadLmax",0.04,
                              "--alignIntronMin",20,
                              "--alignIntronMax",1000000,
                              "--alignMatesGapMax",1000000,
                              "--limitBAMsortRAM",12000000000)):
    '''
    A method running star alignment
    
    :param two_pass_mode: Run two-pass mode of star, default True
    :param dry_run: A toggle forreturning the star cmd without actual run, default False
    :param star_patameters: A dictionary of star parameters, default encode parameters
    :returns: A genomic_bam and a transcriptomic bam,log file, gene count file and star commandline
    '''
    try:
      temp_dir = \
        get_temp_dir(use_ephemeral_space=self.use_ephemeral_space)              # get a temp dir
      self._run_checks()
      temp_path_prefix = \
        '{0}/{1}'.format(
          temp_dir,
          self.output_prefix)
      default_star_align_params = {
        "--runThreadN":quote(str(self.threads)),
        "--genomeDir":quote(self.genome_dir),
        "--genomeLoad":"NoSharedMemory",
        "--runMode":"alignReads",
        "--quantMode":["TranscriptomeSAM","GeneCounts"],
        "--outSAMtype":["BAM","SortedByCoordinate"],
        "--outFilterType":"BySJout",
        "--outSAMunmapped":"Within",
        "--sjdbGTFfile":quote(self.reference_gtf),
        "--outFileNamePrefix":quote(temp_path_prefix),
        "--outSAMattributes":["NH","HI","AS","NM","MD"]}
      star_cmd = [self.star_exe]
      for key,val in default_star_align_params.items():
        for field in [key,val]:
          if isinstance(field,list):
            star_cmd.extend(field)
          else:
            star_cmd.append(field)

      if isinstance(star_patameters,tuple) and \
         len(star_patameters)>0:
        star_patameters = \
          {item:star_patameters[index+1]
             for index, item in enumerate(star_patameters)
               if index %2==0}                                                  # convert default star param tuple to dict

      if not isinstance(star_patameters, dict):
        raise TypeError('Expecting a dictionary for star run parameters and got {0}'.\
                        format(type(star_patameters)))

      if len(star_patameters)>0:
        unique_keys = \
          set(star_patameters.keys()).\
            difference(set(default_star_align_params.keys()))                   # filter default params from star cmdline
        param_list = [
          quote(str(field))
            for key,val in star_patameters.items()
              if key in unique_keys
                for field in [key,val]
                  if field is not None and field != '']                         # flatten param dictionary
        star_cmd.extend(param_list)                                             # add params to command line

      if two_pass_mode:
        star_cmd.extend(["--twopassMode","Basic"])                              # enable two-pass more for star

      if detect_non_fastq_in_file_list(self.input_files):
        raise ValueError('Expecting only fastq files as input')

      zipped_pattern = re.compile(r'\S+\.gz')
      read1_list,read2_list = \
        identify_fastq_pair(input_list=self.input_files)                        # fetch input fastq files
      if re.match(zipped_pattern,os.path.basename(read1_list[0])):
        star_cmd.extend(["--readFilesCommand","zcat"])                          # command for gzipped reads

      star_cmd.extend(["--readFilesIn",quote(read1_list[0])])                   # add read 1
      if len(read2_list)>0:
        star_cmd.append(quote(read2_list[0]))                                   # add read 2

      if dry_run:
        return star_cmd                                                         # return star cmd

      subprocess.check_call(' '.join(star_cmd),shell=True)
      genomic_bam = ''
      transcriptomic_bam = ''
      star_log_file = ''
      star_gene_count_file = ''
      genomic_bam_pattern = re.compile(r'\S+sortedByCoord\.out\.bam$')          # pattern for genomic bam
      transcriptomic_bam_pattern = re.compile(r'\S+toTranscriptome\.out\.bam$') # pattern for transcriptomic bam
      star_log_pattern = re.compile(r'\S+Log\.final\.out$')                     # pattern for star final log
      star_count_pattern = re.compile(r'\S+ReadsPerGene.out\.tab$')             # pattern for star gene count file
      for file in os.listdir(temp_dir):
        if fnmatch.fnmatch(file, '*.bam'):
          source_path = os.path.join(temp_dir,file)
          destinationa_path = os.path.join(self.output_dir,file)
          copy_local_file(
            source_path=source_path,
            destinationa_path=destinationa_path)                                # copying bams to output dir
          if re.match(genomic_bam_pattern,file):
            genomic_bam=destinationa_path

          if re.match(transcriptomic_bam_pattern,file):
            transcriptomic_bam = destinationa_path

        if re.match(star_log_pattern,file):
          star_log_file = os.path.join(self.output_dir,file)
          copy_local_file(
            source_path=os.path.join(temp_dir,file),
            destinationa_path=star_log_file)                                    # copy star log file

        if re.match(star_count_pattern,file):
          star_gene_count_file = os.path.join(self.output_dir,file)
          copy_local_file(
            source_path=os.path.join(temp_dir,file),
            destinationa_path=star_gene_count_file)                             # copy star gene count file

      if genomic_bam == '' or \
         transcriptomic_bam == '' or \
         star_log_file=='' or \
         star_gene_count_file=='':
        raise ValueError('Star output bam files, log file or gene count file not found')

      remove_dir(temp_dir)                                                      # removing temp run dir
      return genomic_bam,transcriptomic_bam,star_log_file,star_gene_count_file,star_cmd
    except:
      if os.path.exists(temp_dir):
        remove_dir(temp_dir)
      raise

  def generate_rna_bigwig(self,bedGraphToBigWig_path,chrom_length_file,bedsort_path,
                          stranded=True,dry_run=False):
    '''
    A method for generating bigWig signal tracks from star aligned bams files
    
    :param bedGraphToBigWig_path: bedGraphToBigWig executable path
    :param chrom_length_file: A file containing chromosome length, e.g. .fai file
    :param bedsort_path: bedSort executable path
    :param stranded:Param for stranded analysis, default True
    :param dry_run: A toggle forreturning the star cmd without actual run, default False
    :returns: A list of bigWig files and star commandline
    '''
    try:
      temp_dir = \
        get_temp_dir(use_ephemeral_space=self.use_ephemeral_space)              # get a temp dir
      self._run_checks()
      check_file_path(bedGraphToBigWig_path)
      check_file_path(chrom_length_file)
      temp_path_prefix = \
        '{0}/{1}'.format(
          temp_dir,
          self.output_prefix)
      default_star_signal_params = {
        "--runThreadN":quote(str(self.threads)),
        "--genomeLoad":"NoSharedMemory",
        "--runMode":"inputAlignmentsFromBAM",
        "--outWigType":"bedGraph",
        "--outFileNamePrefix":quote(temp_path_prefix),}
      star_cmd = [self.star_exe]
      for key,val in default_star_signal_params.items():
        for field in [key,val]:
          if isinstance(field,list):
            star_cmd.extend(field)
          else:
            star_cmd.append(field)

      if stranded:
        star_cmd.extend(["--outWigStrand","Stranded"])                          # stranded rnaseq

      bam_pattern = re.compile(r'\S+\.bam$')
      input_files = self.input_files
      if not re.match(bam_pattern,input_files[0]):
        raise ValueError('Input bam file not found in input file list star run: {0}'.\
                         format(input_files))

      star_cmd.extend(["--inputBAMfile",quote(input_files[0])])                 # set input for star run
      if dry_run:
        return star_cmd                                                         # return star cmd

      subprocess.check_call(' '.join(star_cmd),shell=True)
      output_list = list()
      for file in os.listdir(temp_dir):
        if not fnmatch.fnmatch(file,'*.UniqueMultiple.*') and \
           file.endswith('.bg'):
          output_sorted_path = \
            os.path.join(
              temp_dir,
              file.replace('.bg','sorted.bg'))
          output_path = \
            os.path.join(
              temp_dir,
              file.replace('.bg','.bw'))
          bedsort_cmd = [
            bedsort_path,
            os.path.join(temp_dir,file),
            output_sorted_path
          ]
          subprocess.check_call(bedsort_cmd)
          bw_cmd = [
            bedGraphToBigWig_path,
            output_sorted_path,
            chrom_length_file,
            output_path,]
          subprocess.check_call(bw_cmd)
          output_list.append(output_path)

      if len(output_list)==0:
        raise ValueError('No bigwig file found from star run')

      for file in output_list:
        if not os.path.exists(file):
          raise IOError('Bigwig output {0} not found'.format(file))

      return output_list,star_cmd
    except:
      if os.path.exists(temp_dir):
        remove_dir(temp_dir)
      raise