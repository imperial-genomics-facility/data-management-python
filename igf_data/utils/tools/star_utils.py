import os,subprocess,re,fnmatch
from shlex import quote
from igf_data.utils.fastq_utils import detect_non_fastq_in_file_list,identify_fastq_pair
from igf_data.utils.fileutils import check_file_path,get_temp_dir,remove_dir,copy_local_file

class Star_utils:
  def __init__(self,star_exe,input_files,genome_dir,reference_gtf,
               output_dir,output_prefix,threads=1):
    self.star_exe=star_exe
    self.input_files=input_files
    self.genome_dir=genome_dir
    self.reference_gtf=reference_gtf
    self.output_dir=output_dir
    self.output_prefix=output_prefix
    self.threads=threads

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
                            star_patameters={
                              "--outFilterMultimapNmax":20,
                              "--alignSJoverhangMin":8,
                              "--alignSJDBoverhangMin":1,
                              "--outFilterMismatchNmax":999,
                              "--outFilterMismatchNoverReadLmax":0.04,
                              "--alignIntronMin":20,
                              "--alignIntronMax":1000000,
                              "--alignMatesGapMax":1000000,
                              "--limitBAMsortRAM":12000000000}):
    '''
    A method running star alignment
    
    :param two_pass_mode: Run two-pass mode of star, default True
    :param dry_run: A toggle forreturning the star cmd without actual run, default False
    :param star_patameters: A dictionary of star parameters, default encode parameters
    :returns: A genomic_bam and a transcriptomic bam and star commandline
    '''
    try:
      temp_dir=get_temp_dir(use_ephemeral_space=True)                           # get a temp dir
      self._run_checks()
      temp_path_prefix='{0}/{1}'.format(temp_dir,
                                        self.output_prefix)
      default_star_align_params=\
            {"--runThreadN":quote(str(self.threads)),
             "--genomeDir":quote(self.genome_dir),
             "--genomeLoad":"NoSharedMemory",
             "--runMode":"alignReads",
             "--quantMode":"TranscriptomeSAM",
             "--outSAMtype":["BAM","SortedByCoordinate"],
             "--outFilterType":"BySJout",
             "--outSAMunmapped":"Within",
             "--sjdbGTFfile":quote(self.reference_gtf),
             "--outFileNamePrefix":quote(temp_path_prefix),
             "--outSAMattributes":["NH","HI","AS","NM","MD"],
            }
      star_cmd=[self.star_exe]
      for key,val in default_star_align_params.items():
        for field in [key,val]:
          if isinstance(field,list):
            star_cmd.extend(field)
          else:
            star_cmd.append(field)

      if not isinstance(star_patameters, dict):
        raise TypeError('Expecting a dictionary for star run parameters and got {0}'.\
                        format(type(star_patameters)))

      if len(star_patameters)>0:
        unique_keys=set(star_patameters.keys()).\
                    difference(set(default_star_align_params.keys()))           # filter default params from star cmdline
        param_list=[quote(str(field))
                      for key,val in star_patameters.items()
                        if key in unique_keys
                          for field in [key,val]
                            if field is not None and field != ''
                   ]                                                            # flatten param dictionary
        star_cmd.extend(param_list)                                             # add params to command line

      if two_pass_mode:
        star_cmd.extend(["--twopassMode","Basic"])                              # enable two-pass more for star

      if detect_non_fastq_in_file_list(self.input_files):
        raise ValueError('Expecting only fastq files as input')

      zipped_pattern=re.compile(r'\S+\.gz')
      read1_list,read2_list=identify_fastq_pair(input_list=self.input_files)    # fetch input fastq files
      if re.match(zipped_pattern,os.path.basename(read1_list[0])):
        star_cmd.extend(["--readFilesCommand","zcat"])                          # command for gzipped reads

      star_cmd.extend(["--readFilesIn",quote(read1_list[0])])                   # add read 1
      if len(read2_list)>0:
        star_cmd.append(quote(read2_list[0]))                                   # add read 2

      if dry_run:
        return star_cmd                                                         # return star cmd

      subprocess.check_call(star_cmd,shell=False)
      genomic_bam=''
      transcriptomic_bam=''
      genomic_bam_pattern=re.compile(r'\S+sortedByCoord.out.bam$')              # pattern for genomic bam
      transcriptomic_bam_pattern=re.compile(r'\S+toTranscriptome.out.bam$')     # pattern for transcriptomic bam
      for file in os.listdir(temp_dir):
        if fnmatch.fnmatch(file, '*.bam'):
          source_path=os.path.join(temp_dir,file)
          destinationa_path=os.path.join(self.output_dir,file)
          copy_local_file(source_path=source_path,
                          destinationa_path=destinationa_path)                  # copying bams to output dir
          if re.match(genomic_bam_pattern,
                      os.path.basename(destinationa_path)):
            genomic_bam=destinationa_path

          if re.match(transcriptomic_bam_pattern,
                      os.path.basename(destinationa_path)):
            transcriptomic_bam=destinationa_path

      if genomic_bam == '' or \
         transcriptomic_bam == '':
        raise ValueError('Star output bam files not found')

      remove_dir(temp_dir)                                                      # removing temp run dir
      return genomic_bam,transcriptomic_bam,star_cmd
    except:
      if os.path.exists(temp_dir):
        remove_dir(temp_dir)
      raise

  def generate_rna_bigwig(self,bedGraphToBigWig_path,chrom_length_file,
                          stranded=True,dry_run=False):
    '''
    A method for generating bigWig signal tracks from star aligned bams files
    
    :param bedGraphToBigWig_path: bedGraphToBigWig_path executable path
    :param chrom_length_file: A file containing chromosome length, e.g. .fai file
    :param stranded:Param for stranded analysis, default True
    :param dry_run: A toggle forreturning the star cmd without actual run, default False
    :returns: A list of bigWig files and star commandline
    '''
    try:
      temp_dir=get_temp_dir(use_ephemeral_space=True)                           # get a temp dir
      self._run_checks()
      check_file_path(bedGraphToBigWig_path)
      check_file_path(chrom_length_file)
      temp_path_prefix='{0}/{1}'.format(temp_dir,
                                        self.output_prefix)
      default_star_signal_params=\
            {"--runThreadN":quote(str(self.threads)),
             "--genomeLoad":"NoSharedMemory",
             "--runMode":"inputAlignmentsFromBAM",
             "--outWigType":"bedGraph",
             "--outFileNamePrefix":quote(temp_path_prefix),
            }
      star_cmd=[self.star_exe]
      for key,val in default_star_signal_params.items():
        for field in [key,val]:
          if isinstance(field,list):
            star_cmd.extend(field)
          else:
            star_cmd.append(field)

      if stranded:
        star_cmd.extend(["--outWigStrand","Stranded"])                          # stranded rnaseq

      bam_pattern=re.compile(r'\S+\.bam$')
      input_files=self.input_files
      if not re.match(bam_pattern,input_files[0]):
        raise ValueError('Input bam file not found in input file list star run: {0}'.\
                         format(input_files))

      star_cmd.extend(["--inputBAMfile",quote(input_files[0])])                 # set input for star run
      if dry_run:
        return star_cmd                                                         # return star cmd

      subprocess.check_call(star_cmd)
      output_list=list()
      for file in os.listdir(temp_dir):
        if fnmatch.fnmatch(file,'*.bg$'):
          output_path=os.path.join(temp_dir,file.replace('.bg','.bw'))
          bw_cmd=[bedGraphToBigWig_path,
                  os.path.join(temp_dir,file),
                  chrom_length_file,
                  output_path,
                 ]
          subprocess.check_call(bw_cmd)
          output_list.append(output_path)

      if len(output_path)==0:
        raise ValueError('No bigwig file found from star run')

      for file in output_list:
        if not os.path.exists(file):
          raise IOError('Bigwig output {0} not found'.format(file))

      return output_path,star_cmd
    except:
      if os.path.exists(temp_dir):
        remove_dir(temp_dir)
      raise