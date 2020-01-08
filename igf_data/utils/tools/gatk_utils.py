import os,subprocess
from shlex import quote
from igf_data.utils.fileutils import check_file_path,get_temp_dir,remove_dir,copy_local_file

class GATK_tools:
  '''
  A python class for running gatk tools
  
  :param gatk_exe: Gatk exe path
  :param java_param: Java parameter, default '-XX:ParallelGCThreads=1 -Xmx4g'
  :param ref_fasta: Input reference fasta filepath
  '''
  def __init__(self,gatk_exe,ref_fasta,java_param='-XX:ParallelGCThreads=1 -Xmx4g'):
    self.gatk_exe = gatk_exe
    self.java_param = java_param
    self.ref_fasta = ref_fasta

  def _run_gatk_checks(self):
    '''
    An internal method for running checks before GATK run
    '''
    try:
      check_file_path(self.gatk_exe)
      check_file_path(self.ref_fasta)
    except:
      raise

  def run_BaseRecalibrator(self,input_bam,output_table,known_snp_sites=None,
                           known_indel_sites=None,force=False,dry_run=False):
    '''
    A method for running GATK BaseRecalibrator
    
    :param input_bam: An input bam file
    :param output_table: An output table filepath for recalibration results
    :param known_snp_sites: Known snp sites (e.g. dbSNP vcf file), default None
    :param known_indel_sites: Known indel sites (e.g.Mills_and_1000G_gold_standard indels vcf), default None
    :param force: Overwrite output file, if force is True
    :param dry_run: Return GATK command, if its true, default False
    :returns: GATK commandline
    '''
    try:
      self._run_gatk_checks()                                                   # run initial checks
      check_file_path(input_bam)
      temp_dir = get_temp_dir()                                                 # get temp dir
      temp_output = \
        os.path.join(
          temp_dir,
          os.path.basename(output_table))
      gatk_cmd = [
        quotes(self.gatk_exe),
        "BaseRecalibrator",
        "-I",quotes(input_bam),
        "-O",quotes(temp_output),
        "--reference",quotes(self.ref_fasta),
        "--java-options",quotes(self.java_param)]
      if known_snp_sites is not None:
        check_file_path(known_snp_sites)
        gatk_cmd.\
          append(
            "--known-sites",
            quotes(known_snp_sites))

      if known_indel_sites:
        check_file_path(known_indel_sites)
        gatk_cmd.\
          append(
            "--known-sites",
            quotes(known_indel_sites))

      if dry_run:
        return gatk_cmd

      subprocess.check_call(gatk_cmd)                                           # run gatk cmd
      copy_local_file(
        source_path=temp_output,
        destinationa_path=output_table,
        force=force)                                                            # copy output file
      remove_dir(temp_dir)                                                      # remove temp dir
      return gatk_cmd
    except:
      raise

  def run_ApplyBQSR(self,bqsr_recal_file,input_bam,output_bam_path,force=False,
                    dry_run=False):
    '''
    A method for running GATK ApplyBQSR
    
    :param input_bam: An input bam file
    :param bqsr_recal_file: An bqsr table filepath
    :param output_bam_path: A bam output file
    :param force: Overwrite output file, if force is True
    :param dry_run: Return GATK command, if its true, default False
    :returns: GATK commandline
    '''
    try:
      self._run_gatk_checks()                                                   # run initial checks
      check_file_path(input_bam)
      check_file_path(bqsr_recal_file)
      temp_dir = get_temp_dir()                                                 # get temp dir
      temp_output = \
        os.path.join(
          temp_dir,
          os.path.basename(output_bam_path))
      gatk_cmd = [
        quotes(self.gatk_exe),
        "ApplyBQSR",
        "--emit_original_quals",
        "--bqsr-recal-file",quotes(bqsr_recal_file),
        "-I",quotes(input_bam),
        "-O",quotes(temp_output),
        "--java-options",quotes(self.java_param)]
      if dry_run:
        return gatk_cmd

      subprocess.check_call(gatk_cmd)
      copy_local_file(
        source_path=temp_output,
        destinationa_path=output_bam_path,
        force=force)
      remove_dir(temp_dir)
      return gatk_cmd
    except:
      raise

  def run_AnalyzeCovariates(self,before_report_file,after_report_file,output_pdf_path,
                            force=False,dry_run=False):
    '''
    A method for running GATK AnalyzeCovariates tool
    
    :param before_report_file: A file containing bqsr output before recalibration
    :param after_report_file: A file containing bqsr output after recalibration
    :param output_pdf_path: An output pdf filepath
    :param force: Overwrite output file, if force is True
    :param dry_run: Return GATK command, if its true, default False
    :returns: GATK commandline
    '''
    try:
      self._run_gatk_checks()                                                   # run initial checks
      check_file_path(before_report_file)
      check_file_path(after_report_file)
      temp_dir = get_temp_dir()                                                 # get temp dir
      temp_output = \
        os.path.join(
          temp_dir,
          os.path.basename(output_pdf_path))
      gatk_cmd = [
        quotes(self.gatk_exe),
        "AnalyzeCovariates",
        "--before-report-file",quotes(before_report_file),
        "--after-report-file",quotes(after_report_file),
        "--plots-report-file",quotes(temp_output),
        "--java-options",quotes(self.java_param)]
      if dry_run:
        return gatk_cmd

      subprocess.check_call(gatk_cmd)
      copy_local_file(source_path=temp_output,
                      destinationa_path=output_pdf_path,
                      force=force)
      remove_dir(temp_dir)
      return gatk_cmd
    except:
      raise

  def run_HaplotypeCaller(self,input_bam,output_vcf_path,dbsnp_vcf,emit_gvcf=True,
                          force=False,dry_run=False):
    '''
    A method for running GATK HaplotypeCaller
    
    :param input_bam: A input bam file
    :param output_vcf_path: A output vcf filepath
    :param dbsnp_vcf: A dbsnp vcf file
    :param emit_gvcf: A toggle for GVCF generation, default True
    :param force: Overwrite output file, if force is True
    :param dry_run: Return GATK command, if its true, default False
    :returns: GATK commandline
    '''
    try:
      self._run_gatk_checks()                                                   # run initial checks
      check_file_path(input_bam)
      check_file_path(dbsnp_vcf)
      temp_dir = get_temp_dir()                                                 # get temp dir
      temp_output = os.path.join(temp_dir,
                               os.path.basename(output_vcf_path))
      gatk_cmd = [
        quotes(self.gatk_exe),
        "HaplotypeCaller",
        "-I",quotes(input_bam),
        "-O",quotes(temp_output),
        "--reference",quotes(self.ref_fasta),
        "--dbsnp",quotes(dbsnp_vcf),
        "--java-options",quotes(self.java_param)]
      if emit_gvcf:
        gatk_cmd.append("--emit-ref-confidence","GVCF")

      if dry_run:
        return gatk_cmd

      subprocess.check_call(gatk_cmd)
      copy_local_file(
        source_path=temp_output,
        destinationa_path=output_vcf_path,
        force=force)
      remove_dir(temp_dir)
      return gatk_cmd
    except:
      raise




