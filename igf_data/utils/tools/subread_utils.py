import os,re,subprocess
from shlex import quote
from igf_data.utils.fileutils import check_file_path,get_temp_dir,remove_dir,copy_local_file

def run_featureCounts(featurecounts_exe,input_gtf,input_bams,output_file,thread=1,
                      use_ephemeral_space=0,options=None):
  '''
  A wrapper method for running featureCounts tool from subread package
  
  :param featurecounts_exe: Path of featureCounts executable
  :param input_gtf: Input gtf file path
  :param input_bams: input bam files
  :param output_file: Output filepath
  :param thread: Thread counts, default is 1
  :param options: FeaturCcount options, default in None
  :param use_ephemeral_space: A toggle for temp dir settings, default 0
  :returns: A summary file path and featureCounts command
  '''
  try:
    temp_dir = get_temp_dir(use_ephemeral_space=use_ephemeral_space)
    check_file_path(featurecounts_exe)
    check_file_path(input_gtf)
    if not isinstance(input_bams,list) or \
       len(input_bams)==0:
      raise ValueError('No input bam found for featureCounts run')

    for bam in input_bams:
      check_file_path(bam)

    temp_output = \
      os.path.join(
        temp_dir,
        os.path.basename(output_file))
    temp_summary = '{0}.summary'.format(temp_output)
    summary_file = '{0}.summary'.format(output_file)
    featureCount_cmd = [
      quote(featurecounts_exe),
      '-a',quote(input_gtf),
      '-o',quote(temp_output),
      '-T',quote(str(thread))]
    if options is not None and \
       isinstance(options,dict) and \
       len(options.keys()>0):
      options = [
        quote(opt)
          for key,val in options.items()
            for opt in [key,val]
              if opt !='' ]
      featureCount_cmd.extend(options)

    input_bams = [
      quote(bam)
        for bam in input_bams]
    featureCount_cmd.extend(input_bams)
    subprocess.check_call(' '.join(featureCount_cmd),shell=True)
    
    if not os.path.exists(temp_output) or \
       not os.path.exists(temp_summary):
      raise ValueError('Missing temp output files for featureCounts run')

    copy_local_file(
      source_path=temp_output,
      destinationa_path=output_file,
      force=True)
    copy_local_file(
      source_path=temp_summary,
      destinationa_path=summary_file,
      force=True)
    remove_dir(temp_dir)
    return summary_file,featureCount_cmd
  except:
    if temp_dir and \
       os.path.exists(temp_dir):
      remove_dir(temp_dir)
    raise