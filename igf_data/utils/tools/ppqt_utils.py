import os,subprocess
from shlex import quote
import pandas as pd
from igf_data.utils.fileutils import check_file_path, get_temp_dir,copy_local_file,remove_dir

class Ppqt_tools:
  '''
  A class for running Phantom quality control tools (PPQT)
  '''
  def __init__(self,rscript_path,ppqt_exe,thread=1):
    self.rscript_path = rscript_path
    self.ppqt_exe = ppqt_exe
    self.thread = thread

  def run_ppqt(self,input_bam,output_dir,output_spp_name,output_pdf_name,ppqt_stdout_name):
    '''
    A method for running PPQT on input bam

    :param input_bam: Input bam file
    :param output_spp_name: Output spp out file
    :param output_pdf_name: Output pdf plot
    :param output_dir: Destination output dir
    :param ppqt_stdout_name: PPQT stdout filename
    '''
    try:
      temp_dir=get_temp_dir(use_ephemeral_space=False)
      run_cmd = \
        self._pre_process(\
          input_bam=input_bam,
          output_spp_name=output_spp_name,
          output_pdf_name=output_pdf_name,
          output_dir=temp_dir,
          temp_dir=temp_dir)                                                    # preprocess and fetch run cmd
      tmp_stdout = os.path.join(temp_dir,ppqt_stdout_name)

      with open(tmp_stdout,'w') as fp:
        with subprocess.Popen(run_cmd, stdout=subprocess.PIPE) as proc:
          fp.write(proc.stdout.read().decode('utf-8'))                          # run ppqt and capture stdout

      self._post_process(\
        output_spp_name=output_spp_name,
        output_pdf_name=output_pdf_name,
        ppqt_stdout_name=ppqt_stdout_name,
        output_dir=output_dir,
        temp_dir=temp_dir)                                                      # copy files from temp dir
      remove_dir(temp_dir)                                                      # clean up temp dir

    except:
      raise

  def _pre_process(self,input_bam,output_spp_name,output_pdf_name,output_dir,temp_dir):
    '''
    An internal method for preprocessing before the exe run

    :param input_bam: Input bam file
    :param output_spp_name: Output spp filename
    :param output_pdf_name: Output pdf filename
    :param output_dir: Destination output dir
    :param temp_dir: Source temp dir
    '''
    try:
      check_file_path(self.rscript_path)
      check_file_path(self.ppqt_exe)
      if not os.path.exists(output_dir):
        os.makedirs(output_dir,mode=0o770)

      run_cmd = \
        [quote(self.rscript_path),
         quote(self.ppqt_exe),
         quote('-c={0}'.format(input_bam)),
         quote('-rf'),
         quote('-p={0}'.format(self.thread)),
         quote('-savep={0}'.format(output_pdf)),
         quote('-out={0}'.format(output_spp)),
         quote('-tmpdir={0}'.format(temp_dir)),
         quote('-odir={0}'.format(output_dir))]
      return run_cmd
    except:
      raise

  @staticmethod
  def _post_process(output_spp_name,output_pdf_name,ppqt_stdout_name,
                    output_dir,temp_dir):
    '''
    A static method for post processing ppqt analysis

    :param output_spp_name: Output spp filename
    :param output_pdf_name: Output pdf filename
    :param ppqt_stdout_name: PPQT stdout filename
    :param output_dir: Destination output dir
    :param temp_dir: Source temp dir
    :returns: None
    '''
    try:
      tmp_spp_file = os.path.join(temp_dir,output_spp_name)
      dest_spp_file = os.path.join(output_dir,output_spp_name)
      tmp_pdf_file = os.path.join(temp_dir,output_pdf_name)
      dest_pdf_file = os.path.join(output_dir,output_pdf_name)
      tmp_ppqt_stdout = os.path.join(temp_dir,ppqt_stdout_name)
      dest_ppqt_stdout = os.path.join(output_dir,ppqt_stdout_name)
      check_file_path(tmp_spp_file)
      check_file_path(tmp_pdf_file)
      check_file_path(tmp_ppqt_stdout)
      copy_local_file(\
        source_path=tmp_spp_file,
        destinationa_path=dest_spp_file,
        force=True)
      copy_local_file(\
        source_path=tmp_pdf_file,
        destinationa_path=dest_pdf_file,
        force=True)
      copy_local_file(\
        source_path=tmp_ppqt_stdout,
        destinationa_path=dest_ppqt_stdout,
        force=True)
    except:
      raise
