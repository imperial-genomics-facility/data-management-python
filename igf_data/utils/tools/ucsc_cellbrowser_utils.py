import os,subprocess
from shutil import copytree
from shlex import quote
from igf_data.utils.fileutils import get_temp_dir,remove_dir,check_file_path

def convert_scanpy_h5ad_to_cellbrowser_dir(cbImportScanpy_path,h5ad_path,project_name,
                                           cellbrowser_htmldir,use_ephemeral_space=0):
  '''
  A wrapper function for Scanpy h5ad file to UCSC cellbrowser html dir conversion

  :param cbImportScanpy_path: Path for cbImportScanpy executable
  :param h5ad_path: Path of input Scanpy h5ad file
  :param project_name: Project name for cellbrowser
  :param cellbrowser_htmldir: Output cellbrowser htmldir path
  :param use_ephemeral_space: A toggle for temp dir setting, default 0
  '''
  try:
    if os.path.exists(cellbrowser_htmldir):
      raise IOError('Cellbrowser output path already present')

    check_file_path(os.path.dirname(cellbrowser_htmldir))
    check_file_path(cbImportScanpy_path)
    check_file_path(h5ad_path)
    temp_dir = get_temp_dir(use_ephemeral_space=use_ephemeral_space)
    temp_cellbrowser_html = \
      os.path.join(\
        temp_dir,
        os.path.basename(cellbrowser_htmldir))
    temp_cellbrowser_dir = \
      os.path.join(\
        temp_dir,
        'out')
    cbImportScanpy_cmd = \
      [quote(cbImportScanpy_path),
       '-n',quote(project_name),
       '-i',quote(h5ad_path),
       '-o',temp_cellbrowser_dir,
       '--htmlDir',temp_cellbrowser_html
      ]
    subprocess.check_call(' '.join(cbImportScanpy_cmd),shell=True)
    copytree(\
      temp_cellbrowser_html,
      cellbrowser_htmldir)
  except:
    raise
