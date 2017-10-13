#!/usr/bin/env python
import os,tarfile,fnmatch
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.igf_irods_client import IGF_irods_uploader

class UploadFastqToIrods(IGFBaseProcess):
  def run(self):
    pass