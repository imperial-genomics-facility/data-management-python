#!/usr/bin/env python
import os, subprocess
from tempfile import mkdtemp
from shutil import rmtree, move,copyfile,copy2

def move_file(source_path,destinationa_path, force=False):
    '''
    A method for moving files to local disk
    required params:
    source_path: A source file path
    destination_path: A destination file path
    force: Optional, set True to overwrite existing
           destination file, default is False
    '''
    try:
        if not os.path.exists(source_path):
            raise IOError('source file {0} not found'.format(source_path))
        if os.path.exists(destinationa_path) and not force:
            raise IOError('destination file {0} already present. set option "force" as True to overwrite it'.format(destinationa_path))
        move(, dst, copy_function)
    except:
        raise