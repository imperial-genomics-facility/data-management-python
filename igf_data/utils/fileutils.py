#!/usr/bin/env python
import os, subprocess
from tempfile import mkdtemp
from shutil import rmtree, move, copy2

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
        move(source_path, destinationa_path, copy_function=copy2)
    except:
        raise


def copy_local_file(source_path,destinationa_path, force=False):
    '''
    A method for copy files to local disk
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
        copy2(source_path, destinationa_path, follow_symlinks=True)
    except:
        raise


def copy_remote_file(source_path,destinationa_path, source_address=None, destination_address=None, copy_method='rsync'):
    '''
    A method for copy files from or to remote location
    required params:
    source_path: A source file path
    destination_path: A destination file path
    source_address: Address of the source server
    destination_address: Address of the destination server
    copy_method: A nethod for copy files, default is 'rsync'
    '''
    try:
        if source_address is None and destination_address is None:
            raise ValueError('required a remote source address or a destination address')
        
        if source_address is not None:
            source_path='{0}@{1}'.format(source_address,source_path)
        
        if destination_address is not None:
            destinationa_path='{0}@{1}'.format(destination_address,destinationa_path)
        
        if copy_method == 'rsync':
            cmd=['rsync','-e','ssh',source_path,destinationa_path]
        else:
            raise ValueError('copy method {0} is not supported'.format(copy_method))
        
        subprocess.check_call(cmd)
    except:
        raise


def get_temp_dir(work_dir, prefix='temp'):
    '''
    A function for creating temp directory
    required params:
    work_dir: A path for work directory
    prefix: A prefix for directory path, default 'temp'
    returns temp_dir
    '''
    try:
        if not os.path.isdir(work_dir):              # check if work directory is present
            raise IOError('work directory {0} is not present'.format(work_dir))
        temp_dir=mkdtemp(prefix=prefix,dir=work_dir)  # create a temp dir
        return temp_dir
    except:
        raise
    
    
def remove_dir(dir_path):
    '''
    A function for removing directory containing files
    required params:
    dir_path: A directory path
    '''
    try:
        if not os.path.isdir(dir_path):
            raise IOError('directory path {0} is not present'.format(dir_path))
        rmtree(dir_path)
    except:
        raise