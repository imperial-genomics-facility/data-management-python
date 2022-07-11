import json, os
import subprocess
from collections import defaultdict
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import remove_dir
from tempfile import TemporaryFile

def get_pbspro_job_count(job_name_prefix=''):
  '''
  A function for fetching running and queued job information from a PBSPro HPC cluster

  :param job_name_prefix: A text to filter running jobs, default ''
  :returns: A defaultdict object with the following structure
            { job_name: {'Q': counts, 'R': counts }}
  '''
  try:
    # with TemporaryFile() as tmp_file:
    #   subprocess.\
    #   check_call(
    #       'qstat -t -f -F json|grep -v BASH_FUNC_module',                       # this can fix or break pipeline as well
    #       shell=True,
    #       stdout=tmp_file)
    #   tmp_file.seek(0)
    #   json_data = tmp_file.read()
    #   json_data = json.loads(json_data)
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    temp_queue_file = \
      os.path.join(temp_dir, 'queue_data.json')
    with open(temp_queue_file, 'w') as tmp_file:
      subprocess.\
        check_call(
            'qstat -t -f -F json|grep -v BASH_FUNC_module',                       # this can fix or break pipeline as well
            shell=True,
            stdout=tmp_file)
    ## close the tempfile and then read the json data
    with open(temp_queue_file, 'r') as fp:
      json_data = json.load(fp)
    jobs = json_data.get('Jobs')
    active_jobs = dict()
    remove_dir(temp_dir)
    if jobs is not None:
      active_jobs = defaultdict(lambda: defaultdict(int))
      if len(jobs) > 0:
        for _,job_data in jobs.items():
          job_name = job_data.get('Job_Name')
          job_state = job_data.get('job_state')
          if job_name.startswith(job_name_prefix):
            if job_state == 'Q':
              active_jobs[job_name]['Q'] += 1
            if job_state == 'R':
              active_jobs[job_name]['R'] += 1
    return active_jobs
  except Exception as e:
    raise ValueError('Failed to get job counts from hpc, error: {0}'.format(e))