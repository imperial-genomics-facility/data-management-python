import redis,json,logging

def fetch_queue_list_from_redis_server(url):
  """
  A function for fetching pending job counts from redis db

  :param url: A redis db connection URL
  :returns: A list of dictionaries with queue name as key and pending job counts as the value
  """
  try:
    queue_list = list()
    r = redis.from_url(url)
    for i in r.keys():
      queue = i.decode()
      if not queue.startswith('_') and \
         not queue.startswith('unacked'):
        q_len = r.llen(queue)
        queue_list.append({queue:q_len})
    return queue_list
  except Exception as e:
    raise ValueError(
            'Failed to fetch from redis server, error: {0}'.\
              format(e))


def calculate_new_workers(queue_list,active_jobs_dict,max_workers_per_queue=10,max_total_workers=70):
  '''
  A function for calculating new worker size

  :param queue_list: A list dictionary containing all the queued jobs
                      [{queue_name:job_count}]
  :param active_jobs_dict: A dictionary containing all job counts for each queues
                           {queue_name:{job_state:job_count}}
  :param max_workers_per_queue: Max allowed worker per queue, default 10
  :param max_total_workers: Max total worker for the queue, default 70
  :returns: A dictionary containing all the target jobs
            {queue_name:target_job_counts}
            and a list of unique queue names
            [queue_name]
  '''
  try:
    worker_to_submit = dict()
    unique_queue_list = list()
    total_active_jobs = 0
    for _,job_data in active_jobs_dict.items():
      for job_state,job_count in job_data.items():
        if job_state in ('Q','R'):
          total_active_jobs += job_count
    if isinstance(queue_list,list) and \
       len(queue_list) > 0 and \
       total_active_jobs < max_total_workers:
      for entry in queue_list:                                                  # this list should be unique
        for queue_name,waiting_jobs in entry.items():
          if waiting_jobs > max_workers_per_queue:
            waiting_jobs = max_workers_per_queue
          active_job = active_jobs_dict.get(queue_name)
          total_running_for_queue = 0
          active_queued_job = 0
          if active_job is not None:
            for job_state,job_counts in active_job.items():
              if job_state in ('Q','R'):
                total_running_for_queue += job_counts
              if job_state == 'Q':
                active_queued_job += job_counts
          if active_queued_job < 1:
            if total_running_for_queue==0 and \
               (total_active_jobs + waiting_jobs) < max_total_workers:
              worker_to_submit.\
                update({queue_name : waiting_jobs})
            if total_running_for_queue > 0:
              if waiting_jobs > total_running_for_queue:
                  waiting_jobs = waiting_jobs - total_running_for_queue
              if (total_active_jobs + waiting_jobs) < max_total_workers:
                worker_to_submit.\
                  update({queue_name : waiting_jobs})
                total_active_jobs += waiting_jobs
          else:
            logging.info('Not submitting new jobs for queue {0}'.\
                           format(queue_name))
    if len(worker_to_submit.keys()) > 0:
      unique_queue_list = list(worker_to_submit.keys())
    return worker_to_submit,unique_queue_list
  except Exception as e:
    raise ValueError(
            'Failed to calculate airflow worker size, error: {0}'.\
              format(e))