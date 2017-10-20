import pandas as pd
import datetime
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory

class PipeseedFactory(IGFBaseJobFactory):
  '''
  Job factory class for pipeline seed
  '''
  def param_defaults(self):
    params_dict=super(IGFBaseJobFactory,self).param_defaults()
    params_dict.update({ 'seed_id_label':'seed_id',
                         'seqrun_id_label':'seqrun_id',
                         'seeded_label':'SEEDED',
                         'running_label':'RUNNING',
                         'seqrun_date_label':'seqrun_date',
                         'seqrun_igf_id_label':'seqrun_igf_id',
                         'seed_status_label':'status',
                       })
    return params_dict


  def run(self):
    try:
      igf_session_class = self.param_required('igf_session_class')              # set by base class
      pipeline_name = self.param_required('pipeline_name')
      seed_id = self.param_required('seed_id_label')
      seqrun_id = self.param_required('seqrun_id_label')
      seeded_label=self.param_required('seeded_label')
      running_label=self.param_required('running_label')
      seqrun_date_label=self.param_required('seqrun_date_label')
      seqrun_igf_id_label=self.param_required('seqrun_igf_id_label')
      seed_status_label=self.param_required('seed_status_label')

      pa = PipelineAdaptor(**{'session_class':igf_session_class})               # get db adaptor
      pa.start_session()                                                        # connect to db
      (pipeseeds_data, table_data) = \
              pa.fetch_pipeline_seed_with_table_data(pipeline_name)             # fetch requires entries as list of dictionaries from table for the seeded entries
      if isinstance(pipeseeds_data.to_dict(orient='records'), list)  \
                    and len(pipeseeds_data.to_dict(orient='records'))>0:
        if seed_id is None:
          raise ValueError('missing pipeline seed_id label')                    # check for seed id label
        
        pipeseeds_data[seed_id]=pipeseeds_data[seed_id].map(lambda x: int(x))   # convert pipeseed column type
        table_data[seqrun_id]=table_data[seqrun_id].map(lambda x: int(x))       # convert seqrun data column type
        merged_data=pd.merge(pipeseeds_data,table_data,how='inner',\
                             on=None,left_on=[seed_id],right_on=[seqrun_id],\
                             left_index=False,right_index=False)                # join dataframes
        merged_data[seqrun_date_label]=merged_data[seqrun_igf_id_label].\
                                   map(lambda x: \
                                   self._get_date_from_seqrun(seqrun_igf_id=x)) # get seqrun date from seqrun id
        seed_data=merged_data.applymap(lambda x: str(x)).\
                              to_dict(orient='records')                         # convert dataframe to string and add as list of dictionaries
        self.param('sub_tasks',seed_data)                                       # set sub_tasks param for the data flow
        pipeseeds_data[seed_status_label]=pipeseeds_data[seed_status_label].\
                                 map({seeded_label:running_label})              # update seed records in pipeseed table, changed status to RUNNING
        pa.update_pipeline_seed(data=pipeseeds_data.to_dict(orient='records'))  # set pipeline seeds as running
        
        message='Total {0} new job found for {1}'.\
                 format(len(seed_data),self.__class__.__name__)                 # format msg for slack
        self.post_message_to_slack(message,reaction='pass')                     # send update to slack
      else:
        message='{0}: no new job created'.format(self.__class__.__name__)       # format msg for failed jobs
        self.warning(message)
        self.post_message_to_slack(message,reaction='sleep')                    # post about failed job to slack

    except Exception as e:
      message='Error in {0}: {1}'.format(self.__class__.__name__, e)            # format slack msg
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # send msg to slack
      raise                                                                     # mark worker as failed
    
    finally:
      if pa:
        pa.close_session()                                                      # close db session if its open
    
    
  def _get_date_from_seqrun(self,seqrun_igf_id):
    '''
    An internal method for extracting sequencing run date from seqrun_igf_id
    '''
    seqrun_date=seqrun_igf_id.split('_')[0]                                     # set first part of the string, e.g., 171001_XXX_XXX-XXX
    seqrun_date=datetime.datetime.strptime(seqrun_date,'%y%m%d').date()         # convert it to date opbect
    return seqrun_date