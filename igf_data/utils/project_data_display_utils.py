import gviz_api
import pandas as pd
from igf_data.utils.seqrunutils import get_seqrun_date_from_igf_id

def _count_total_reads(data,seqrun_list):
  '''
  An internal function for counting total reads
  
  required params:
  data: A dictionary containing seqrun ids a key an read counts as values
  seqrun_list: A list of sequencing runs
  '''
  try:
    if len(seqrun_list) >1:
      if 'total_read' not in data:
        data['total_read']=0
        for run in seqrun_list:
          data['total_read'] += data[run]
    return data
  except:
    raise


def convert_project_data_gviz_data(input_data,output_file):
  '''
  A utility method for converting project's data availability information to
  gviz data table format
  https://developers.google.com/chart/interactive/docs/reference#DataTable
  
  required params:
  input_data: A pandas data frame, it should contain following columns
              sample_igf_id, 
              seqrun_igf_id, 
              attribute_value (R1_READ_COUNT)
  output_file: A filepath for writing gviz json data
  '''
  try:
    if not isinstance(input_data, pd.DataFrame):
      raise AttributeError('Expecting a pandas dataframe and got {0}'.\
                           format(type(input_data)))

    processed_data=input_data.\
                   pivot_table(values='attribute_value',
                               index=['sample_igf_id',
                                      'seqrun_igf_id'],
                               aggfunc='sum')                                   # group data by sample id and seq runs
    processed_data.\
    reset_index(['sample_igf_id',
                 'seqrun_igf_id' ],
                inplace=True)                                                   # reset index for processed data
    intermediate_data=list()                                                    # define empty intermediate data structure
    seqrun_set=set()                                                            # define empty seqrun set
    for line in processed_data.to_dict(orient='region'):                        # reformat processed data to required structure
      tmp_data=dict()
      tmp_data.update({'sample_igf_id':line['sample_igf_id'],
                        line['seqrun_igf_id']:line['attribute_value']})
      seqrun_set.add(line['seqrun_igf_id'])
      intermediate_data.append(tmp_data)

    intermediate_data=pd.DataFrame(intermediate_data)                           # convert intermediate data to dataframe
    intermediate_data.fillna(0,inplace=True)                                    # replace NAN values with zero
    intermediate_data=intermediate_data.\
                      pivot_table(index='sample_igf_id',
                                  aggfunc='sum').\
                      reset_index('sample_igf_id')                              # group data by samples id
    intermediate_data=intermediate_data.\
                      apply(lambda line: \
                            _count_total_reads(data=line,
                                               seqrun_list=list(seqrun_set)),
                            axis=1)                                             # count total reads for multiple seq runs
    intermediate_data.fillna(0,inplace=True)                                    # fail safe for missing samples
    
    
    description = {"sample_igf_id": ("string", "Sample ID")}                    # define description
    if len(list(seqrun_set)) >1:
        description.update({"total_read":("number", "Total Reads")})            # add total read column for samples with multiple runs
        intermediate_data['total_read']=intermediate_data['total_read'].\
                                        astype(float)                           # convert column to number

    for run in list(seqrun_set):
        description.update({run:("number",run)})                                # add seqrun columns
        intermediate_data[run]=intermediate_data[run].\
                               astype(float)                                    # convert column to number

    intermediate_data=intermediate_data.to_dict(orient='region')                # convert data frame to json
    data_table = gviz_api.DataTable(description)                                # load description to gviz api
    data_table.LoadData(intermediate_data)                                      # load data to gviz_api
    column_list=['sample_igf_id']                                               # define column order
    column_list.extend(list(seqrun_set))
    if len(list(seqrun_set)) >1:
        column_list.append('total_read')                                        # total read is present only for multiple runs

    final_data=data_table.ToJSon(columns_order=tuple(column_list))              # create final data structure
    with open(output_file,'w') as jf:
        jf.write(final_data)                                                    # write final data to output file
  except:
    raise

def _modify_seqrun_data(data_series):
  '''
  An internal method for parsing seqrun dataframe and adding remote dir path
  required columns: seqrun_igf_id, flowcell_id
  output column: flowcell_id, path
  '''
  try:
    if not isinstance(data_series,pd.Series):
      raise AttributeError('Expecting a pandas data series and got {0}'.\
                           format(type(data_series)))

    seqrun_igf_id=data_series['seqrun_igf_id']
    flowcell_id=data_series['flowcell_id']
    seqrun_date=get_seqrun_date_from_igf_id(seqrun_igf_id)
    data_series['path']=os.path.join(seqrun_date,flowcell_id)
    del data_series['seqrun_igf_id']
    return data_series
  except:
    raise

def add_seqrun_path_info(input_data,output_file):
  '''
  A utility method for adding remote path to a dataframe for each sequencing runs
  of a project
  
  required params:
  input_data: A input dataframe containing the following columns
              seqrun_igf_id
              flowcell_id
  output_file: An output filepath for the json data
  '''
  try:
    if not isinstance(input_data,pd.DataFrame):
      raise AttributeError('Expecting a pandas dataframe and got {0}'.\
                           format(type(input_data)))

    input_data.drop_duplicates(inplace=True)                                    # remove duplicate entries
    input_data=input_data.\
               apply(lambda line: \
                     _modify_seqrun_data(data_series=line),
                     axis=1)                                                    # add remote seqrun path
    input_data=input_data.to_json(orient='records')                             # encode output json
    with open(output_file,'w') as j_data:
      j_data.write(input_data)                                                  # write output json file
  except:
    raise