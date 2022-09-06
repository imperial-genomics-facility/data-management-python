import os
import pandas as pd
from igf_data.utils.seqrunutils import get_seqrun_date_from_igf_id


def _count_total_reads(data,seqrun_list):
  '''
  An internal function for counting total reads
  
  required params:
  :param data, A dictionary containing seqrun ids a key an read counts as values
  :param seqrun_list, A list of sequencing runs
  '''
  try:
    data['run_count'] = 0
    if 'total_read' not in data:
      data['total_read']=0

    if len(seqrun_list) >1:
      for run in seqrun_list:
        if data[run] > 0:
          data['run_count'] += 1
          data['total_read'] += data[run]
    #if data['run_count'] == 1:
    #  data['total_read'] = 0
    return data
  except:
    raise


def convert_project_data_gviz_data(input_data,
                                   sample_col='sample_igf_id',
                                   read_count_col='attribute_value',
                                   seqrun_col='flowcell_id'):
  '''
  A utility method for converting project's data availability information to
  gviz data table format
  https://developers.google.com/chart/interactive/docs/reference#DataTable
  
  required params:
  :param input_data: A pandas data frame, it should contain following columns
              sample_igf_id, 
              flowcell_id, 
              attribute_value (R1_READ_COUNT)
  :param sample_col, Column name for sample id, default sample_igf_id
  :param seqrun_col, Column name for sequencing run identifier, default flowcell_id
  :param read_count_col, Column name for sample read counts, default attribute_value

  return 
    a dictionary of description
    a list of data dictionary
    a tuple of column_order
  
  '''
  try:
    if not isinstance(input_data, pd.DataFrame):
      raise AttributeError('Expecting a pandas dataframe and got {0}'.\
                           format(type(input_data)))
    input_data[read_count_col]=input_data[read_count_col].astype(float)         # convert reac counts to int
    processed_data=input_data.\
                   pivot_table(values=read_count_col,
                               index=[sample_col,
                                      seqrun_col],
                               aggfunc='sum')                                   # group data by sample id and seq runs
    processed_data.\
    reset_index([sample_col,
                 seqrun_col],
                inplace=True)                                                   # reset index for processed data
    intermediate_data=list()                                                    # define empty intermediate data structure
    seqrun_set=set()                                                            # define empty seqrun set
    for line in processed_data.to_dict(orient='records'):                       # reformat processed data to required structure
      tmp_data=dict()
      tmp_data.update({sample_col:line[sample_col],
                        line[seqrun_col]:line[read_count_col]})
      seqrun_set.add(line[seqrun_col])
      intermediate_data.append(tmp_data)

    intermediate_data=pd.DataFrame(intermediate_data)                           # convert intermediate data to dataframe
    intermediate_data.fillna(0,inplace=True)                                    # replace NAN values with zero
    intermediate_data=intermediate_data.\
                      pivot_table(index=sample_col,
                                  aggfunc='sum').\
                      reset_index(sample_col)                                   # group data by samples id
    intermediate_data=intermediate_data.\
                      apply(lambda line: \
                            _count_total_reads(data=line,
                                               seqrun_list=list(seqrun_set)),
                            axis=1)                                             # count total reads for multiple seq runs
    
    multiple_run_data=intermediate_data[intermediate_data['run_count'] > 1]     # check for multi run projects
    if len(multiple_run_data.index)==0 and \
       'total_read' in multiple_run_data.columns:
      intermediate_data.drop('total_read',axis=1,inplace=True)                  # drop the total read column if all samples are single run

    if 'run_count'  in intermediate_data.columns:
      intermediate_data.drop('run_count',axis=1,inplace=True)                   # removing run_count column

    intermediate_data.fillna(0,inplace=True)                                    # fail safe for missing samples
    description = {sample_col: ("string", "Sample ID")}                         # define description
    if len(list(seqrun_set)) >1 and \
       'total_read' in intermediate_data.columns:
      description.update({"total_read":("number", "Total Reads")})              # add total read column for samples with multiple runs
      intermediate_data['total_read']=intermediate_data['total_read'].\
                                      astype(float)                             # convert column to number

    for run in list(seqrun_set):
      description.update({run:("number",run)})                                  # add seqrun columns
      intermediate_data[run]=intermediate_data[run].\
                             astype(float)                                      # convert column to number


    column_list=[sample_col]                                                    # define column order
    column_list.extend(list(seqrun_set))
    if len(list(seqrun_set)) > 1 and \
       'total_read' in intermediate_data.columns:
      column_list.append('total_read')                                          # total read is present only for multiple runs

    intermediate_data=intermediate_data.to_dict(orient='records')               # convert data frame to json
    column_order=tuple(column_list)
    return description,intermediate_data,column_order
  except:
    raise

def _modify_seqrun_data(data_series,seqrun_col,flowcell_col,path_col):
  '''
  An internal method for parsing seqrun dataframe and adding remote dir path
  required columns: seqrun_igf_id, flowcell_id
  :param seqrun_col, Column name for sequencing run id, default seqrun_igf_id
  :param flowcell_col, Column namae for flowcell id, default flowcell_id
  :param path_col, Column name for path, default path
  returns a data series with following columns: flowcell_id, path
  '''
  try:
    if not isinstance(data_series,pd.Series):
      raise AttributeError('Expecting a pandas data series and got {0}'.\
                           format(type(data_series)))

    seqrun_igf_id=data_series[seqrun_col]
    flowcell_id=data_series[flowcell_col]
    seqrun_date=get_seqrun_date_from_igf_id(seqrun_igf_id)
    # data_series[path_col]=os.path.join(seqrun_date,flowcell_id)                 # adding path to data series
    data_series[path_col] = flowcell_id
    data_series[flowcell_col] = f'{flowcell_id} - {seqrun_date}'
    del data_series[seqrun_col]
    return data_series
  except:
    raise

def add_seqrun_path_info(input_data,output_file,seqrun_col='seqrun_igf_id',
                         flowcell_col='flowcell_id',path_col='path'):
  '''
  A utility method for adding remote path to a dataframe for each sequencing runs
  of a project
  
  required params:
  :param input_data, A input dataframe containing the following columns
          seqrun_igf_id
          flowcell_id
  :param seqrun_col, Column name for sequencing run id, default seqrun_igf_id
  :param flowcell_col, Column namae for flowcell id, default flowcell_id
  :param path_col, Column name for path, default path
  output_file: An output filepath for the json data
  '''
  try:
    if not isinstance(input_data,pd.DataFrame):
      raise AttributeError('Expecting a pandas dataframe and got {0}'.\
                           format(type(input_data)))

    input_data.drop_duplicates(inplace=True)                                    # remove duplicate entries
    input_data=input_data.\
               apply(lambda line: \
                     _modify_seqrun_data(data_series=line,
                                         seqrun_col=seqrun_col,
                                         flowcell_col=flowcell_col,
                                         path_col=path_col),
                     axis=1)                                                    # add remote seqrun path
    input_data=input_data.to_json(orient='records')                             # encode output json
    with open(output_file,'w') as j_data:
      j_data.write(input_data)                                                  # write output json file
  except:
    raise