import gviz_api
import pandas as pd

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
    intermediate_data=intermediate_data.to_dict(orient='region')                # convert data frame to json
    description = {"sample_igf_id": ("string", "Sample ID")}                    # define description
    if len(list(seqrun_set)) >1:
        description.update({"total_read":("number", "Total Reads")})            # add total read column for samples with multiple runs

    for run in list(seqrun_set):
        description.update({run:("number",run)})                                # add seqrun columns

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