import gviz_api

def convert_to_gviz_json_for_display(description,data,columns_order,output_file=None):
  '''
  A utility method for writing gviz format json file for data display using Google charts
  
  :param description, A dictionary for the data table description
  :param data, A dictionary containing the data table
  :column_order, A tuple of data table column order
  :param output_file, Output filename, default None
  :returns: None if output_file name is present, or else json_data string
  '''
  try:
    data_table = gviz_api.DataTable(description)                                # load description to gviz api
    data_table.LoadData(data)                                                   # load data to gviz_api
    final_data=data_table.ToJSon(columns_order=columns_order)                   # create final data structure
    if output_file is None:
      return final_data
    else:
      with open(output_file,'w') as jf:
        jf.write(final_data)                                                    # write final data to output file
      return None
  except:
    raise