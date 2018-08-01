import matplotlib
matplotlib.use('Agg')
import os
import matplotlib.pyplot as plt
from igf_data.utils.tools.scanpy_utils import Scanpy_tool
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import move_file,get_temp_dir,remove_dir

class RunScanpy(IGFBaseProcess):
  '''
  A ehive process class for running scanpy analysis
  '''
  def param_defaults(self):
    params_dict=super(RunSamtools,self).param_defaults()
    params_dict.update({
        'output_report':'',
        'report_template_file':'',
        'cellranger_collection_type':'CELLRANGER_RESULTS',
        'species_name_lookup':{'HG38':'hsapiens',
                               'MM10':'mmusculus'},
      })
    return params_dict

  def run(self):
    '''
    A method for running samtools commands
    
    :param project_igf_id: A project igf id
    :param sample_igf_id: A sample igf id
    :param experiment_igf_id: A experiment igf id
    :param igf_session_class: A database session class
    :param species_name: species_name
    :param base_work_dir: Base work directory
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      igf_session_class=self.param_required('igf_session_class')
      species_name=self.param_required('species_name')
      report_template_file=self.param_required('report_template_file')
      output_report=self.param('output_report')
      species_name_lookup=self.param('species_name_lookup')
      cellranger_collection_type=self.param('cellranger_collection_type')

      if species_name in species_name_lookup.keys():                            # check for human or mice
        ensembl_species_name=species_name_lookup[species_name]                  # get ensembl species name
        output_report=''
        # fetch cellranger tar path from db
        # extract filtered metrics files from tar
        input_path=''                                                           # set input path
        output_report=''                                                        # set output report path
        matrix_file=os.path.join(input_path,'matrix.mtx')
        gene_file=os.path.join(input_path,'genes.tsv')
        barcode_file=os.path.join(input_path,'barcodes.tsv')

        sp=Scanpy_tool(
             project_name=project_igf_id,
             sample_name=sample_igf_id,
             matrix_file=matrix_file,
             gene_tsv=gene_file,
             barcode_tsv=barcode_file,
             html_template_file=report_template_file,
             species_name=ensembl_species_name,
             output_file=output_report
            )
        sp.generate_report()                                                    # generate scanpy report
        # load files to db and disk

      self.param('dataflow_params',{'output_report':output_report})             # pass on output report filepath
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise