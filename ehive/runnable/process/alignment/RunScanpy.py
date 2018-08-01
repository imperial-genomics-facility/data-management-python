import matplotlib
matplotlib.use('Agg')
import os,re
import matplotlib.pyplot as plt
from igf_data.utils.tools.scanpy_utils import Scanpy_tool
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.igfdb.collectionadaptor import Collection_attribute
from igf_data.utils.fileutils import move_file,get_temp_dir,remove_dir
from igf_data.utils.analysis_collection_utils import Analysis_collection_utils

class RunScanpy(IGFBaseProcess):
  '''
  A ehive process class for running scanpy analysis
  '''
  def param_defaults(self):
    params_dict=super(RunSamtools,self).param_defaults()
    params_dict.update({
        'analysis_name':'scanpy',
        'collection_table':'experiment',
        'report_template_file':'',
        'cellranger_collection_type':'CELLRANGER_RESULTS',
        'scanpy_collection_type':'SCANPY_RESULTS',
        'species_name_lookup':{'HG38':'hsapiens',
                               'MM10':'mmusculus'},
      })
    return params_dict

  @staticmethod
  def _extract_cellranger_filtered_metrics(tar_file,output_dir,
                                           matrics_file='matrix.mtx',
                                           genes_tsv_file='genes.tsv',
                                           barcodes_tsv_file='barcodes.tsv'):
    '''
    A static internal method for extracting cellranger output files from tar
    
    :param tar_file: A tar.gz file containing cellranger count outputs
    :param output_dir: A output directory path
    :param matrics_file: File name for matrics output, default matrix.mtx
    :param genes_tsv_file: File name for genes list file, default genes.tsv
    :param barcodes_tsv_file: File name for barcodes list file, default barcodes.tsv
    :returns: matrics_file_path,genes_tsv_file_path,barcodes_tsv_file_path
    '''
    try:
      matrics_file_path=''
      genes_tsv_file_path=''
      barcodes_tsv_file_path=''
      matrix_file_pattern=re.compile(r'\S+/filtered_gene_bc_matrices/\S+/{0}'.\
                                     format(matrics_file))
      genes_file_pattern=re.compile(r'\S+/filtered_gene_bc_matrices/\S+/{0}'.\
                                    format(genes_tsv_file))
      barcodes_file_pattern=re.compile(r'\S+/filtered_gene_bc_matrices/\S+/{0}'.\
                                       format(barcodes_tsv_file))
      if not os.path.exists(tar_file):
        raise IOError('File {0} not found'.format(tar_file))

      tar = tarfile.open(tar_file,'r:gz')                                       # open tar file
      for tarinfo in tar:
        if re.match(metrix_file_pattern,tarinfo.name) or \
           re.match(genes_file_pattern,tarinfo.name) or \
           re.match(barcodes_file_pattern,tarinfo.name):
          tar.extract(tarinfo.name,
                      path=output_dir)                                          # extract target output files

      tar.close()                                                               # close tar file
      for root,dir,files in os.walk(output_dir):                                # check output dir and find files
        for file in files:
          if file==matrics_file:
            matrics_file_path=os.path.join(root,file)
          elif file==genes_tsv_file:
            genes_tsv_file_path=os.path.join(root,file)
          elif file==barcodes_tsv_file:
            barcodes_tsv_file_path=os.path.join(root,file)
  
      if matrics_file_path=='' or \
         genes_tsv_file_path=='' or \
         barcodes_tsv_file_path=='':
        raise ValueError('Required cellranger output not found in tar {0}'.\
                         format(tar_file))

      return matrics_file_path,genes_tsv_file_path,barcodes_tsv_file_path
    except:
      raise

  def run(self):
    '''
    A method for running samtools commands
    
    :param project_igf_id: A project igf id
    :param sample_igf_id: A sample igf id
    :param experiment_igf_id: A experiment igf id
    :param igf_session_class: A database session class
    :param species_name: species_name
    :param base_result_dir: Base results directory
    :param report_template_file: A template file for writing scanpy report
    :param analysis_name: Analysis name, default scanpy
    :param species_name_lookup: A dictionary for ensembl species name lookup
    :param cellranger_collection_type: Cellranger analysis collection type, default CELLRANGER_RESULTS
    :param scanpy_collection_type: Scanpy report collection type, default SCANPY_RESULTS
    :param collection_table: Collection table name for loading scanpy report, default experiment
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      igf_session_class=self.param_required('igf_session_class')
      species_name=self.param_required('species_name')
      report_template_file=self.param_required('report_template_file')
      analysis_name=self.param_required('analysis_name')
      base_result_dir=self.param_required('base_result_dir')
      species_name_lookup=self.param('species_name_lookup')
      cellranger_collection_type=self.param('cellranger_collection_type')
      scanpy_collection_type=self.param('scanpy_collection_type')
      collection_table=self.param('collection_table')

      output_report=''
      if species_name in species_name_lookup.keys():                            # check for human or mice
        ensembl_species_name=species_name_lookup[species_name]                  # get ensembl species name
        # fetch cellranger tar path from db
        ca=Collection_attribute(**{'session_class':igf_session_class})
        ca.start_session()                                                      # connect to database
        cellranger_tarfiles=ca.get_collection_files(\
                              collection_name=experiment_igf_id,
                              collection_type=cellranger_collection_type,
                              output_mode='dataframe')                          # fetch collection files
        ca.close_session()
        if len(cellranger_tarfiles.index)==0:
          raise ValueError('No cellranger analysis output found for exp {0}'.\
                           format(experiment_igf_id))

        cellranger_tarfile=cellranger_tarfiles['file_path'].values[0]           # select first file as analysis file
        # extract filtered metrics files from tar
        output_dir=get_temp_dir()                                               # get a temp dir
        output_report=os.path.join(output_report,
                                   'report.html')                               # get temp report path
        matrix_file,gene_file,barcode_file=self._extract_cellranger_filtered_metrics(\
                                             tar_file=cellranger_tarfile,
                                             output_dir=output_dir)             # get cellranger output files
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
        au=Analysis_collection_utils(\
             dbsession_class=igf_session_class,
             analysis_name=analysis_name,
             tag_name=species_name,
             collection_name=experiment_igf_id,
             collection_type=scanpy_collection_type,
             collection_table=collection_table,
             base_path=base_result_dir)                                         # initiate loading of report file
        output_file_list=au.load_file_to_disk_and_db(\
                            input_file_list=[output_report],
                            withdraw_exisitng_collection=True)                  # load file to db and disk
        output_report=output_file_list[0]

      self.param('dataflow_params',{'output_report':output_report})             # pass on output report filepath
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise

