import os
from igf_data.utils.tools.picard_util import Picard_tools
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils
from igf_data.utils.fileutils import get_temp_dir,remove_dir,move_file,get_datestamp_label

class RunPicard(IGFBaseProcess):
  def param_defaults(self):
    params_dict = super(RunPicard,self).param_defaults()
    params_dict.update({
        'reference_type':'GENOME_FASTA',
        'reference_refFlat':'GENE_REFFLAT',
        'ribosomal_interval_type':'RIBOSOMAL_INTERVAL',
        'java_param':'-Xmx4g',
        'copy_input':0,
        'patterned_flowcell_list':['HISEQ4000','NEXTSEQ'],
        'analysis_files':[],
        'picard_option':{},
        'output_prefix':None,
        'load_metrics_to_cram':False,
        'cram_collection_type':'ANALYSIS_CRAM',
        'use_ephemeral_space':0,
      })
    return params_dict

  def run(self):
    '''
    A method for running picard commands

    :param project_igf_id: A project igf id
    :param sample_igf_id: A sample igf id
    :param experiment_igf_id: A experiment igf id
    :param igf_session_class: A database session class
    :param reference_type: Reference genome collection type, default GENOME_FASTA
    :param reference_refFlat: Reference genome collection type, default GENE_REFFLAT
    :param ribosomal_interval_type: Collection type for ribosomal interval list, default RIBOSOMAL_INTERVAL
    :param species_name: species_name
    :param java_exe: Java path
    :param java_java_paramexe: Java run parameters
    :param picard_jar: Picard jar path
    :param picard_command: Picard command
    :param base_work_dir: Base workd directory
    :param copy_input: A toggle for copying input file to temp, 1 for True default 0 for False
    :param use_ephemeral_space: A toggle for temp dir setting, default 0
    :param patterned_flowcell_list: A list of paterned flowcells, default ['HISEQ4000','NEXTSEQ']
    '''
    try:
      temp_output_dir = False
      project_igf_id = self.param_required('project_igf_id')
      experiment_igf_id = self.param_required('experiment_igf_id')
      sample_igf_id = self.param_required('sample_igf_id')
      java_exe = self.param_required('java_exe')
      java_param = self.param_required('java_param')
      picard_jar = self.param_required('picard_jar')
      input_files = self.param_required('input_files')
      picard_command = self.param_required('picard_command')
      igf_session_class = self.param_required('igf_session_class')
      species_name = self.param('species_name')
      reference_type = self.param('reference_type')
      reference_refFlat = self.param('reference_refFlat')
      ribosomal_interval_type = self.param('ribosomal_interval_type')
      base_work_dir = self.param_required('base_work_dir')
      analysis_files = self.param_required('analysis_files')
      picard_option = self.param('picard_option')
      patterned_flowcell_list = self.param('patterned_flowcell_list')
      platform_name = self.param_required('platform_name')
      output_prefix = self.param('output_prefix')
      load_metrics_to_cram = self.param('load_metrics_to_cram')
      cram_collection_type = self.param('cram_collection_type')
      seed_date_stamp = self.param_required('date_stamp')
      use_ephemeral_space = self.param('use_ephemeral_space')
      seed_date_stamp = get_datestamp_label(seed_date_stamp)
      if output_prefix is not None:
        output_prefix = \
          '{0}_{1}'.\
            format(
              output_prefix,
              seed_date_stamp)                                                  # adding seed datestamp to output prefix

      work_dir_prefix = \
        os.path.join(
          base_work_dir,
          project_igf_id,
          sample_igf_id,
          experiment_igf_id)
      work_dir = \
        self.get_job_work_dir(work_dir=work_dir_prefix)                         # get a run work dir
      temp_output_dir = \
        get_temp_dir(use_ephemeral_space=use_ephemeral_space)                   # get temp work dir
      ref_genome = \
        Reference_genome_utils(
          genome_tag=species_name,
          dbsession_class=igf_session_class,
          genome_fasta_type=reference_type,
          gene_reflat_type=reference_refFlat,
          ribosomal_interval_type=ribosomal_interval_type)                      # setup ref genome utils
      genome_fasta = ref_genome.get_genome_fasta()                              # get genome fasta
      ref_flat_file = ref_genome.get_gene_reflat()                              # get refFlat file
      ribosomal_interval_file = ref_genome.get_ribosomal_interval()             # get ribosomal interval file
      patterned_flowcell = False
      if platform_name in patterned_flowcell_list:                              # check for patterned flowcell
        patterned_flowcell = True

      if load_metrics_to_cram and \
         not cram_collection_type:
        raise ValueError('Cram file collection type is required for loading picard metrics to db')

      picard=\
        Picard_tools(\
          java_exe=java_exe,
          java_param=java_param,
          picard_jar=picard_jar,
          input_files=input_files,
          output_dir=temp_output_dir,
          ref_fasta=genome_fasta,
          patterned_flowcell=patterned_flowcell,
          ref_flat_file=ref_flat_file,
          picard_option=picard_option,
          output_prefix=output_prefix,
          use_ephemeral_space=use_ephemeral_space,
          ribisomal_interval=ribosomal_interval_file)                           # setup picard tool
      temp_output_files,picard_command_line,picard_metrics = \
        picard.run_picard_command(command_name=picard_command)                  # run picard command
      output_file_list = list()
      for source_path in temp_output_files:
        dest_path=\
          os.path.join(
            work_dir,
            os.path.basename(source_path))                                      # get destination filepath
        move_file(
          source_path=source_path,
          destinationa_path=dest_path,
          force=True)                                                           # move files to work dir
        output_file_list.append(dest_path)
      remove_dir(temp_output_dir)
      analysis_files.extend(output_file_list)
      bam_files = list()
      for file in output_file_list:
        if file.endswith('.bam'):
          bam_files.append(file)

      if load_metrics_to_cram and \
         len(picard_metrics)>0:
        ca = CollectionAdaptor(**{'session_class':igf_session_class})
        attribute_data = \
          ca.prepare_data_for_collection_attribute(
            collection_name=experiment_igf_id,
            collection_type=cram_collection_type,
            data_list=picard_metrics)                                           # fromat data for collection attribute table
        ca.start_session()
        try:
          ca.create_or_update_collection_attributes(\
            data=attribute_data,
            autosave=False
          )                                                                     # load data to collection attribute table
          ca.commit_session()
          ca.close_session()
        except:
          ca.rollback_session()
          ca.close_session()
          raise

      self.param('dataflow_params',
                 {'analysis_files':analysis_files,
                  'bam_files':bam_files,
                  'seed_date_stamp':seed_date_stamp})                           # pass on picard output list
      message = \
        'finished picard {0} for {1} {2}'.\
          format(
            picard_command,
            project_igf_id,
            sample_igf_id)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.post_message_to_ms_team(
          message=message,
          reaction='pass')
      message = \
        'Picard {0} command: {1}'.\
          format(
            picard_command,
            picard_command_line)
      #self.comment_asana_task(task_name=project_igf_id, comment=message)        # send commandline to Asana
    except Exception as e:
      if temp_output_dir and \
         os.path.exists(temp_output_dir):
        remove_dir(temp_output_dir)

      message = \
        'project: {2}, sample:{3}, Error in {0}: {1}'.\
          format(
            self.__class__.__name__,
            e,
            project_igf_id,
            sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      self.post_message_to_ms_team(
          message=message,
          reaction='fail')
      raise