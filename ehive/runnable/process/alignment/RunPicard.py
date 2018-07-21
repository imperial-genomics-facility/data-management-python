import os
from igf_data.utils.tools.picard_util import Picard_tools
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils
from igf_data.utils.fileutils import get_temp_dir,remove_dir,move_file

class RunPicard(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(RunPicard,self).param_defaults()
    params_dict.update({
        'reference_type':'GENOME_FASTA',
        'reference_refFlat':'GENE_REFFLAT',
        'java_param':'-Xmx4g',
        'copy_input':0,
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
    :param species_name: species_name
    :param java_exe: Java path
    :param java_java_paramexe: Java run parameters
    :param picard_jar: Picard jar path
    :param picard_command: Picard command
    :param base_work_dir: Base workd directory
    :param copy_input: A toggle for copying input file to temp, 1 for True default 0 for False
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      java_exe=self.param_required('java_exe')
      java_param=self.param_required('java_param')
      picard_jar=self.param_required('picard_jar')
      input_file=self.param_required('input_file')
      picard_command=self.param_required('picard_command')
      igf_session_class=self.param_required('igf_session_class')
      species_name=self.param('species_name')
      reference_type=self.param('reference_type')
      reference_refFlat=self.param('reference_refFlat')
      base_work_dir=self.param_required('base_work_dir')
      copy_input=self.param('copy_input')
      work_dir_prefix=os.path.join(base_work_dir,
                                   project_igf_id,
                                   sample_igf_id,
                                   experiment_igf_id)
      work_dir=self.get_job_work_dir(work_dir=work_dir_prefix)                  # get a run work dir
      if copy_input==1:
        input_file=self.copy_input_file_to_temp(input_file=input_file)          # copy input to temp dir

      temp_output_dir=get_temp_dir()                                            # get temp work dir
      ref_genome=Reference_genome_utils(genome_tag=species_name,
                                        dbsession_class=igf_session_class,
                                        genome_fasta_type=reference_type,
                                        gene_reflat_type=reference_refFlat
                                       )                                        # setup ref genome utils
      genome_fasta=ref_genome.get_genome_fasta()                                # get genome fasta
      ref_flat_file=ref_genome.get_gene_reflat()                                # get refFlat file
      picard=Picard_tools(java_exe=java_exe,
                          java_param=java_param,
                          picard_jar=picard_jar,
                          input_file=input_file,
                          output_dir=temp_output_dir,
                          ref_fasta=genome_fasta,
                          ref_flat_file=ref_flat_file)                          # get genome fasta)                               # setup picard tool
      temp_output_files,picard_command_line=\
           picard.run_picard_command(command_name=picard_command)               # run picard command
      output_file_list=list()
      for source_path in temp_output_files:
        dest_path=os.path.join(work_dir,
                               os.path.basename(source_path))                   # get destination filepath
        move_file(source_path=source_path,
                  destinationa_path=dest_path,
                  force=True)                                                   # move files to work dir
        output_file_list.append(dest_path)
      remove_dir(temp_output_dir)
      self.param('dataflow_params',{picard_command:output_file_list})           # pass on picard output list
      message='finished picard {0} for {1} {2}'.\
              format(picard_command,
                     project_igf_id,
                     sample_igf_id)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.comment_asana_task(task_name=project_igf_id, comment=message)        # send comment to Asana
      message='Picard {0} command: {1}'.\
              format(picard_command,
                     picard_command_line)
      self.comment_asana_task(task_name=project_igf_id, comment=message)        # send commandline to Asana
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise