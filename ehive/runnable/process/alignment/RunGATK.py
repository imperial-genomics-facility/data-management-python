import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.tools.gatk_utils import GATK_tools
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils

class RunGATK(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(RunGATK,self).param_defaults()
    params_dict.update({
        'reference_dbsnp_type':'DBSNP_VCF',
        'reference_indel_type':'INDEL_LIST_VCF',
        'reference_fasta_type':'GENOME_FASTA',
        'java_param':'-XX:ParallelGCThreads=1 -Xmx4g',
        'options':None,
        'force_overwrite':True,
        'hc_gvcf':True,
        'gatk_allowed_commands':['BaseRecalibrator',
                                 'ApplyBQSR',
                                 'AnalyzeCovariates',
                                 'HaplotypeCaller'
                                ]
      })
    return params_dict

  def run(self):
    '''
    A method to run ehive runnable for GATK 4
    '''
    try:
      project_igf_id = self.param_required('project_igf_id')
      experiment_igf_id = self.param_required('experiment_igf_id')
      sample_igf_id = self.param_required('sample_igf_id')
      gatk_exe = self.param_required('gatk_exe')
      gatk_command = self.param_required('gatk_command')
      gatk_allowed_commands = self.param('gatk_allowed_commands')
      igf_session_class = self.param_required('igf_session_class')
      species_name = self.param('species_name')
      reference_dbsnp_type = self.param_required('reference_dbsnp_type')
      reference_indel_type = self.param_required('reference_indel_type')
      reference_fasta_type = self.param_required('reference_fasta_type')
      base_work_dir = self.param_requiredd('base_work_dir')
      java_param = self.param_required('java_param')
      force_overwrite = self.param('force_overwrite')
      if gatk_command not in gatk_allowed_commands:
        raise ValueError(
                'Gatk command {0} not supported'.\
                  format(gatk_command))

      work_dir_prefix = \
        os.path.join(
          base_work_dir,
          project_igf_id,
          sample_igf_id,
          experiment_igf_id)
      work_dir = \
        self.get_job_work_dir(
          work_dir=work_dir_prefix)                                             # get a run work dir
      ref_genome = \
        Reference_genome_utils(
          genome_tag=species_name,
          dbsession_class=igf_session_class,
          genome_fasta_type=reference_fasta_type,
          genome_dbsnp_type=reference_dbsnp_type,
          gatk_indel_ref_type=reference_indel_type)                             # setup ref genome utils
      genome_fasta = ref_genome.get_genome_fasta()                              # get genome fasta
      dbsnp_vcf = ref_genome.get_dbsnp_vcf()                                    # get dbsnp vcf
      indel_vcf = ref_genome.get_gatk_indel_ref()                               # get indel vcf
      gatk_obj = \
        GATK_tools(
          gatk_exe=gatk_exe,
          ref_fasta=genome_fasta,
          java_param=java_param)                                                # setup gatk tool for run
      gatk_cmdline = None
      if gatk_command == 'BaseRecalibrator':
        input_bam = self.param_required('input_bam')
        output_table = \
          os.path.join(
            work_dir,
            '{0}_{1}.table'.\
            format(
              os.path.basename(input_bam),
              gatk_command))
        gatk_cmdline = \
          gatk_obj.\
            run_BaseRecalibrator(
              input_bam=input_bam,
              output_table=output_table,
              known_snp_sites=dbsnp_vcf,
              known_indel_sites=indel_vcf,
              force=force_overwrite)
        self.param(
          'dataflow_params',
          {'BaseRecalibrator_table':output_table})                              # pass on bqsr output list

      elif gatk_command == 'ApplyBQSR':
        bqsr_recal_file = self.param_required('bqsr_recal_file')
        input_bam = self.param_required('input_bam')
        output_bam_path = \
          os.path.join(
            work_dir,
            '{0}_{1}.bam'.\
              format(
                os.path.basename(input_bam).\
                  replace('.bam',''),
                gatk_command))
        gatk_cmdline = \
          gatk_obj.\
            run_ApplyBQSR(
              bqsr_recal_file=bqsr_recal_file,
              input_bam=input_bam,
              output_bam_path=output_bam_path,
              force=force_overwrite)
        self.param(
          'dataflow_params',
          {'ApplyBQSR_bam':output_bam_path})                                    # pass on apply bqsr bam

      elif gatk_command == 'AnalyzeCovariates':
        before_report_file = self.param_required('before_report_file')
        after_report_file = self.param_required('after_report_file')
        output_pdf_path = \
          os.path.join(
            work_dir,
            '{0}_{1}.pdf'.\
              format(
                os.path.basename(before_report_file).\
                  replace('.table',''),
                gatk_command))
        gatk_cmdline = \
          gatk_obj.\
            run_AnalyzeCovariates(
               before_report_file=before_report_file,
               after_report_file=after_report_file,
               output_pdf_path=output_pdf_path,
               force=force_overwrite)
        self.param(
          'dataflow_params',
          {'AnalyzeCovariates_pdf':output_pdf_path})                            # pass on apply bqsr bam

      elif gatk_command == 'HaplotypeCaller':
        input_bam = self.param_required('input_bam')
        emit_gvcf = self.param('hc_gvcf')
        output_vcf_path = \
          os.path.join(
            work_dir,
            '{0}_{1}.g.vcf'.\
              format(
                os.path.basename(before_report_file).\
                  replace('.bam',''),
                gatk_command))
        gatk_cmdline = \
          gatk_obj.\
            run_HaplotypeCaller(
              input_bam=input_bam,
              output_vcf_path=output_vcf_path,
              dbsnp_vcf=dbsnp_vcf,
              emit_gvcf=emit_gvcf,
              force=force_overwrite)
        if emit_gvcf:
          self.param(
            'dataflow_params',
            {'HaplotypeCaller_gvcf':output_vcf_path})                           # pass on hc gvcf
        else:
          self.param(
            'dataflow_params',
            {'HaplotypeCaller_vcf':output_vcf_path})                            # pass on hc vcf

      else:
        raise ValueError('Gatk command {0} not supported'.format(gatk_command))

      message = \
        'Finished GATK {0} for {1}: {2}'.\
          format(
            project_igf_id,
            sample_igf_id,
            gatk_cmdline)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.comment_asana_task(
        task_name=project_igf_id,
        comment=message)                                                        # send comment to Asana
      message = \
        'GATK {0}, {1} command: {2}'.\
          format(
            experiment_igf_id,
            gatk_command,
            gatk_cmdline)
      self.comment_asana_task(
        task_name=project_igf_id,
        comment=message)                                                        # send commandline to Asana
    except Exception as e:
      message = \
        'project: {2}, sample:{3}, Error in {0}: {1}'.\
          format(
            self.__class__.__name__,
            e,
            project_igf_id,
            sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise