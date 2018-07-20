import os, subprocess
from shlex import quote
from igf_data.utils.fileutils import check_file_path

class Picard_tools:
  '''
  A class for running picard tool
  
  :param java_exe: Java executable path
  :param picard_jar: Picard path
  :param input_file: Input bam filepath
  :param output_dir: Output directory filepath
  :param ref_fasta: Input reference fasta filepath
  :param picard_option: Additional picard run parameters as dictionary, default None
  :param java_param: Java parameter, default '-Xmx4g'
  :param strand_info: RNA-Seq strand information, default NONE
  :param ref_flat_file: Input ref_flat file path, default None
  :param suported_commands: A list of supported picard commands
  
                           CollectAlignmentSummaryMetrics
                           CollectGcBiasMetrics
                           QualityScoreDistribution
                           CollectRnaSeqMetrics
                           CollectBaseDistributionByCycle
                           MarkDuplicates
                           AddOrReplaceReadGroups
  '''
  def __init__(self,java_exe,picard_jar,input_file,output_dir,ref_fasta,
               picard_option=None,java_param='-Xmx4g',strand_info='NONE',ref_flat_file=None,
               suported_commands=['CollectAlignmentSummaryMetrics',
                                  'CollectGcBiasMetrics',
                                  'QualityScoreDistribution',
                                  'CollectRnaSeqMetrics',
                                  'CollectBaseDistributionByCycle',
                                  'MarkDuplicates',
                                  'AddOrReplaceReadGroups']):
    self.java_exe=java_exe
    self.picard_jar=picard_jar
    self.java_param=java_param
    self.input_file=input_file
    self.output_dir=output_dir
    self.ref_fasta=ref_fasta
    self.picard_option=picard_option
    self.strand_info=strand_info
    self.ref_flat_file=ref_flat_file
    self.suported_commands=suported_commands

  def _get_param_for_picard_command(self,command_name):
    '''
    An internal method for configuring run parameters for picard commands
    
    :param command_name: A picard command name
    :returns: A dictionary of picard run parameter if command is supported or None
    :returns: A list of output files or an empty list
    '''
    try:
      param_dict=None
      output_list=list()
      output_prefix=os.path.join(self.output_dir,
                                 os.path.basename(self.input_file))             # set output file prefix
      output_file='{0}.{1}'.format(output_prefix,
                                     command_name)                              # set output path without any extension
      chart_file='{0}.{1}'.format(output_file,
                                    'pdf')                                      # set chart filepath
      metrics_file='{0}.{1}'.format(output_file,
                                    'summary.txt')                              # set summary metrics path
      if command_name=='CollectAlignmentSummaryMetrics':
        output_file='{0}.{1}'.format(output_file,
                                    'txt')                                      # add correct extension for output file
        param_dict={'I':self.input_file,
                    'O':output_file,
                    'R':self.ref_fasta
                   }
        output_list=[output_file]

      elif command_name=='CollectGcBiasMetrics':
        output_file='{0}.{1}'.format(output_file,
                                    'txt')                                      # add correct extension for output file
        param_dict={'I':self.input_file,
                    'O':output_file,
                    'R':self.ref_fasta,
                    'CHART':chart_file,
                    'S':metrics_file
                   }
        output_list=[output_file,
                     chart_file,
                     metrics_file
                    ]

      elif command_name=='QualityScoreDistribution':
        output_file='{0}.{1}'.format(output_file,
                                    'txt')                                      # add correct extension for output file
        param_dict={'I':self.input_file,
                    'O':output_file,
                    'CHART':chart_file
                   }
        output_list=[output_file,
                     chart_file,
                    ]

      elif command_name=='CollectRnaSeqMetrics':
        if self.ref_flat_file is None:
          raise ValueError('Missing refFlat annotation file for command {0}'.\
                           format(command_name))

        check_file_path(file_path=self.ref_flat_file)                                # check refFlat file path
        output_file='{0}.{1}'.format(output_file,
                                    'txt')                                      # add correct extension for output file
        param_dict={'I':self.input_file,
                    'O':output_file,
                    'R':self.ref_fasta,
                    'REF_FLAT':self.ref_flat_file,
                    'STRAND':self.strand_info,
                    'CHART':chart_file
                   }
        output_list=[output_file,
                     chart_file,
                    ]

      elif command_name=='CollectBaseDistributionByCycle':
        output_file='{0}.{1}'.format(output_file,
                                    'txt')                                      # add correct extension for output file
        param_dict={'I':self.input_file,
                    'O':output_file,
                    'CHART':chart_file
                   }
        output_list=[output_file,
                     chart_file,
                    ]

      elif command_name=='MarkDuplicates':
        output_file='{0}.{1}'.format(output_file,
                                    'bam')                                      # add correct extension for output file
        param_dict={'I':self.input_file,
                    'O':output_file,
                    'M':metrics_file
                   }
        output_list=[output_file,
                     metrics_file
                    ]

      elif command_name=='AddOrReplaceReadGroups':
        output_file='{0}.{1}'.format(output_file,
                                    'bam')                                      # add correct extension for output file
        param_dict={'I':self.input_file,
                    'O':output_file
                   }                                                           # not checking for other required inputs
        output_list=[output_file]

      return param_dict, output_list
    except:
      raise

  def run_picard_command(self,command_name):
    '''
    A method for running generic picard command
    
    :param command_name: Picard command name
    :returns: A list of output files from picard run and picard run command
    '''
    try:
      check_file_path(file_path=self.java_exe)
      check_file_path(file_path=self.picard_jar)
      check_file_path(file_path=self.input_file)
      check_file_path(file_path=self.ref_fasta)

      command=[self.java_exe,
               self.java_param,
               '-jre',
               self.picard_jar,
               quote(command_name)]
      if isinstance(self.picard_option,dict) and \
          len(self.picard_option)>0:
        picard_option=['{0}={1}'.format(param,quote(val))
                       for param,val in self.picard_option.items()]
        command.extend(picard_option)                                           # additional picard params

      picard_run_param,output_file_list=\
                  self._get_param_for_picard_command(command_name=command_name) # get picard params and output list
      if isinstance(picard_run_param,dict) and \
          len(picard_run_param)>1:
        picard_option=['{0}={1}'.format(param,quote(val))
                       for param,val in picard_run_param.items()]
        command.extend(picard_option)                                           # main picard params
        subprocess.check_call(command)                                          # run picard command
        return output_file_list,command
      else:
        raise ValueError('Picard command {0} not supported yet'.\
                         format(command_name))

    except:
      raise