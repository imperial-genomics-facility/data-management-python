import os, subprocess
from shlex import quote
from igf_data.utils.fileutils import check_file_path

class Picard_tools:
  '''
  A class for running picard tool
  
  :param java_exe: Java executable path
  :param picard_jar: Picard path
  :param input_files: Input bam filepaths list
  :param output_dir: Output directory filepath
  :param ref_fasta: Input reference fasta filepath
  :param picard_option: Additional picard run parameters as dictionary, default None
  :param java_param: Java parameter, default '-Xmx4g'
  :param strand_info: RNA-Seq strand information, default NONE
  :param ref_flat_file: Input ref_flat file path, default None
  :param suported_commands: A list of supported picard commands
  :param output_prefix: Output prefix name, default None
  :param threads: Number of threads to run for java, default 1
  
                           CollectAlignmentSummaryMetrics
                           CollectGcBiasMetrics
                           QualityScoreDistribution
                           CollectRnaSeqMetrics
                           CollectBaseDistributionByCycle
                           MarkDuplicates
                           AddOrReplaceReadGroups
  '''
  def __init__(self,java_exe,picard_jar,input_files,output_dir,ref_fasta,
               picard_option=None,java_param='-Xmx4g',strand_info='NONE',
               ref_flat_file=None,ribisomal_interval=None,output_prefix=None,
               threads=1,
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
    self.input_file=input_files
    self.output_dir=output_dir
    self.ref_fasta=ref_fasta
    self.picard_option=picard_option
    self.strand_info=strand_info
    self.ref_flat_file=ref_flat_file
    self.suported_commands=suported_commands
    self.ribisomal_interval=ribisomal_interval
    self.output_prefix=output_prefix
    self.threads=threads

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
      input_list=self.input_files
      if self.output_prefix is None:
        output_prefix=os.path.join(self.output_dir,
                                   os.path.basename(input_list[0]))             # set output file prefix
      else:
        output_prefix=self.output_prefix

      output_file='{0}.{1}'.format(output_prefix,
                                     command_name)                              # set output path without any extension
      chart_file='{0}.{1}'.format(output_file,
                                    'pdf')                                      # set chart filepath
      metrics_file='{0}.{1}'.format(output_file,
                                    'summary.txt')                              # set summary metrics path
      if command_name=='CollectAlignmentSummaryMetrics':
        if len(input_list)>1:
          raise ValueError('More than one input file found for picard command {0}'.\
                           format(command_name))

        output_file='{0}.{1}'.format(output_file,
                                    'txt')                                      # add correct extension for output file
        param_dict=[{'I':input_list[0],
                     'O':output_file,
                     'R':self.ref_fasta}
                   ]
        output_list=[output_file]

      elif command_name=='CollectGcBiasMetrics':
        if len(input_list)>1:
          raise ValueError('More than one input file found for picard command {0}'.\
                           format(command_name))

        output_file='{0}.{1}'.format(output_file,
                                    'txt')                                      # add correct extension for output file
        param_dict=[{'I':input_list[0],
                     'O':output_file,
                     'R':self.ref_fasta,
                     'CHART':chart_file,
                     'S':metrics_file}
                   ]
        output_list=[output_file,
                     chart_file,
                     metrics_file
                    ]

      elif command_name=='QualityScoreDistribution':
        if len(input_list)>1:
          raise ValueError('More than one input file found for picard command {0}'.\
                           format(command_name))

        output_file='{0}.{1}'.format(output_file,
                                    'txt')                                      # add correct extension for output file
        param_dict=[{'I':input_list[0],
                     'O':output_file,
                     'CHART':chart_file}
                   ]
        output_list=[output_file,
                     chart_file,
                    ]

      elif command_name=='CollectRnaSeqMetrics':
        if len(input_list)>1:
          raise ValueError('More than one input file found for picard command {0}'.\
                           format(command_name))

        if self.ref_flat_file is None:
          raise ValueError('Missing refFlat annotation file for command {0}'.\
                           format(command_name))

        check_file_path(file_path=self.ref_flat_file)                                # check refFlat file path
        output_file='{0}.{1}'.format(output_file,
                                    'txt')                                      # add correct extension for output file
        param_dict=[{'I':input_list[0],
                     'O':output_file,
                     'R':self.ref_fasta,
                     'REF_FLAT':self.ref_flat_file,
                     'STRAND':self.strand_info,
                     'CHART':chart_file}
                   ]
        if self.ribisomal_interval is not None:
          check_file_path(file_path=self.ribisomal_interval)
          param_dict.update({'RIBOSOMAL_INTERVALS':self.ribisomal_interval})

        output_list=[output_file,
                     chart_file,
                    ]

      elif command_name=='CollectBaseDistributionByCycle':
        if len(input_list)>1:
          raise ValueError('More than one input file found for picard command {0}'.\
                           format(command_name))

        output_file='{0}.{1}'.format(output_file,
                                    'txt')                                      # add correct extension for output file
        param_dict=[{'I':input_list[0],
                     'O':output_file,
                     'CHART':chart_file}
                   ]
        output_list=[output_file,
                     chart_file,
                    ]

      elif command_name=='MarkDuplicates':
        output_file='{0}.{1}'.format(output_file,
                                    'bam')                                      # add correct extension for output file
        param_dict=[{'O':output_file,
                     'M':metrics_file}
                   ]
        for file in input_list:
          param_dict.append({'I':file})

        output_list=[output_file,
                     metrics_file
                    ]

      elif command_name=='AddOrReplaceReadGroups':
        if len(input_list)>1:
          raise ValueError('More than one input file found for picard command {0}'.\
                           format(command_name))

        required_RG_params=["RGID",
                            "RGLB",
                            "RGPL",
                            "RGPU",
                            "RGSM",
                            "RGCN"
                           ]
        if not set(required_RG_params).issubset(set(self.picard_option.keys())):
          raise ValueError('Missing required options for picard cmd {0}:{1}'.\
                           format(command_name,required_RG_params))             # check for required params

        output_file='{0}.{1}'.format(output_file,
                                    'bam')                                      # add correct extension for output file
        param_dict=[{'I':input_list[0],
                     'O':output_file}
                   ]                                                           # not checking for other required inputs
        output_list=[output_file]

      return param_dict, output_list
    except:
      raise

  def run_picard_command(self,command_name,dry_run=False):
    '''
    A method for running generic picard command
    
    :param command_name: Picard command name
    :param dry_run: A toggle for returning picard command without the actual run, default False
    :returns: A list of output files from picard run and picard run command
    '''
    try:
      check_file_path(file_path=self.java_exe)
      check_file_path(file_path=self.picard_jar)
      check_file_path(file_path=self.ref_fasta)
      if not isinstance(self.input_files, list) or \
         len(self.input_files)==0:
        raise ValueError('Missing input file list for picard run')

      for file in self.input_files:
        check_file_path(file_path=file)

      command=[self.java_exe,
               '-XX:ParallelGCThreads={0}'.\
               format(self.threads),
               self.java_param,
               '-jar',
               self.picard_jar,
               quote(command_name)]
      if isinstance(self.picard_option,dict) and \
          len(self.picard_option)>0:
        picard_option=['{0}={1}'.format(quote(param),
                                        quote(val))
                       for param,val in self.picard_option.items()]
        command.extend(picard_option)                                           # additional picard params

      picard_run_param,output_file_list=\
                  self._get_param_for_picard_command(command_name=command_name) # get picard params and output list
      if isinstance(picard_run_param,list) and \
          len(picard_run_param)>1:
        picard_option=['{0}={1}'.format(quote(param),
                                        quote(val))
                       for param_dicts in picard_run_param
                         for param,val in param_dicts.items()]
        command.extend(picard_option)                                           # main picard params
        if dry_run:
          return command,output_file_list

        subprocess.check_call(command)                                          # run picard command
        return output_file_list,command
      else:
        raise ValueError('Picard command {0} not supported yet'.\
                         format(command_name))

    except:
      raise