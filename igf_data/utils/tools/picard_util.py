import os, subprocess
from shlex import quote

class Picard_util:
  '''
  A class for running picard tool
  
  :param java_exe: Java executable path
  :param picard_jar: Picard path
  :param java_param: Java parameter, default '-Xmx4g'
  '''
  def __init__(self,java_exe,picard_jar,java_param='-Xmx4g'):
    try:
      self.java_exe=java_exe
      self.picard_jar=picard_jar
      self.java_param=java_param
      if not os.path.exists(self.java_exe):
        raise IOError('Missing java executable')

      if not os.path.exists(self.picard_jar):
        raise IOError('Missing picard jar file')

    except:
      raise

  def run_picard_command(self,command_name,picard_option):
    '''
    A method for running generic picard command
    
    :param command_name: Picard command name
    :param picard_option: Picard command options as python dictionary
    '''
    try:
      command=[self.java_exe,
               self.java_param,
               '-jre',
               self.picard_jar,
               quote(command_name)]
      if isinstance(picard_option,dict) and \
          len(picard_option)>1:
        picard_option=['{0}={1}'.format(param,quote(val))
                       for param,val in picard_option.items()]
        command.extend(picard_option)
        subprocess.check_call(command)
      else:
        raise ValueError('Missing required parameters')

    except:
      raise