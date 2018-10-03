import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.tools.rsem_utils import RSEM_utils
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils

class RunRSEM(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(RunRSEM,self).param_defaults()
    params_dict.update({
      'strandedness':'reverse',
      'threads':1,
      
    })
    return params_dict

  