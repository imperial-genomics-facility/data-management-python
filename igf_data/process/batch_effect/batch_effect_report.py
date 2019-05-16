from shlex import quote
import pandas as pd
from copy import copy
import matplotlib
from sklearn.decomposition import PCA
import subprocess,re,os,json,base64
from igf_data.utils.fileutils import get_temp_dir,remove_dir,check_file_path,copy_local_file
from jinja2 import Template,Environment, FileSystemLoader,select_autoescape
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

class Batch_effect_report:
  '''
  A python class for checking lane level batch effect for RNA-Seq sample
  
  :param input_json_file: A json file containing the list of following info
  
                           file
                           flowcell
                           lane
                           
  :param template_file: A template file for writing the report
  :param rscript_path: Rscript exe path
  :param batch_effect_rscript_path: CPM conversion R-script file path
  :param strand_info: RNA-Seq strand information, default reverse_strand
  :param read_threshold: Threshold for low number of reads, default 5
  :param allowed_strands: A list of allowed strands,
   
                           reverse_strand
                           forward_strand
                           unstranded'
  '''
  def __init__(self,input_json_file,template_file,rscript_path,batch_effect_rscript_path,
               allowed_strands=('reverse_strand','forward_strand','unstranded'),
               read_threshold=5,strand_info='reverse_strand'):
    self.input_json_file=input_json_file
    self.template_file=template_file
    self.batch_effect_rscript_path=batch_effect_rscript_path
    self.rscript_path=rscript_path
    self.strand_info=strand_info
    self.allowed_strands=list(allowed_strands)
    self.read_threshold=read_threshold


  @staticmethod
  def _encode_png_image(png_file):
    '''
    A static method for encoding png files to string data
    
    :param png_file: A png filepath
    :returns: A string data
    '''
    try:
      if not os.path.exists(png_file):
        raise ValueError('File not present')
    
      encoded=base64.b64encode(open(png_file, "rb").read()).decode()
      return encoded
    except:
        raise


  def check_lane_effect_and_log_report(self,project_name,sample_name,output_file):
    '''
    A function for generating batch effect report for a sample and project
    
    :param project_name: A project name for the report file
    :param sample_name: A sample name for the report file
    :param output_file: Path of the output report file
    '''
    try:
      if self.strand_info not in self.allowed_strands:
        raise ValueError('{0} is not a valid strand'.format(self.strand_info))

      temp_dir=get_temp_dir(use_ephemeral_space=True)
      temp_merged_output=os.path.join(temp_dir,'merged.csv')
      temp_cpm_output=os.path.join(temp_dir,'merged_cpm.csv')
      temp_png_output=os.path.join(temp_dir,'plot.png')
      temp_clustermap=os.path.join(temp_dir,'clustermap.png')
      temp_corr=os.path.join(temp_dir,'corr.png')
      temp_pca_flowcell=os.path.join(temp_dir,'pca_flowcell.png')
      temp_pca_flowcell_lane=os.path.join(temp_dir,'pca_flowcell_lane.png')
      temp_html_report=os.path.join(temp_dir,
                                    os.path.basename(self.template_file))
      check_file_path(self.input_json_file)
      check_file_path(self.rscript_path)
      check_file_path(self.batch_effect_rscript_path)
      with open(self.input_json_file,'r') as json_data:
        input_list=json.load(json_data)

      if len(input_list)<2:
        raise ValueError('Minimum two input files are required for lane level batch effect checking')

      gene_name_label='gene_name'
      final_df=pd.DataFrame()
      for entry in input_list:
        file=entry.get('file')
        flowcell=entry.get('flowcell')
        lane=entry.get('lane')
        if file is None or \
           flowcell is None or \
           lane is None:
          raise ValueError('Missing required info for batch effect check: {0}'.\
                           format(entry))
        unstranded_label='unstranded_{0}_{1}'.format(flowcell,lane)
        reverse_strand_label='reverse_strand_{0}_{1}'.format(flowcell,lane)
        forward_strand_label='forward_strand_{0}_{1}'.format(flowcell,lane)
        data=pd.read_csv(\
                  file,
                  sep='\t',
                  header=None,
                  skiprows=4,
                  index_col=False,
                  names=[gene_name_label,
                         unstranded_label,
                         forward_strand_label,
                         reverse_strand_label])
        if self.strand_info=='reverse_strand':
          data=data[[gene_name_label,
                     reverse_strand_label
                   ]]
          data=data[data[reverse_strand_label]>self.read_threshold]                  # filter series and remove any low value gene
        elif self.strand_info=='forward_strand':
          data=data[[gene_name_label,
                     forward_strand_label
                   ]]
          data=data[data[forward_strand_label]>self.read_threshold]                  # filter series and remove any low value gene
        elif self.strand_info=='unstranded':
          data=data[[gene_name_label,
                     unstranded_label
                   ]]
          data=data[data[unstranded_label]>self.read_threshold]                      # filter series and remove any low value gene
        if len(final_df.index)==0:
          final_df=copy(data)
        else:
          final_df=final_df.\
                   merge(data,
                         how='outer',
                         on=gene_name_label)

      final_df=final_df.dropna().set_index(gene_name_label)                     # remove any row with NA values from df
      final_df.\
      applymap(lambda x: float(x)).\
      to_csv(temp_merged_output,index=True)                                     # dump raw counts as csv file
      rscript_cmd=[quote(self.rscript_path),
                   quote(self.batch_effect_rscript_path),
                   quote(temp_merged_output),
                   quote(temp_cpm_output),
                   quote(temp_png_output)
                  ]
      subprocess.check_call(rscript_cmd,shell=False)                            # run r script for cpm counts
      check_file_path(temp_cpm_output)                                          # check output file
      mod_data=pd.read_csv(temp_cpm_output).\
               rename(columns={'Unnamed: 0':gene_name_label}).\
               set_index(gene_name_label)                                       # read output file
      sns_fig=sns.clustermap(mod_data,figsize=(10,10))
      sns_fig.fig.savefig(temp_clustermap)
      check_file_path(temp_clustermap)                                          # plot clustermap
      corr_df=mod_data.corr()
      cmap=sns.diverging_palette(220, 10,
                                 as_cmap=True)
      fig,ax=plt.subplots(figsize=(7,7))
      sns.heatmap(corr_df,
                  cmap=cmap,
                  square=True,
                  linewidths=.5,
                  cbar_kws={"shrink": .4},
                  ax=ax);
      plt.savefig(temp_corr)
      check_file_path(temp_corr)                                                # plot correlation values
      pca = PCA(n_components=2)
      X_r = pca.fit(mod_data.T).transform(mod_data.T)
      pattern1=re.compile(r'(rev_strand|forward_strand|unstranded)_(\S+)_([1-8])')
      pattern2=re.compile(r'(rev_strand|forward_strand|unstranded)_(\S+_[1-8])')
      results_df=pd.DataFrame(\
                     {'PCA1':X_r[:,0],
                      'PCA2':X_r[:,1],
                      'flowcell':[re.match(pattern1,label).group(2)
                                  if re.match(pattern1,label) else label
                                    for label in mod_data.T.index],
                      'flowcell_lane':[re.match(pattern2,label).group(2)
                                       if re.match(pattern2,label) else label
                                         for label in mod_data.T.index]
                     })
      pca_plot=sns.lmplot('PCA1',
                          'PCA2',
                          hue='flowcell',
                          data=results_df,
                          fit_reg=False);
      pca_plot.fig.savefig(temp_pca_flowcell)                                   # plot flowcell level pca
      pca_plot=sns.lmplot('PCA1',
                          'PCA2',
                          hue='flowcell_lane',
                          data=results_df,
                          fit_reg=False);
      pca_plot.fig.savefig(temp_pca_flowcell_lane)                              # plot flowcell-lane level pca
      template_env=Environment(\
                     loader=FileSystemLoader(\
                              searchpath=os.path.dirname(self.template_file)),
                     autoescape=select_autoescape(['xml']))
      template_file=template_env.\
                    get_template(os.path.basename(self.template_file))
      template_file.\
        stream(ProjectName=project_name,
               SampleName=sample_name,
               mdsPlot=self._encode_png_image(png_file=temp_png_output),
               clustermapPlot=self._encode_png_image(png_file=temp_clustermap),
               corrPlot=self._encode_png_image(png_file=temp_corr),
               pca1Plot=self._encode_png_image(png_file=temp_pca_flowcell),
               pca2Plot=self._encode_png_image(png_file=temp_pca_flowcell_lane),
              ).\
        dump(temp_html_report)
      copy_local_file(temp_html_report,
                      output_file,
                      force=True)
    except:
      raise
        