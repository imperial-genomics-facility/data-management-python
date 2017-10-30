import os, json, math, re
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.sequtils import rev_comp

class IndexBarcodeValidationError(Exception):
  '''
  A custom exception class for checking and reporting validation error
  returns a message and a list of plots file path
  '''
  def __init__(self,message,plots):
    self.message=message
    self.plots=plots
    
    
class CheckSequenceIndexBarcodes:
  '''
  A class for sequencing run index barcode stats calculation and validation
  required parameters:
  stats_json_file: Stats.json file from demultiplexing run
  samplesheet_file: Samplesheet file containing all sample barcodes (for the lane)
  '''
  def __init__(self,stats_json_file,samplesheet_file,platform_name=None):
    self.stats_json_file=stats_json_file
    self.samplesheet_file=samplesheet_file
    self.platform_name=platform_name
    self._check_barcode_stats()
    
 
  def _get_dataframe_from_stats_json(self,json_file):
    '''
    A internal method for reading Illumina Stats.json file as dataframes
    required params:
    json_file: Stats.json file from sequencing run
    '''
    if not os.path.exists(json_file):
      raise IOError('file {0} not found'.format(json_file))
    
    with open(json_file,'r') as json_data:
      json_stats=json.load(json_data)

    data1=list()
    runid=json_stats['RunId']
    total_read=None
    for row in json_stats['ConversionResults']:
      lane=row['LaneNumber']
      total_read=row['TotalClustersPF']
      for sample in row['DemuxResults']:
        sample_id=sample['SampleId']
        number_reads=sample['NumberReads']
        for index in sample['IndexMetrics']:
          index_seq=index['IndexSequence']
          data1.append({'lane':lane,
                        'sample':sample_id,
                        'index':index_seq,
                        'reads':number_reads,
                        'tag':'known',
                        'runid':runid,
                        'total_read':total_read})
    df1=pd.DataFrame(data1)
    df1=df1.sort_values(['reads'],ascending=True)
  
    data2=list()
    for row in json_stats['UnknownBarcodes']:
      lane=row['Lane']
      total_read=df1.groupby('lane').get_group(lane)['total_read'].max()
      for barcode,count in sorted(row['Barcodes'].items(), key=lambda x: x[1], \
                                  reverse=True):
        data2.append({'lane':lane,
                      'sample':'undetermined',
                      'index':barcode,
                      'reads':count,
                      'tag':'unknown',
                      'runid':runid,
                      'total_read':total_read})
        
    df2=pd.DataFrame(data2)
    df=pd.concat([df1,df2])
    df['mapping_ratio']=df['reads'].map(lambda x: x/total_read)
    return df

  
  def _generate_pct(self,x):
    '''
    calculate percentage values
    '''
    known_read=int(x['known_read'])
    unknown_read=int(x['unknown_read'])
    known_pct=(known_read/(known_read+unknown_read))*100
    x['known_pct']=known_pct
    unknown_pct=(unknown_read/(known_read+unknown_read))*100
    x['unknown_pct']=unknown_pct
    return x

  
  def _check_barcode_stats(self, nextseq_label='NEXTSEQ', miseq_label='MISEQ'):
    '''
    An internal method for converting barcode stats json file to dataframes, read samplesheet,
    remove all known barcodes for the flowcell lane from the pool of unknown barcodes
    required params:
    stats_json: Stats.json file from demultiplexing output
    sample_sheet: Samplesheet from sequencing run
    '''
    stats_json=self.stats_json_file
    sample_sheet=self.samplesheet_file
    platform_name=self.platform_name
    stats_df=pd.DataFrame(columns=['index','lane','reads',
                                   'runid','sample','tag',
                                   'total_read','mapping_ratio'])               # define structure for stats df
  
    json_data=self._get_dataframe_from_stats_json(json_file=stats_json)         # get stats json data for each file
    stats_df=pd.concat([json_data,stats_df])                                    # combine all json files
    raw_df=pd.DataFrame()
    
    for rid, rg in stats_df.groupby('runid'):
      for lid, lg in rg.groupby('lane'):
        samplesheet_data=SampleSheet(infile=sample_sheet)                       # using the same samplesheet for filter
        u_stats_df=lg.groupby('tag').get_group('unknown')
        k_stats_df=lg.groupby('tag').get_group('known')                         # separate known and unknown groups
        all_lanes=[1,2,3,4] if platform_name==nextseq_label \
                            else samplesheet_data.get_lane_count();             # nextseq in weird
        if platform_name==nextseq_label:
          samplesheet_data.add_pseudo_lane_for_nextseq()                        # add pseudo lane info for NextSeq
        
        if platform_name==miseq_label:
          samplesheet_data.add_pseudo_lane_for_miseq()                          # add pseudo lane info for NextSeq
        
        if lid in all_lanes:
          if platform_name==nextseq_label:
            samplesheet_data.filter_sample_data(condition_key='PseudoLane', \
                                                condition_value=lid)            # filter samplesheet for the Pseudolane dynamically
          elif platform_name==miseq_label:
            samplesheet_data.filter_sample_data(condition_key='PseudoLane', \
                                                condition_value=lid)            # filter samplesheet for the Pseudolane dynamically
          else:
            samplesheet_data.filter_sample_data(condition_key='Lane', \
                                                condition_value=lid)            # filter samplesheet for the lane dynamically
            
          all_known_indexes=samplesheet_data.get_indexes()                      # get all known indexes present on the lane
          u_stats_df=u_stats_df[u_stats_df['index'].\
                                isin(all_known_indexes)==False]                 # filter all known barcodes from unknown with exact match
          u_stats_df=u_stats_df.apply(lambda x: \
                                      self._check_index_for_match\
                                      (data_series=x,\
                                       index_vals=all_known_indexes),\
                                      axis=1)                                   # check and modify tags for unknown indexes
        
        raw_df=pd.concat([k_stats_df,u_stats_df,raw_df])                        # merge dataframe back together
    raw_df['log_total_read']=raw_df['total_read'].map(lambda x: math.log2(x))   # add log2 of total reads in df
    summary_df=raw_df.pivot_table(values=['reads'],\
                                  index=['tag','lane','runid'], \
                                  aggfunc=np.sum)                               # create summary df
    processed_df=pd.DataFrame(columns=['id','known_read','unknown_read'])       # define processed df structure
  
    for rid, rg in summary_df.groupby('runid'):
      for lid, lg in rg.groupby('lane'):
        known_reads=lg.loc['known']['reads'].values[0]
        unknown_reads=lg.loc['unknown']['reads'].values[0]
        runid='{0}_{1}'.format(rid, lid)
        df=pd.DataFrame([{'id':runid,'known_read':known_reads,
                          'unknown_read':unknown_reads}])
        processed_df=pd.concat([df,processed_df])                               # format processed df
      
    processed_df=processed_df.apply(lambda x: self._generate_pct(x), axis=1)    # calculate % of known and unknown barcodes
    self.raw_df=raw_df
    self.processed_df=processed_df


  def validate_barcode_stats(self,work_dir,know_barcode_ratio_cutoff=80, strict_check=True):
    '''
    A method for checking barcode stats for validating sequencing run
    required params:
    work_dir: A path to the work directory
    know_barcode_ratio_cutoff: Lower cut-off for % of known barcode reads, default=80
    strict_check: Parameter to turn on or off the strict checking, default is True
    '''
    try:
      summary_df=self.processed_df
      raw_df=self.raw_df
      
      # check known_pct/unknown_pct
      for runid, grp in summary_df.groupby('id'):
        if strict_check and grp['known_pct'].values[0] < know_barcode_ratio_cutoff:
          (all_barcode_plot,index_plot)=self._generate_barcode_plots(work_dir=work_dir)                 # plot stats
          raise IndexBarcodeValidationError(message='{0} failed total barcode check: {1}'.\
                                            format(runid, grp['known_pct'].values[0]), \
                                            plots=[all_barcode_plot,index_plot])
      # check for individual barcodes mapping ratios
      for rid, rgrp in raw_df.groupby('runid'):
        for lid,lgrp in rgrp.groupby('lane'):
          all_tag_groups=lgrp.groupby('tag').groups.keys()
          extra_message=None                                                    # default extra message is none
          if 'known' not in all_tag_groups and \
             'unknown' not in all_tag_groups:
            raise ValueError('tag known and unknown not found:{0}'.\
                             format(all_tag_groups))
          if 'index_1_revcomp' in all_tag_groups:
            message='{0} {1} found index_1_revcomp'.format(rid, lid)
            extra_message += ', '+message
          
          if 'only_index_1_revcomp' in all_tag_groups:
            message='{0} {1} found only_index_1_revcomp'.format(rid, lid)
            extra_message += ', '+message
            
          if 'index_1_and_index_2_revcomp' in all_tag_groups:
            message='{0} {1} found index_1_and_index_2_revcomp'.\
                                              format(rid, lid)
            extra_message += ', '+message
          
          if 'only_index_2_revcomp' in all_tag_groups:
            message='{0} {1} found only_index_2_revcomp'.\
                                              format(rid, lid)
            extra_message += ', '+message
                        
          known_grp=lgrp.groupby('tag').get_group('known')
          min_known_mpr=known_grp['mapping_ratio'].min()
          unknown_grp=lgrp.groupby('tag').get_group('unknown')
          max_unknown_mpr=unknown_grp['mapping_ratio'].max()
          if strict_check and min_known_mpr < max_unknown_mpr:
            (all_barcode_plot,index_plot)=self._generate_barcode_plots(work_dir=work_dir)            # plot stats
            raise IndexBarcodeValidationError(message='{0} {1} failed mapping ratio check, {2}'.\
                                              format(rid, lid,extra_message), \
                                              plots=[all_barcode_plot,index_plot])                   # report about the first issue
            
    except:
        raise


  def _check_index_for_match(self,data_series,index_vals,index_tag='index'):
    '''
    An internal method for checking unknown indexes
    required params:
    data_series: A Pandas series containing the row of raw_df
    index_vals: A list of indexes present in the reformatted samplesheet
    '''
    try:
      if not isinstance(data_series, pd.Series):
        data_series=pd.Series(data_series)
      
      index_pattern=re.compile(r'([ATGCN]+)(\+)?([ATCGN]+)?')                   # define pattern for index reads
      tag=data_series['tag']
      if tag=='unknown':                                                        # check only unknown indexes
        unknown_index=data_series[index_tag]
        unknown_index=unknown_index.strip().strip('\n')                         # remove space and new line
        unknown_index_1=False
        unknown_index_2=False                                                   # define index reads for unknown index 
        unknown_index_match=index_pattern.match(unknown_index)
        if unknown_index_match.group(2) and unknown_index_match.group(2)=='+':
          unknown_index_1=unknown_index_match.group(1)
          unknown_index_2=unknown_index_match.group(3)
        else:
          unknown_index_1=unknown_index_match.group(1)
          
        for index_seq in index_vals:
          index_seq=index_seq.strip().strip('\n')                               # remove all white space and new line
          known_index_1=False
          known_index_2=False
          known_index_match=index_pattern.match(index_seq)
          if known_index_match.group(2) and known_index_match.group(2)=='+':
            known_index_1=known_index_match.group(1)
            known_index_2=known_index_match.group(3)
          else:
            known_index_1=known_index_match.group(1)
          
          # CASE 1: unknown index is 8 and known index from other project is 8+8 or reverse
          if len(unknown_index_1) == len(known_index_1) and \
             unknown_index_1==known_index_1:
            if unknown_index_2 is False or known_index_2 is False:
              tag='mix_index_match'                                             # its safe to assume about the possible mixed barcode issue
            elif unknown_index_2 and known_index_2 and \
                 unknown_index_2==known_index_2:
              tag='known'                                                       # assign tag as known
              
          # CASE 2: unknown index is 6 or 6+6 and knonw_index from other project is 8 or 8+8
          elif len(unknown_index_1) < len(known_index_1):
            temp_known_index_1=known_index_1[0:len(unknown_index_1)]            # slice known index 1
            if unknown_index_2 and known_index_2:
              temp_known_index_2=known_index_2[0:len(unknown_index_2)] \
                                 if len(unknown_index_2) < len(known_index_2) \
                                 else  known_index_2                            # slice known index 2, if its shorter too
              if unknown_index_1==temp_known_index_1 and \
                 unknown_index_2==temp_known_index_2:                           # both indexes are dual index
                tag='mix_index_match'
            else:
              if unknown_index_1==temp_known_index_1:                           # one of the index is single index
                tag='mix_index_match'
            
          # CASE 3: unknown index is 8 or 8+8 and known_index is 6 or 6+6
          elif len(unknown_index_1) > len(known_index_1):
            temp_unknown_index_1=unknown_index_1[0:len(known_index_1)]          # slice unknown index 1
            if unknown_index_2 and known_index_2:
              temp_unknown_index_2=unknown_index_2[0:len(known_index_2)]  \
                                   if len(unknown_index_2) > len(known_index_2)\
                                   else  unknown_index_2                        # slice unknown index 2, , if its shorter too
              if temp_unknown_index_1==known_index_1 and \
                 temp_unknown_index_2==known_index_2:                           # both indexes are dual index
                tag='mix_index_match'
            else:
              if temp_unknown_index_1==known_index_1:                           # one index is single index
                tag='mix_index_match'
                
          # CASE 4: index 1 or index 1 and index 2 are both reverse complement
          elif rev_comp(unknown_index_1) == known_index_1:
            tag='index_1_revcomp'                                               # add initial tag if index 1 is revcomp
            if unknown_index_2 and known_index_2 and \
               unknown_index_2 == known_index_2:
              tag='only_index_1_revcomp'                                        # modify tag if index 2 is exact match
            elif unknown_index_2 and known_index_2 and \
                 rev_comp(unknown_index_2) == known_index_2:
              tag='index_1_and_index_2_revcomp'                                 # modify tag if both reads are revcomp
          
          # CASE 5: index 1 is exact match but index 2 is reverse complement
          elif unknown_index_2 and known_index_2 and \
               rev_comp(unknown_index_2) == known_index_2 and \
               unknown_index_1==known_index_1:
            tag='only_index_2_revcomp'                                          # only index 2 is revcomp
            
      data_series['tag']=tag                                                    # reset tag in data series
      return data_series
    except:
      raise
    
    
  def _generate_barcode_plots(self,work_dir,all_barcode_plot='total_barcodes.png',\
                              index_plot='individual_barcodes.png',save_plot=True):
    '''
    An internal method for plotting barcode stats
    required params:
    work_dir: A path to the work dir
    all_barcode_plot: A file for plotting all stats, default total_barcodes.png
    index_plot: A file for plotting the individual barcodes, default individual_barcodes.png
    '''
    try:
      summary_df=self.processed_df
      raw_df=self.raw_df
      summary_df_temp=summary_df.set_index('id')
      fig, ax=plt.subplots()
      summary_df_temp[['known_pct','unknown_pct']].plot(ax=ax,kind='bar', \
                                                        color=['green','orange'],\
                                                        stacked=True)
      plt.xlabel('Sequencing lane id')
      plt.ylabel('% of reads')
      all_barcode_plot=os.path.join(work_dir,all_barcode_plot)
      
      if save_plot:
        if os.path.exists(all_barcode_plot):
          os.remove(all_barcode_plot)
        plt.savefig(all_barcode_plot)
        
      raw_df_tmp=raw_df.set_index('index')
      fig, ax=plt.subplots()                                                    # plotting total barcode stats
      for gk,gr in raw_df_tmp.groupby('tag'):
        if gk=='known':
          ax.scatter(x=gr['log_total_read'],y=gr['mapping_ratio'],color='green')
        elif gk=='unknown':
          ax.scatter(x=gr['log_total_read'],y=gr['mapping_ratio'],color='orange')
       
        
      plt.xlabel('Log of total reads')
      plt.ylabel('read % for index barcode')
      index_plot=os.path.join(work_dir,index_plot)
      if save_plot:
        if os.path.exists(index_plot):
          os.remove(index_plot)
        plt.savefig(index_plot)                                                 # plotting individual barcode stats
        return (all_barcode_plot,index_plot)
      else:
        plt.show()
    except:
      raise