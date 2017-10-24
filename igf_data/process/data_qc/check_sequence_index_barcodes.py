import os, json, math
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from igf_data.illumina.samplesheet import SampleSheet


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

  
  def _check_barcode_stats(self):
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
        all_lanes=samplesheet_data.get_lane_count()
        if lid in all_lanes:
          if platform_name=='NEXTSEQ':
            samplesheet_data.add_pseudo_lane_for_nextseq()                      # add pseudo lane info for NextSeq
            samplesheet_data.filter_sample_data(condition_key='PseudoLane', \
                                                condition_value=lid)            # filter samplesheet for the Pseudolane dynamically
          else:
            samplesheet_data.filter_sample_data(condition_key='Lane', \
                                                condition_value=lid)            # filter samplesheet for the lane dynamically
          all_known_indexes=samplesheet_data.get_indexes()                      # get all known indexes present on the lane
          u_stats_df=u_stats_df[u_stats_df['index'].\
                                isin(all_known_indexes)==False]                 # filter all known barcodes from unknown
        
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
      
    processed_df=processed_df.apply(lambda x: self._generate_pct(x), axis=1)         # calculate % of known and unknown barcodes
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
          (all_barcode_plot,index_plot)=self._generate_barcode_plots            # plot stats
          raise IndexBarcodeValidationError(message='{0} failed total barcode check: {1}'.\
                                            format(runid, grp['known_pct'].values[0]), \
                                            plots=[all_barcode_plot,index_plot])
      # check for individual barcodes mapping ratios
      for rid, rgrp in raw_df.groupby('runid'):
        for lid,lgrp in rgrp.groupby('lane'):
          known_grp=lgrp.groupby('tag').get_group('known')
          min_known_mpr=known_grp['mapping_ratio'].min()
          unknown_grp=lgrp.groupby('tag').get_group('unknown')
          max_unknown_mpr=unknown_grp['mapping_ratio'].max()
          if strict_check and min_known_mpr < max_unknown_mpr:
            (all_barcode_plot,index_plot)=self._generate_barcode_plots          # plot stats
            raise IndexBarcodeValidationError(message='{0} {1} failed mapping ratio check'.\
                                              format(rid, lid), \
                                              plots=[all_barcode_plot,index_plot])
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
      plt.savefig(all_barcode_plot)
      raw_df_tmp=raw_df.set_index('index')
      fig, ax=plt.subplots()                                                    # plotting total barcode stats
      for gk,gr in raw_df_tmp.groupby('tag'):
        if gk=='known':
          ax.scatter(x=gr['log_total_read'],y=gr['mapping_ratio'],color='green')
        elif gk=='unknown':
          ax.scatter(x=gr['log_total_read'],y=gr['mapping_ratio'],color='orange')
        else:
          raise ValueError('tag {0} is undefined'.format(gk))
        
      plt.xlabel('Log of total reads')
      plt.ylabel('read % for index barcode')
      index_plot=os.path.join(work_dir,index_plot)
      if save_plot:
        plt.savefig(index_plot)                                                 # plotting individual barcode stats
        return (all_barcode_plot,index_plot)
      else:
        plt.show()
    except:
      raise