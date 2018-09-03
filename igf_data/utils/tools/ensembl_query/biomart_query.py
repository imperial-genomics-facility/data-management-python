import pandas as pd
from bioservices import biomart

def fetch_rRNA_genes_from_biomart(org,host='www.ensembl.org'):
  '''
  '''
  try:
    if org not in ['hsapiens',
                   'mmusculus']:
      raise ValueError('Organism {0} not supported for biomart lookup'.\
                       format(org))
    s=biomart.BioMart(host=host)
    s.new_query()
    if org=='hsapiens':
      s.add_dataset_to_xml('hsapiens_gene_ensembl')
      s.add_attribute_to_xml('hgnc_symbol')                                     # fetch human gene names
    elif org=='mmusculus':
      s.add_dataset_to_xml('mmusculus_gene_ensembl')
      s.add_attribute_to_xml('mgi_symbol')                                      # fetch mouse gene names
    else:
      raise ValueError('Organism {0} not supported for biomart lookup'.\
                         format(org))
    s.add_filter_to_xml("biotype", "rRNA")                                      # filter query for rRNA genes
    s.add_attribute_to_xml('chromosome_name')                                   # fetch chr names
    xml=s.get_xml()                                                             # fetch xml
    res=pd.read_csv(StringIO(s.query(xml)), sep='\t', header=None)
    res.columns=['symbol','chromosome_name']                                    # reformat dataframe
    res.set_index('symbol',inplace=True)
    res=res[~res.index.duplicated(keep='first')]
    return res.index
  except:
    raise