import os,base64
import scanpy.api as sc
import pandas as pd
from igf_data.utils.fileutils import get_temp_dir,copy_local_file,remove_dir
from jinja2 import Template,Environment, FileSystemLoader,select_autoescape

class Scanpy_tool:
  '''
  A class for running scanpy tool and generating a html report for the input data
  
  :param project_name: A project name string for the report page
  :param sample_name: A sample name string for the report page
  :param matrix_file: A matrix file from cellranger count output
  :param gene_tsv: A genes.tsv file from cellranger count output
  :param barcode_tsv: A barcodes.tsv file from cellranger count output
  :param output_file: An output file path
  :param html_template_file: A html template for writing the report
  :param species_name: A species name for MT-genes lookup
  :param min_gene_count: Minimum gene count for data filtering, default 200
  :param min_cell_count: Minimum cell count for data filtering, default 3
  :param:
  :param:
  '''
  def __init__(self,project_name,sample_name,matrix_file,gene_tsv,barcode_tsv,
               output_file,html_template_file,species_name,min_gene_count=200,
               min_cell_count=3):
    self.project_name=project_name,
    self.sample_name=sample_name
    self.matrix_file=matrix_file
    self.gene_tsv=gene_tsv
    self.barcode_tsv=barcode_tsv
    self.output_file=output_file
    self.html_template_file=html_template_file
    self.work_dir=get_temp_dir()
    self.min_gene_count=min_gene_count
    self.min_cell_count=min_cell_count

  @staticmethod
  def _fetch_mitochondrial_genes(species_name,url='www.ensembl.org'):
    '''
    A static method for fetching mitochondrial genes from Ensembl
    
    :param species_name: A string for species name
    :param url: A url string, default 'www.ensembl.org'
    :returns: A list of mitochondial gene names
    '''
    try:
      if species_name not in ['hsapiens','mmusculus']:
        raise ValueError('Species {0} not supported'.format(species_name))
        
      mito_genes=sc.queries.mitochondrial_genes(url,species_name)
      return mito_genes
    except:
      raise

  @staticmethod
  def _encode_png_image(png_file):
    try:
      if not os.path.exists(png_file):
        raise ValueError('File not present')
    
      encoded=base64.b64encode(open(png_file, "rb").read()).decode()
      return encoded
    except:
        raise
        
  def generate_report(self):
    '''
    A method for generating html report from scanpy analysis
    '''
    try:
      os.chdir(self.work_dir)
      if os.path.exists(os.path.join(self.work_dir,'cache')):
        remove_dir(os.path.join(self.work_dir,'cache'))

      # step 1: read cellranger output matrix
      adata=sc.read(self.matrix_file,
                    cache=True).T
      adata.var_names = pd.read_csv(self.gene_tsv,
                                    header=None,
                                    sep='\t')[1]
      adata.obs_names = pd.read_csv(self.barcode_tsv,
                                    header=None)[0]
      adata.var_names_make_unique()

      # step 2: filter data based on cell and genes
      sc.pp.filter_cells(adata,
                         min_genes=self.min_gene_count)
      sc.pp.filter_genes(adata,
                         min_cells=self.min_cell_count)

      # step 3: fetch mitochondrial genes
      mt_genes=self._fetch_mitochondrial_genes(species_name='hsapiens')
      mt_genes=[name for name in adata.var_names if name in mt_genes]           # filter mito genes which are not present in data

      # step 4: calculate mitochondrial read percentage
      adata.obs['percent_mito']=np.sum(adata[:, mt_genes].X, axis=1).A1 / np.sum(adata.X, axis=1).A1
      adata.obs['n_counts']=adata.X.sum(axis=1).A1                              # add the total counts per cell as observations-annotation to adata
      sc.pl.violin(adata,
                   ['n_genes', 'n_counts', 'percent_mito'],
                   jitter=0.4,
                   multi_panel=True,
                   save='.png')                                                 # violin plot of the computed quality measures /figures/violin.png
      violin_plot=os.path.join(self.work_dir,'figures/violin.png')
      mito_plot_data=self._encode_png_image(png_file=violin_plot)
      sc.pl.scatter(adata,
                    x='n_counts',
                    y='percent_mito',
                    save='.png')                                                # scatter plots for data quality 1
      mt_scatter_plot1=os.path.join(self.work_dir,
                                    'figures/scatter.png')
      mito_plot_scatter1=self._encode_png_image(png_file=mt_scatter_plot1)
      sc.pl.scatter(adata,
                    x='n_counts',
                    y='n_genes',
                    save='.png')                                                # scatter plots for data quality 2
      mt_scatter_plot2=os.path.join(self.work_dir,
                                    'figures/scatter.png')
      mito_plot_scatter2=self._encode_png_image(png_file=mt_scatter_plot2)

      # step 5: Filtering data bases on percent mito
      adata=adata[adata.obs['n_genes']<2000,:]
      adata=adata[adata.obs['percent_mito'] < 0.05, :]
      adata.raw=sc.pp.log1p(adata, copy=True)                                   # copy raw data

      # step 6: Normalise and filter data
      sc.pp.normalize_per_cell(adata)
      filter_result=sc.pp.filter_genes_dispersion(adata.X,
                                                  flavor='cell_ranger',
                                                  n_top_genes=1000 )            # identify highly variable genes
      sc.pl.filter_genes_dispersion(filter_result,save='.png')                  # plot variable genes
      genes_dispersion_file=os.path.join(self.work_dir,
                                         'figures/filter_genes_dispersion.png')
      genes_dispersion_data=self._encode_png_image(png_file=genes_dispersion_file)
      adata=adata[:, filter_result.gene_subset]                                 # replace data with filtered data

      # step 7: Analyze data
      sc.pp.log1p(adata)                                                        # logarithmize the data
      sc.pp.regress_out(adata, ['n_counts', 'percent_mito'])                    # regress out effects of total counts per cell and the percentage of mitochondrial genes expressed
      sc.pp.scale(adata, max_value=10)                                          # scale the data to unit variance
      sc.tl.pca(adata)                                                          # pca
      sc.pl.pca_loadings(adata,save='.png')                                     # plot pca loading graph
      pca_file=os.path.join(self.work_dir,
                            'figures/pca_loadings.png')
      pca_data=self._encode_png_image(png_file=pca_file)
      sc.tl.tsne(adata,
                 random_state=2,
                 n_pcs=10)                                                      # tsne
      sc.pp.neighbors(adata, n_neighbors=10)                                    # neighborhood graph
      sc.tl.louvain(adata)                                                      # louvain graph clustering
      sc.pl.tsne(adata,
                 color='louvain',
                 save='.png')                            # plot tSNE data
      tsne_file=os.path.join(self.work_dir,
                             'figures/tsne.png')
      tsne_data=self._encode_png_image(png_file=tsne_file)

      # step 8: Finding marker genes
      sc.tl.rank_genes_groups(adata, 'louvain')
      sc.pl.rank_genes_groups(adata,
                              n_genes=20,
                              save='.png')                                      # plot rank gene
      rank_genes_groups_file=os.path.join(self.work_dir,
                                          'figures/rank_genes_groups_louvain.png')
      rank_genes_groups_data=self._encode_png_image(png_file=rank_genes_groups_file)
      result = adata.uns['rank_genes_groups']
      groups = result['names'].dtype.names
      gene_score=pd.DataFrame({group + '_' + key: result[key][group]
                               for group in groups 
                                 for key in ['names', 'scores']})               # table data
      marker_gene_violin_data=list()
      for group in result['names'].dtype.names:
        key='{0}_names'.format(group)
        genes=gene_score[key].head(10).to_dict().values()
        sc.pl.violin(adata,
                     genes,
                     groupby='louvain',
                     save='.png',
                     multi_panel=True,
                     scale='width',
                     multi_panel_figsize=[8.0,16.0])                            # violin plot for marker genes
        violin_plot=os.path.join(self.work_dir,
                                 'figures/violin.png')
        violin_plot_data=self._encode_png_image(png_file=violin_plot)
        marker_gene_violin_data.append({'cluster':group,
                                        'violin_data':violin_plot_data})
        
      template_env=Environment(\
                     loader=FileSystemLoader(\
                              searchpath=os.path.dirname(self.html_template_file)), \
                     autoescape=select_autoescape(['xml']))
      template_file=template_env.get_template(os.path.basename(self.html_template_file))
      template_file.\
        stream(ProjectName=self.project_name,
               SampleName=self.sample_name,
               MitoPlot=mito_plot_data,
               MitoScatter1=mito_plot_scatter1,
               MitoScatter2=mito_plot_scatter2,
               GenesDispersion=genes_dispersion_data,
               Pca=pca_data,
               Tsne=tsne_data,
               RankGenesGroups=rank_genes_groups_data,
               MarkerGeneViolin=marker_gene_violin_data,
              ).\
        dump(os.path.join(self.work_dir,'test.html'))
      copy_local_file(os.path.join(self.work_dir,'test.html'),
                      self.output_file)
    except:
      raise