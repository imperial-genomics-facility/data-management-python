import os,base64
import scanpy as sc
import numpy as np
import pandas as pd
from copy import deepcopy
from datetime import datetime
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
from igf_data.utils.fileutils import get_temp_dir,copy_local_file,remove_dir
from jinja2 import Template,Environment, FileSystemLoader,select_autoescape

class Scanpy_tool:
  '''
  A class for running scanpy tool and generating a html report for the input data
  
  FIX ME: Replace with a notebook
  
  Reference notebooks
  v3
  https://scanpy-tutorials.readthedocs.io/en/latest/pbmc3k.html

  V2
  https://github.com/theislab/scanpy_usage/blob/master/170505_seurat/seurat.ipynb
  https://github.com/theislab/scanpy_usage/blob/master/170503_zheng17/zheng17.ipynb

  :param project_name: A project name string for the report page
  :param sample_name: A sample name string for the report page
  :param matrix_file: A matrix.mtx.gz file from cellranger count output
  :param features_tsv: A features.tsv.gz file from cellranger count output
  :param barcode_tsv: A barcodes.tsv.gz file from cellranger count output
  :param output_file: An output file path
  :param html_template_file: A html template for writing the report
  :param species_name: A species name for MT-genes lookup
  :param min_gene_count: Minimum gene count for data filtering, default 200
  :param min_cell_count: Minimum cell count for data filtering, default 3
  :param force_overwrite: A toggle for replacing existing output file, default True
  :param cellbrowser_h5ad: Path for h5ad output for cellbrowser, default None
  :param use_ephemeral_space: A toggle for temp dir settings, default 0
  '''
  def __init__(self,project_name,sample_name,matrix_file,features_tsv,barcode_tsv,
               output_file,html_template_file,species_name,min_gene_count=200,
               min_cell_count=3,force_overwrite=True,cellbrowser_h5ad=None,
               use_ephemeral_space=0):
    self.project_name = project_name
    self.sample_name = sample_name
    self.matrix_file = matrix_file
    self.features_tsv = features_tsv
    self.barcode_tsv = barcode_tsv
    self.output_file = output_file
    self.html_template_file = html_template_file
    self.use_ephemeral_space = use_ephemeral_space
    self.work_dir = get_temp_dir(use_ephemeral_space=self.use_ephemeral_space)
    self.min_gene_count = min_gene_count
    self.min_cell_count = min_cell_count
    self.force_overwrite = force_overwrite
    self.cellbrowser_h5ad = cellbrowser_h5ad

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

    :param generate_cb_data: A toggle for generating cellbrowser data, default False
    :param cb_data_path: A output path for cellbrowser data, default None
    '''
    try:
      os.chdir(self.work_dir)
      if os.path.exists(os.path.join(self.work_dir,'cache')):
        remove_dir(os.path.join(self.work_dir,'cache'))

      date_stamp = datetime.now().strftime('%d-%b-%Y %H:%M:%S')

      # step 1: read input files
      temp_input_dir = \
        get_temp_dir(use_ephemeral_space=self.use_ephemeral_space)              # fix for hpc
      local_matrix_file = \
        os.path.join(\
          temp_input_dir,
          os.path.basename(self.matrix_file))
      local_barcode_tsv = \
        os.path.join(\
          temp_input_dir,
          os.path.basename(self.barcode_tsv))
      local_features_tsv = \
        os.path.join(\
          temp_input_dir,
          os.path.basename(self.features_tsv))
      copy_local_file(\
        source_path=self.matrix_file,
        destination_path=local_matrix_file)
      copy_local_file(\
        source_path=self.barcode_tsv,
        destination_path=local_barcode_tsv)
      copy_local_file(\
        source_path=self.features_tsv,
        destination_path=local_features_tsv)
      adata = sc.read_10x_mtx(\
                temp_input_dir,
                var_names='gene_symbols',
                cache=True)                                                     # read input files
      adata.var_names_make_unique()
      sc.pl.highest_expr_genes(\
        adata,
        n_top=30,
        save='.png')                                                            # list of genes that yield the highest fraction of counts in each single cells, across all cells
      highest_gene_expr = \
        self._encode_png_image(\
          png_file=\
            os.path.join(\
              self.work_dir,
              'figures/highest_expr_genes.png'))                                # encode highest gene expr data
      # step 2: filter data based on cell and genes
      sc.pp.filter_cells(\
        adata,
        min_genes=self.min_gene_count)
      sc.pp.filter_genes(\
        adata,
        min_cells=self.min_cell_count)
      # step 3: fetch mitochondrial genes
      mt_genes = self._fetch_mitochondrial_genes(species_name='hsapiens')
      mt_genes = [name
                   for name in adata.var_names 
                     if name in mt_genes]                                       # filter mito genes which are not present in data
      # step 4: calculate mitochondrial read percentage
      adata.obs['percent_mito'] = \
        np.sum(adata[:, mt_genes].X, axis=1).A1 / np.sum(adata.X, axis=1).A1
      adata.obs['n_counts'] = adata.X.sum(axis=1).A1                            # add the total counts per cell as observations-annotation to adata
      sc.pl.violin(\
        adata,
        ['n_genes', 'n_counts', 'percent_mito'],
        jitter=0.4,
        multi_panel=True,
        show=True,
        save='.png')                                                            # violin plot of the computed quality measures /figures/violin.png
      mito_plot_data = \
        self._encode_png_image(\
          png_file=\
            os.path.join(\
              self.work_dir,\
                'figures/violin.png'))
      sc.pl.scatter(\
        adata,
        x='n_counts',
        y='percent_mito',
        show=True,
        save='.png')                                                            # scatter plots for data quality 1
      mito_plot_scatter1 = \
        self._encode_png_image(\
          png_file=os.path.join(\
            self.work_dir,
            'figures/scatter.png'))
      sc.pl.scatter(\
        adata,
        x='n_counts',
        y='n_genes',
        save='.png')                                                            # scatter plots for data quality 2
      mito_plot_scatter2 = \
        self._encode_png_image(\
          png_file=\
            os.path.join(\
              self.work_dir,
              'figures/scatter.png'))
      # step 5: Filtering data bases on percent mito
      adata = adata[adata.obs['n_genes']<2500,:]
      adata = adata[adata.obs['percent_mito'] < 0.05, :]
      # step 6: Normalise and filter data
      sc.pp.normalize_per_cell(adata)                                           # Total-count normalize (library-size correct) the data matrix to 10,000 reads per cell, so that counts become comparable among cells.
      sc.pp.log1p(adata)
      adata.raw = adata
      sc.pp.highly_variable_genes(\
        adata,
        min_mean=0.0125,
        max_mean=3,
        min_disp=0.5)                                                           # Identify highly-variable genes
      sc.pl.highly_variable_genes(adata,save='.png')
      genes_dispersion_data = \
        self._encode_png_image(\
          png_file=\
            os.path.join(\
              self.work_dir,
              'figures/filter_genes_dispersion.png'))                           # plot highly-variable genes
      adata = adata[:, adata.var['highly_variable']]                            # filter highly-variable genes
      # step 7: Analyze data
      sc.pp.regress_out(\
        adata,
        ['n_counts', 'percent_mito'])                                           # regress out effects of total counts per cell and the percentage of mitochondrial genes expressed
      sc.pp.scale(\
        adata,
        max_value=10)                                                           # scale the data to unit variance
      sc.tl.pca(\
        adata,
        svd_solver='arpack')                                                    # run pca
      sc.pl.pca_loadings(\
        adata,
        show=True,
        save='.png')                                                            # plot pca loading graph
      pca_data = \
        self._encode_png_image(\
          png_file=\
            os.path.join(\
              self.work_dir,
              'figures/pca_loadings.png'))                                      # load pca loading graph
      sc.pl.pca_variance_ratio(\
        adata,
        log=True,save='.png')                                                   # save pca variation ratio
      pca_var_data = \
        self._encode_png_image(\
          png_file=\
            os.path.join(\
              self.work_dir,
              'figures/pca_variance_ratio.png'))                                # load pca variation graph
      sc.tl.tsne(\
        adata,
        random_state=2,
        n_pcs=10)                                                               # legacy tsne
      sc.pp.neighbors(\
        adata,
        n_neighbors=10,
        n_pcs=40)                                                               # neighborhood graph
      # step 7.5 Plot 3D UMAP
      sc.tl.umap(\
        adata,
        n_components=3)                                                         # generate UMAP with 3PCs
      sc.tl.louvain(adata)                                                      # louvain graph clustering
      dict_map = { \
        '0':'#4682B4',
        '1':'#A233A2',
        '2':'#FF7F50',
        '3':'#6787E7',
        '4':'#B75555',
        '5':'#2E8B57',
        '6':'#191970',
        '7':'#DB7093',
        '8':'#90EE90',
        '9':'#00FFFF',
        '10':'#FFD700',
        '11':'#DC143C',
        '12':'#B0C4DE',
        '13':'#00FA9A',
        '14':'#FA8072',
        '15':'#FFF0F5',
        '16':'#DB7093'
      }
      louvain_series = deepcopy(adata.obs['louvain'])
      color_map = louvain_series.map(dict_map).values
      labels = list(adata.obs.index)
      hovertext = \
        ['cluster: {0}, barcode: {1}'.\
         format(grp,labels[index])
           for index,grp in enumerate(louvain_series.values)]
      threeDUmapDiv = \
        plot([go.Scatter3d( \
                x=adata.obsm['X_umap'][:, 0],
                y=adata.obsm['X_umap'][:, 1],
                z=adata.obsm['X_umap'][:, 2], 
                mode = 'markers',
                marker = dict(color = color_map,
                              size = 5),
                opacity=0.6,
                text=labels,
                hovertext=hovertext,
             )],
             output_type='div',
             include_plotlyjs='cdn')                                            # capture 3d div for umap plot
      sc.tl.umap(adata,n_components=2)                                          # umap with 2PCs or original adata
      sc.pl.tsne(\
        adata,
        color='louvain',
        show=True,
        save='.png')                                                            # plot tSNE data
      tsne_data = \
        self._encode_png_image(\
          png_file=\
            os.path.join(\
              self.work_dir,
              'figures/tsne.png'))                                              # load t-SNE
      # step 8: Finding marker genes
      sc.pl.umap(\
        adata,
        color=['louvain'],
        save='.png')                                                            # plot umap
      umap_data = \
        self._encode_png_image(\
          png_file=\
            os.path.join(\
              self.work_dir,
              'figures/umap.png'))                                              # load umap
      sc.tl.rank_genes_groups(\
        adata,
        'louvain',
        method='t-test')                                                        # compute a ranking for the highly differential genes in each cluster
      sc.pl.rank_genes_groups(\
        adata,
        n_genes=20,
        show=True,
        sharey=False,
        save='.png')                                                            # plot diff genes in each clusters
      rank_genes_groups_data = \
        self._encode_png_image(\
          png_file=\
            os.path.join(\
              self.work_dir,
              'figures/rank_genes_groups_louvain.png'))                         # load ranking plot
      sc.pl.rank_genes_groups_stacked_violin(\
        adata,
        n_genes=10,
        save='.png')                                                            # ranked genes group stacked violin plot
      rank_genes_groups_stacked_violin = \
        self._encode_png_image(\
          png_file=\
            os.path.join(\
              self.work_dir,
              'figures/stacked_violin.png'))                                    # load stacked violin plot data
      sc.pl.rank_genes_groups_dotplot(\
        adata,
        n_genes=10,
        color_map='bwr',
        dendrogram='dendrogram_louvain',
        save='.png')                                                            # ranked genes group dot plot
      rank_genes_groups_dotplot = \
        self._encode_png_image(\
          png_file=\
            os.path.join(\
              self.work_dir,
              'figures/dotplot.png'))                                           # load dotplot
      sc.pl.rank_genes_groups_matrixplot(\
        adata,
        n_genes=10,
        save='.png')                                                            # ranked genes group matrix plot
      rank_genes_groups_matrixplot = \
        self._encode_png_image(\
          png_file=\
            os.path.join(\
              self.work_dir,
              'figures/matrixplot.png'))                                        # load matrix plot
      sc.pl.rank_genes_groups_heatmap(\
        adata,
        n_genes=10,
        show_gene_labels=True,
        save='.png')                                                            # ranked gene heatmap plot
      rank_genes_groups_heatmap = \
        self._encode_png_image(\
          png_file=\
            os.path.join(\
              self.work_dir,
              'figures/heatmap.png'))                                           # load heatmap plot
      sc.pl.rank_genes_groups_tracksplot(\
        adata,
        n_genes=10,
        cmap='bwr',
        save='.png')                                                            # ranked gene tracks plot
      rank_genes_groups_tracksplot = \
        self._encode_png_image(\
          png_file=\
            os.path.join(\
              self.work_dir,
              'figures/tracksplot.png'))                                        # load tracks plot

      project_name = self.project_name
      project_name = \
        project_name[0] \
          if isinstance(project_name, tuple) \
            else project_name                                                   # check for project_name object
      template_env = \
        Environment(\
          loader=FileSystemLoader(\
            searchpath=os.path.dirname(self.html_template_file)),
            autoescape=select_autoescape(['xml']))
      template_file = \
        template_env.\
          get_template(\
            os.path.basename(self.html_template_file))
      template_file.\
        stream(\
          ProjectName=project_name,
          SampleName=self.sample_name,
          Date_stamp=date_stamp,
          Highest_gene_expr=highest_gene_expr,
          MitoPlot=mito_plot_data,
          MitoScatter1=mito_plot_scatter1,
          MitoScatter2=mito_plot_scatter2,
          GenesDispersion=genes_dispersion_data,
          Pca=pca_data,
          Pca_var_data=pca_var_data,
          Tsne=tsne_data,
          Umap3DDiv=threeDUmapDiv,
          Umap_data=umap_data,
          RankGenesGroups=rank_genes_groups_data,
          Rank_genes_groups_stacked_violin=rank_genes_groups_stacked_violin,
          Rank_genes_groups_dotplot=rank_genes_groups_dotplot,
          Rank_genes_groups_matrixplot=rank_genes_groups_matrixplot,
          Rank_genes_groups_heatmap=rank_genes_groups_heatmap,
          Rank_genes_groups_tracksplot=rank_genes_groups_tracksplot).\
        dump(os.path.join(self.work_dir,'test.html'))
      copy_local_file(\
        os.path.join(\
          self.work_dir,'test.html'),
          self.output_file,
          force=self.force_overwrite)
      if self.cellbrowser_h5ad is not None:
        try:
          if not os.path.exists(os.path.dirname(self.cellbrowser_h5ad)):
            os.makedirs(os.path.dirname(self.cellbrowser_h5ad))

          temp_h5ad = \
            os.path.join(\
              self.work_dir,
              os.path.basename(self.cellbrowser_h5ad))
          adata.write_h5ad(filename=temp_h5ad)
          copy_local_file(\
            source_path=temp_h5ad,
            destination_path=self.cellbrowser_h5ad,
            force=True)
        except Exception as e:
          raise ValueError('Failed to export Scanpy h5ad, error: {0}'.\
                           format(e))


      remove_dir(temp_input_dir)
      remove_dir(self.work_dir)
    except:
      raise