from igf_data.igfdb.collectionadaptor import CollectionAdaptor

class reference_genome_utils:
  '''
  A class for accessing different components of the reference genome for a specific build
  '''
  def __init__(self,genome_tag,dbsession_class,
               genome_fasta_type='GENOME_FASTA',genome_dict_type='GENOME_DICT',
               gene_gtf_type='GENE_GTF',gene_reflat_type='GENE_REFFLAT',
               bwa_ref_type='GENOME_BWA',minimap2_ref_type='GENOME_MINIMAP2',
               bowtie2_ref_type='GENOME_BOWTIE2',
               tenx_ref_type='TRANSCRIPTOME_TENX',
               star_ref_type='TRANSCRIPTOME_STAR',
               genome_dbsnp_type='DBSNP_VCF',
               gatk_snp_ref_type='GATK_SNP_REF',
               gatk_indel_ref_type='GATK_INDEL_REF'):
    '''
    :param genome_tag: Collection name of the reference genome file
    :param dbsession_class: A sqlalchemy session class for database connection
    :param genome_fasta_type: Collection type for genome fasta file, default GENOME_FASTA
    :param genome_dict_type: Collection type for genome dict file, default GENOME_DICT
    :param gene_gtf_type: Collection type for gene gtf file, default GENE_GTF
    :param gene_reflat_type: Collection type for gene refflat file, default GENE_REFFLAT
    :param bwa_ref_type: Collection type for the BWA reference genome, default GENOME_BWA
    :param minimap2_ref_type: Collection type for the Minimap2 reference genome, default GENOME_MINIMAP2
    :param bowtie2_ref_type: Collection type for the Bowtie2 reference genome, default GENOME_BOWTIE2
    :param tenx_ref_type: Collection type for the 10X Cellranger reference genome, default TRANSCRIPTOME_TENX
    :param star_ref_type: Collection type for the STAR reference genome, default TRANSCRIPTOME_STAR
    :param gatk_snp_ref_type: Collection type for the GATK SNP reference bundle files, default GATK_SNP_REF
    :param gatk_indel_ref_type: Collection type for the GATK INDEL reference bundle files, default gatk_indel_ref_type
    :param genome_dbsnp_type: Collection type for the dbSNP vcf file, default DBSNP_VCF
    '''
    self.genome_tag=genome_tag
    self.dbsession_class=dbsession_class
    self.genome_fasta_type=genome_fasta_type
    self.genome_dict_type=genome_dict_type
    self.gene_gtf_type=gene_gtf_type
    self.gene_reflat_type=gene_reflat_type
    self.bwa_ref_type=bwa_ref_type
    self.minimap2_ref_type=minimap2_ref_type
    self.bowtie2_ref_type=bowtie2_ref_type
    self.tenx_ref_type=tenx_ref_type
    self.star_ref_type=star_ref_type
    self.gatk_snp_ref_type=gatk_snp_ref_type
    self.gatk_indel_ref_type=gatk_indel_ref_type
    self.genome_dbsnp_type=genome_dbsnp_type


  def _fetch_collection_files(self,collection_type,check_missing=False,
                              unique_file=True,file_path_label='file_path'):
    '''
    An internal method for fetching collection group files from database
    
    :param collection_type: Collection type information for database lookup
    :param check_missing: A toggle for checking errors for missing files, default False
    :param unique_file: A toggle for keeping only a single collection file, default True
    :param file_path_label: Name of the file_path column in the File table, default file_path
    :returns: A single file if unique_file is true, else a list of files
    '''
    try:
      ref_file=None
      ca=CollectionAdaptor(**{'session_class':self.dbsession_class})
      ca.start_session()
      collection_files=ca.get_collection_files(collection_name=self.genome_tag,
                                               collection_type=collection_type,
                                               output_mode='dataframe')         # fetch collection files from db
      ca.close_session()
      if len(collection_files.index) >0:
        files=list(collection_files[file_path_label].values())
        if unique_file:
          ref_file=files[0]                                                     # select the first file from db results
        else:
          ref_file=files

      if ref_file is None and check_missing:
        raise ValueError('No file collection found for reference genome {0}:{1}'.\
                         format(self.genome_tag,collection_type))
      return ref_file
    except:
      raise

  def get_genome_fasta(self,check_missing=True):
    '''
    A method for fetching reference genome fasta filepath for a specific genome build
    
    :param check_missing: A toggle for checking errors for missing files, default True
    :returns: A filepath string
    '''
    try:
      ref_file=self._fetch_collection_files(collection_type=self.genome_fasta_type,
                                            check_missing=check_missing)
      return  ref_file
    except:
      raise

  def get_genome_dict(self,check_missing=True):
    '''
    A method for fetching reference genome dictionary filepath for a specific genome build
    
    :param check_missing: A toggle for checking errors for missing files, default True
    :returns: A filepath string
    '''
    try:
      ref_file=self._fetch_collection_files(collection_type=self.genome_dict_type,
                                            check_missing=check_missing)
      return  ref_file
    except:
      raise

  def get_gene_gtf(self,check_missing=True):
    '''
    A method for fetching reference gene annotation gtf filepath for a specific genome build
    
    :param check_missing: A toggle for checking errors for missing files, default True
    :returns: A filepath string
    '''
    try:
      ref_file=self._fetch_collection_files(collection_type=self.gene_gtf_type,
                                            check_missing=check_missing)
      return  ref_file
    except:
      raise

  def get_gene_reflat(self,check_missing=True):
    '''
    A method for fetching reference gene annotation refflat filepath for a specific genome build
    
    :param check_missing: A toggle for checking errors for missing files, default True
    :returns: A filepath string
    '''
    try:
      ref_file=self._fetch_collection_files(collection_type=self.gene_reflat_type,
                                            check_missing=check_missing)
      return  ref_file
    except:
      raise

  def get_genome_bwa(self,check_missing=True):
    '''
    A method for fetching filepath of BWA reference index, for a specific genome build
    
    :param check_missing: A toggle for checking errors for missing files, default True
    :returns: A filepath string
    '''
    try:
      ref_file=self._fetch_collection_files(collection_type=self.bwa_ref_type,
                                            check_missing=check_missing)
      return  ref_file
    except:
      raise

  def get_genome_minimap2(self,check_missing=True):
    '''
    A method for fetching filepath of Minimap2 reference index, for a specific genome build
    
    :param check_missing: A toggle for checking errors for missing files, default True
    :returns: A filepath string
    '''
    try:
      ref_file=self._fetch_collection_files(collection_type=self.minimap2_ref_type,
                                            check_missing=check_missing)
      return  ref_file
    except:
      raise

  def get_genome_bowtie2(self,check_missing=True):
    '''
    A method for fetching filepath of Bowtie2 reference index, for a specific genome build
    
    :param check_missing: A toggle for checking errors for missing files, default True
    :returns: A filepath string
    '''
    try:
      ref_file=self._fetch_collection_files(collection_type=self.bowtie2_ref_type,
                                            check_missing=check_missing)
      return  ref_file
    except:
      raise

  def get_transcriptome_tenx(self,check_missing=True):
    '''
    A method for fetching filepath of 10X Cellranger reference transcriptome, for a specific genome build
    
    :param check_missing: A toggle for checking errors for missing files, default True
    :returns: A filepath string
    '''
    try:
      ref_file=self._fetch_collection_files(collection_type=self.tenx_ref_type,
                                            check_missing=check_missing)
      return  ref_file
    except:
      raise

  def get_transcriptome_star(self,check_missing=True):
    '''
    A method for fetching filepath of STAR reference transcriptome, for a specific genome build
    
    :param check_missing: A toggle for checking errors for missing files, default True
    :returns: A filepath string
    '''
    try:
      ref_file=self._fetch_collection_files(collection_type=self.star_ref_type,
                                            check_missing=check_missing)
      return  ref_file
    except:
      raise

  def get_gatk_snp_ref(self,check_missing=True):
    '''
    A method for fetching filepaths for SNP files from GATK bundle, for a specific genome build
    
    :param check_missing: A toggle for checking errors for missing files, default True
    :returns: A list of filepaths
    '''
    try:
      ref_file=self._fetch_collection_files(collection_type=self.gatk_snp_ref_type,
                                            unique_file=False,
                                            check_missing=check_missing)
      return  ref_file
    except:
      raise

  def get_gatk_indel_ref(self,check_missing=True):
    '''
    A method for fetching filepaths for INDEL files from GATK bundle, for a specific genome build
    
    :param check_missing: A toggle for checking errors for missing files, default True
    :returns: A list of filepaths
    '''
    try:
      ref_file=self._fetch_collection_files(collection_type=self.gatk_indel_ref_type,
                                            unique_file=False,
                                            check_missing=check_missing)
      return  ref_file
    except:
      raise

  def get_dbsnp_vcf(self,check_missing=True):
    '''
    A method for fetching filepath for dbSNP vcf file, for a specific genome build
    
    :param check_missing: A toggle for checking errors for missing files, default True
    :returns: A filepath string
    '''
    try:
      ref_file=self._fetch_collection_files(collection_type=self.genome_dbsnp_type,
                                            check_missing=check_missing)
      return  ref_file
    except:
      raise