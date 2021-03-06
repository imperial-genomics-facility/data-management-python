import os, unittest, sqlalchemy
from sqlalchemy import create_engine
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.igfTables import Base,File,Collection,Collection_group
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils

class Reference_genome_utils_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam=read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname=dbparam['dbname']
    self.species_name='HG38'
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()
    collection_data=[{'name':self.species_name,'type':'TRANSCRIPTOME_TENX'},
                     {'name':self.species_name,'type':'GENOME_FASTA'},
                     {'name':self.species_name,'type':'GENOME_DICT'},
                     {'name':self.species_name,'type':'GENOME_BWA'},
                     {'name':self.species_name,'type':'GATK_SNP_REF'},
                     {'name':self.species_name,'type':'INDEL_LIST_VCF'},
                     {'name':self.species_name,'type':'DBSNP_VCF'},
                    ]
    file_data=[{'file_path':'/path/HG38/TenX'},
               {'file_path':'/path/HG38/fasta'},
               {'file_path':'/path/HG38/dict'},
               {'file_path':'/path/HG38/bwa'},
               {'file_path':'/path/HG38/gatk_snp_1'},
               {'file_path':'/path/HG38/gatk_snp_2'},
               {'file_path':'/path/HG38/gatk_dbsnp_vcf'},
               {'file_path':'/path/HG38/gatk_indel_list_vcf'},
               ]
    collection_group_data=[{'name':self.species_name,'type':'TRANSCRIPTOME_TENX','file_path':'/path/HG38/TenX'},
                           {'name':self.species_name,'type':'GENOME_FASTA','file_path':'/path/HG38/fasta'},
                           {'name':self.species_name,'type':'GENOME_DICT','file_path':'/path/HG38/dict'},
                           {'name':self.species_name,'type':'GENOME_BWA','file_path':'/path/HG38/bwa'},
                           {'name':self.species_name,'type':'GATK_SNP_REF','file_path':'/path/HG38/gatk_snp_1'},
                           {'name':self.species_name,'type':'GATK_SNP_REF','file_path':'/path/HG38/gatk_snp_2'},
                           {'name':self.species_name,'type':'INDEL_LIST_VCF','file_path':'/path/HG38/gatk_indel_list_vcf'},
                           {'name':self.species_name,'type':'DBSNP_VCF','file_path':'/path/HG38/gatk_dbsnp_vcf'},
                          ]
    base.start_session()
    ca = CollectionAdaptor(**{'session':base.session})
    fa = FileAdaptor(**{'session':base.session})
    ca.store_collection_and_attribute_data(data=collection_data)
    fa.store_file_and_attribute_data(data=file_data)
    ca.create_collection_group(data=collection_group_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_tenx_ref(self):
    rf = \
      Reference_genome_utils(
        genome_tag=self.species_name,
        dbsession_class=self.session_class)
    file = rf.get_transcriptome_tenx()
    self.assertEqual(file,'/path/HG38/TenX')

  def test_get_dbsnp_vcf(self):
    rf = \
      Reference_genome_utils(
        genome_tag=self.species_name,
        dbsession_class=self.session_class)
    file = rf.get_dbsnp_vcf()
    self.assertEqual(file,'/path/HG38/gatk_dbsnp_vcf')

  def test_get_gatk_indel_ref(self):
    rf = \
      Reference_genome_utils(
        genome_tag=self.species_name,
        dbsession_class=self.session_class)
    file = rf.get_gatk_indel_ref()
    self.assertEqual(file,'/path/HG38/gatk_indel_list_vcf')

  def test_gatk_snp(self):
    rf = \
      Reference_genome_utils(
        genome_tag=self.species_name,
        dbsession_class=self.session_class)
    files = rf.get_gatk_snp_ref()
    self.assertTrue('/path/HG38/gatk_snp_1' in files)
    self.assertTrue('/path/HG38/gatk_snp_2' in files)

  def test_fef_fasta1(self):
    rf = \
      Reference_genome_utils(
        genome_tag=self.species_name,
        dbsession_class=self.session_class)
    file = rf.get_genome_fasta()
    self.assertEqual(file,'/path/HG38/fasta')

  def test_fef_fasta2(self):
    rf = \
      Reference_genome_utils(
        genome_tag=self.species_name,
        dbsession_class=self.session_class,
        genome_fasta_type='GENOME_FASTA1')

    with self.assertRaises(ValueError):
      file=rf.get_genome_fasta()

    file=rf.get_genome_fasta(check_missing=False)
    self.assertEqual(file,None)


if __name__=='__main__':
  unittest.main()