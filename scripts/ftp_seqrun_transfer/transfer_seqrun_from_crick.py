import argparse
from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import transfer_seqrun_tar_from_crick_ftp

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--ftp_hostname', required=True, help='FTP hostname')
parser.add_argument('-s', '--seqrun_id', required=True, help='Seqrun id')
parser.add_argument('-d', '--seqrun_dir', required=True, help='Seqrun base dir')
parser.add_argument('-c', '--ftp_config_file', required=True, help='FTP config file')
args = parser.parse_args()
ftp_hostname = args.ftp_hostname
seqrun_id = args.seqrun_id
seqrun_dir = args.seqrun_dir
ftp_config_file = args.ftp_config_file

if __name__=='__main__':
  transfer_seqrun_tar_from_crick_ftp(
    ftp_host=ftp_hostname,
    ftp_conf_file=ftp_config_file,
    seqrun_id=seqrun_id,
    seqrun_base_dir=seqrun_dir)