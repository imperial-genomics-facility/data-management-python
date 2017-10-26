import sys

try:
  if sys.version_info[0] < 3:
    # Python 2.x specific import
    from string import maketrans
except:
  raise

def rev_comp(input_seq):
  '''
  A function for converting nucleotide sequence to its reverse complement
  required params:
  input_seq: A nucleotide sequence
  returns the reverse complement version of the input sequence
  '''
  try:
    revcomp_seq=None
    if sys.version_info[0] < 3:
      # For Python 2.x, use maketrans
      revcomp_seq=input_seq.upper().translate(maketrans('ACGT','TGCA'))[::-1]
    else:
      # For Python 3.x, use str.maketrans
      revcomp_seq=input_seq.upper().translate(str.maketrans('ACGT','TGCA'))[::-1]
    return revcomp_seq
  except:
    raise