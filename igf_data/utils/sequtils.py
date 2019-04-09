import sys

def rev_comp(input_seq):
  '''
  A function for converting nucleotide sequence to its reverse complement
  
  :param input_seq: A string of nucleotide sequence
  :returns: Reverse complement version of the input sequence
  '''
  try:
    revcomp_seq=None
    revcomp_seq=input_seq.upper().translate(str.maketrans('ACGT','TGCA'))[::-1]
    return revcomp_seq
  except:
    raise