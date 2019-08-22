class ReformatMetadataForValidation:
  def __init__(self,metadata_file,
               required_columns=\
                 ('project_igf_id',
                  'name',
                  'email_id',
                  'sample_igf_id',
                  'species_name',
                  'expected_read',
                  'experiment_type',
                  'fragment_length_distribution_mean',
                  'fragment_length_distribution_sd',
                  'biomaterial_type'))