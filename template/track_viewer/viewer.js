new Browser({
    chr:          '{{ chrname }}',
    viewStart:    {{ startPosition }},
    viewEnd:      {{ endPosition }},

    coordSystem: {
      speciesName: '{{ speciesCommonName }}',
      taxon: {{ speciesTaxon }},
      auth: '{{ refAuthority }}',
      version: '{{ refVersion }}',
      ucscName: '{{ speciesUcscName }}'
    },
    cookieKey: true,
    sources:     [{name:      'Genome',
                   twoBitURI: '{{ twoBitRefRemoteUrl }}',
                   tier_type: 'sequence'},
                  {name:      'Ensembl transcripts',
                   uri:       'http://rest.ensembl.org',
                   tier_type: 'ensembl',
                   species:   '{{ ensemblSpecies }}',
                   type:      ['transcript', 'exon', 'cds']},
                  {% for row in bwdata %}
                  {name:     '{{ row["track_name"] }}',
                   desc:     '{{ row["track_desc"] }}',
                   bwgURI:   '{{ row["track_path"] }}', 
                   noDownsample: true},
                  {% endfor %}
                 ]
});
