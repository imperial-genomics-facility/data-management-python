new Browser({
    chr:          '{{ chr }}',
    viewStart:    {{ startPosition }},
    viewEnd:      {{ endPosition }},

    coordSystem: {
      speciesName: '{{ speciesName }}',
      taxon: {{ taxonId }},
      auth: '{{ refGenomeAuth }}',
      version: '{{ refVersion }}',
      ucscName: '{{ refUcscName }}'
    },
    cookieKey: true,
    sources:     {{ trackList }}

  });
