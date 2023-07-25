#!/bin/bash

set -eo pipefail

cd {{ WORK_DIR }}

script=$(cat << EOF
  set timeout 3600
  eval spawn {{ GEOMX_NGS_EXE }} \
   --in={{ FASTQ_DIR }} \
   --out={{ OUTPUT_DIR }} \
   --ini={{ INPUT_INI_FILE }} \
   --translation-file={{ INPUT_TRANSLATION_FILE }} \
   {% for p in GEOMX_PARAMS %}{{ p }} {% endfor %}
  expect ""
  send -- "2\r"
  expect "*All done*"
EOF
)
/usr/bin/expect -c "$script"