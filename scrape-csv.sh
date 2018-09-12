#!/bin/bash
# scrapes a csv file from project root using csv-generator, to the 'output' directory in project root
set -e
cd csv-generator
source venv/bin/activate
input_file=$1
python -m csv_generator.process_xml_zip --source-zip "../$input_file" --output-dir ../output
