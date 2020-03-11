# Auth: Eric Crowley
# Date: 2019-11-26
# Desc: ETL pipelin written using beam for INTTRA si data set

from google.cloud import storage
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
import apache_beam as beam
from datetime import datetime


# Create class for providing the file name as a custom option to the pipe
class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--file_to_clean',
            required=True,
            type=str,
            help='Name of file to clean'
        )

# Return true if IMO value is a numeric
def is_valid_imo(text):
    imo = text.split('|')[7].replace('"','')
    if imo != '':
        return imo.isnumeric()
    else:
        return True

# Replace empty strings
def replace_empty_str(text):
    from datetime import datetime
    vals = text.split('|')
    new_str = []
    for val in vals:
        val = val[1:-1]
        if '"' in val:
            val = val.replace('"','""')
        if ',' in val:
            val = f"\"{val}\""
        new_str.append(val)
    new_str.append(datetime.now().strftime('%Y-%m-%d'))
    return ','.join(new_str)

# Setup options for pipe
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
custom_options = options.view_as(CustomPipelineOptions)
google_cloud_options.project = 'freightwaves-engineering-prod'
google_cloud_options.job_name = f"clean-intra-si-{datetime.now().strftime('%Y%m%d%H%M%S')}"
google_cloud_options.staging_location = 'gs://fw-etl-tmp-prod/'
google_cloud_options.temp_location = 'gs://fw-etl-tmp-prod/'
options.view_as(StandardOptions).runner = 'DataFlowRunner'
#options.view_as(StandardOptions).runner = 'DirectRunner'



# Create pipeline object
p = beam.Pipeline(options=options)

# Define the pipeline steps
out = (p | "Input" >> beam.io.ReadFromText(f'gs://fw-etl-raw-prod/inttra/{custom_options.file_to_clean}')
         | "Remove Null Imos" >> beam.Filter(is_valid_imo)
         | "Remove empty strings" >> beam.Map(replace_empty_str)
         | "Output" >> beam.io.WriteToText(f'gs://fw-etl-load-prod/inttra/{custom_options.file_to_clean}', shard_name_template='')
         #| beam.Map(print)
)

# Run the pipeline
result = p.run()
