# Module imports
from google.cloud import storage
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions 
from apache_beam.io.gcp.internal.clients import bigquery
import apache_beam as beam
import uuid



# Setup options for beam pipe
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'freightwaves-engineering-prod'
google_cloud_options.job_name = f"tender-rates-{uuid.uuid4().hex}"
google_cloud_options.staging_location = 'gs://fw-etl-tmp-prod/'
google_cloud_options.temp_location = 'gs://fw-etl-tmp-prod/'
options.view_as(StandardOptions).runner = 'DataFlowRunner'
#options.view_as(StandardOptions).runner = 'DirectRunner'


# Table specifications for loading into BigQuery
forecast_table_spec = bigquery.TableReference(
    projectId='freightwaves-engineering-prod',
    datasetId='warehouse',
    tableId='FWrates_tender_zip3_forecast_mu'
)

# List of column names for target table
bq_table_columns = ['asofdate','zip3','volume_national','rejects_national','volume_outgoing','otms','nrejects_outgoing','otri','volume_incoming','itms','nrejects_incoming','itri','haul','norm_haul','volume_zip3']


# Filter out header row
def is_data(text):
    header_str = ','.join(bq_table_columns)
    if header_str == text:
        return False
    else:
        return True
    

# Convert the csv string into a python dictionary
def str_to_dict(text):
    vals_list = text.split(',')
    vals_dict = {}
    for val, col in zip(vals_list,bq_table_columns):
        vals_dict[col] = val
    return vals_dict


# Define pipeline steps in pipeline object (p)
with beam.Pipeline(options=options) as p:
    pipe = (
        p 
        | "Input" >> beam.io.ReadFromText('gs://fw-etl-tmp-prod/FWrates_tender_zip3_forecast_mu.csv')
        | "Remove Header" >> beam.Filter(is_data)
        | "Convert To Dict" >> beam.Map(str_to_dict)
        | "Load BQ table" >> beam.io.WriteToBigQuery(
            forecast_table_spec,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )
        #| beam.Map(print)
    )







