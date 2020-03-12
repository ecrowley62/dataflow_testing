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


# Query string
query_string = """
SELECT t.target_zip AS target_zip,
       t.radius AS radius,
       t.asofdate AS asofdate,
       AVG(volume_national) AS volume_national,
       AVG(rejects_national) AS rejects_national,
       AVG(volume_outgoing) AS volume_outgoing,
       AVG(otms) AS otms,
       AVG(nrejects_outgoing) AS nrejects_outgoing,
       AVG(otri) AS otri,
       AVG(volume_incoming) AS volume_incoming,
       AVG(itms) AS itms,
       AVG(nrejects_incoming) AS nrejects_incoming,
       AVG(itri) AS itri,
       AVG(haul) AS haul,
       AVG(norm_haul) AS norm_haul,
       AVG(volume_zip3) AS volume_zip3
FROM
    (
        SELECT m.target_zip,
               m.radius,
               th.asofdate,
               AVG(th.volume_national) AS volume_national,
               AVG(th.rejects_national) AS rejects_national,
               ROUND(SUM(th.volume_outgoing), 0) AS volume_outgoing,
               CASE
                  WHEN AVG(th.volume_national) = 0 THEN 0
                  ELSE SUM(th.volume_outgoing) / SUM(th.volume_national)
                  END AS otms,
               SUM(th.nrejects_outgoing) AS nrejects_outgoing,
               CASE
                  WHEN AVG(th.volume_outgoing) = 0 THEN 0
                  ELSE SUM(th.nrejects_outgoing) / SUM(th.volume_outgoing)
                  END AS otri,
               ROUND(SUM(th.volume_incoming), 0) AS volume_incoming,
               CASE
                  WHEN AVG(th.volume_national) = 0 THEN 0
                  ELSE SUM(th.volume_incoming) / SUM(th.volume_national)
                  END AS itms,
               ROUND(SUM(th.nrejects_incoming), 0) AS nrejects_incoming,
               CASE
                  WHEN SUM(th.volume_incoming) = 0 THEN 0
                  ELSE SUM(th.nrejects_incoming) / SUM(th.volume_incoming)
                  END AS itri,
               ROUND(SUM(th.volume_outgoing) - SUM(th.volume_incoming), 0) AS haul,
               CASE
                  WHEN (SUM(th.volume_incoming) + SUM(th.volume_outgoing)) = 0 THEN 0
                  ELSE (SUM(th.volume_outgoing - th.volume_incoming)) / (SUM(th.volume_incoming) + SUM(th.volume_outgoing))
                  END AS norm_haul,
              ROUND(SUM(th.volume_incoming) + SUM(th.volume_outgoing), 0) AS volume_zip3,
       FROM reference_tables.zip3_markets_radius_bands AS m
       INNER JOIN warehouse.fwrates_tender_zip3_7day AS th
          ON m.zips_in_radius = th.zip3
       GROUP BY m.target_zip, m.radius, th.asofdate
       UNION DISTINCT
       SELECT m.target_zip,
              m.radius,
              tf.asofdate,
              AVG(tf.volume_national) AS volume_national,
              AVG(tf.rejects_national) AS rejects_national,
              ROUND(SUM(tf.volume_outgoing), 0) AS volume_outgoing,
              CASE
                  WHEN AVG(tf.volume_national) = 0 THEN 0
                  ELSE SUM(tf.volume_outgoing) / SUM(tf.volume_national)
                  END AS otms,
              SUM(tf.nrejects_outgoing) AS nrejects_outgoing,
              CASE
                  WHEN AVG(tf.volume_outgoing) = 0 THEN 0
                  ELSE SUM(tf.nrejects_outgoing) / SUM(tf.volume_outgoing)
                  END AS otri,
              ROUND(SUM(tf.volume_incoming), 0) AS volume_incoming,
              CASE
                  WHEN AVG(tf.volume_national) = 0 THEN 0
                  ELSE SUM(tf.volume_incoming) / SUM(tf.volume_national)
                  END AS itms,
              ROUND(SUM(tf.nrejects_incoming), 0) AS nrejects_incoming,
              CASE
                  WHEN SUM(tf.volume_incoming) = 0 THEN 0
                  ELSE SUM(tf.nrejects_incoming) / SUM(tf.volume_incoming)
                  END AS itri,
              ROUND(SUM(tf.volume_outgoing) - SUM(tf.volume_incoming), 0) AS haul,
              CASE
                  WHEN (SUM(tf.volume_incoming) + SUM(tf.volume_outgoing)) = 0 THEN 0
                  ELSE (SUM(tf.volume_outgoing - tf.volume_incoming)) / (SUM(tf.volume_incoming) + SUM(tf.volume_outgoing))
                  END AS norm_haul,
              ROUND(SUM(tf.volume_incoming) + SUM(tf.volume_outgoing), 0) AS volume_zip3
      FROM reference_tables.zip3_markets_radius_bands AS m
      INNER JOIN warehouse.FWrates_tender_zip3_forecast_mu AS tf
          ON m.zips_in_radius = tf.zip3
      GROUP BY m.target_zip, m.radius, tf.asofdate, tf.volume_national, tf.rejects_national                    
    ) AS t
GROUP BY t.asofdate, t.target_zip, t.radius;
"""

query_string = """
SELECT *
FROM warehouse.zip3_markets_tender_all
"""





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




# Table specifications for loading into BigQuery
forecast_table_spec = bigquery.TableReference(
    projectId='freightwaves-engineering-prod',
    datasetId='warehouse',
    tableId='FWrates_tender_zip3_forecast_mu'
)


with beam.Pipeline(options=options) as p:
    pipe = (
        p
        | "Input" >> beam.io.Read(beam.io.BigQuerySource(query=query_string))
        | beam.Map(print)
    )





'''
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
''' 






