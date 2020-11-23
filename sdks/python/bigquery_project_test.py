import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.io.gcp.internal.clients import bigquery
import logging

logging.basicConfig(level=logging.DEBUG)

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  # parser.add_argument(
  #     '--output',
  #     dest='output',
  #     required=True,
  #     help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
  google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
  google_cloud_options.project = 'frankzhao-test-project'
  google_cloud_options.job_name = 'test-project-dataset'
  google_cloud_options.staging_location = 'gs://frankzhao-dataflow/staging'
  google_cloud_options.temp_location = 'gs://frankzhao-dataflow/temp'
  google_cloud_options.region = 'us-central1'

  q = "select service_city from `bigquery-public-data.faa.us_airports` limit 3"

  p = beam.Pipeline(options=pipeline_options)
  pipeline = p | "Read from BQ source" >> beam.io.ReadFromBigQuery(
      query=q,
      use_standard_sql=True,
      project='frankzhao-test-project',
      temp_dataset=bigquery.DatasetReference(
          projectId='frank-test-264222', datasetId='bq_temp'))

  result = p.run()
  result.wait_until_finish()
  print(result)

if __name__ == '__main__':
  run()