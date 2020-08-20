import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import argparse
import re

# Read from GCS and write to BQ Table

# Args:
#     input file uri
#     output table id (dataset_id.table_id)

schema =   'jobid:STRING, scenarioid:INT64, migration_pl:FLOAT64, default_pl:FLOAT64'

class convertToDict:

    def parse_method (self, element):
        value = re.split(",", element)

        row = dict(zip(('jobid','scenarioid','migration_pl','default_pl'), value))
        return row

def run(project_name, i_file_path, dest_table_id, pipeline_args):

    data_ingestion = convertToDict()

    pipeline_options = PipelineOptions(
        pipeline_args, streaming=False, save_main_session=True, project=project_name, job_name='csv-gcs-to-bq'
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:

        (   pipeline | "Read From Input Datafile" >> beam.io.ReadFromText(i_file_path)
                     | "Convert to Dict" >> beam.Map(lambda r: data_ingestion.parse_method(r))
                     | "Write to BigQuery Table" >> beam.io.WriteToBigQuery('{0}:{1}'.format(project_name, dest_table_id),
                                                                            schema=schema, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_name",
        help="name of GCP project"
    )

    parser.add_argument(
      "--input_file_path",
      help="uri of the input file"
    )

    parser.add_argument(
      "--dest_table_id",
      help="BQ dataset_id.table_id"
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(known_args.project_name, known_args.input_file_path, known_args.dest_table_id, pipeline_args )

