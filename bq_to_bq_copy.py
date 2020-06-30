import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import argparse


def run(project_name, src_table, dest_table, pipeline_args):

    pipeline_options = PipelineOptions(
        pipeline_args, streaming=False, save_main_session=True, project=project_name, job_name='bq-2-bq-copy'
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:

        (   pipeline | "Read From Input BQ table" >> beam.io.Read(beam.io.BigQuerySource(table=src_table))
                     | "Write To BigQuery" >> beam.io.WriteToBigQuery(dest_table, write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_name",
        help="name of GCP project"
    )

    parser.add_argument(
      "--source_table_name",
      help="BigQuery Source Table Name in format DATASET.TABLE "
    )

    parser.add_argument(
      "--dest_table_name",
      help="BigQuery Dest Table Name in format DATASET.TABLE "
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(known_args.project_name, known_args.source_table_name, known_args.dest_table_name, pipeline_args )

#python -m bq_to_bq_copy --project_name data-analytics-bk --source_table_name 'data_fusion_pipeline.high_cost_view_mv' --dest_table_name 'da_batch_pipeline.high_cost_view_mv' --runner Dataflow --staging_location gs://da_batch_pipeline/stage --temp_location gs://da_batch_pipeline/temp --region europe-west2 --zone=europe-west2-b