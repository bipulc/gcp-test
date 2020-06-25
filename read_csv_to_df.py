import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import csv
import io
import logging
import argparse

def create_dataframe(i_file_path):

    # Open a channel to read the file from GCS
    gcs_file = beam.io.filesystems.FileSystems.open(i_file_path)

    # Read it as csv, you can also use csv.reader
    csv_dict = csv.DictReader(io.TextIOWrapper(gcs_file))

    # Create the DataFrame
    dataFrame = pd.DataFrame(csv_dict)
    #print(dataFrame.to_string())

def run(project_name, i_file_path, o_file_path, pipeline_args):

    pipeline_options = PipelineOptions(
        pipeline_args, streaming=False, save_main_session=True, project=project_name, job_name='csv-to-df'
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:

        (   pipeline | "Read From Input Datafile" >> beam.Create([i_file_path])
                     | "Create Pandas DataFrame" >> beam.FlatMap(create_dataframe)
                     | "Write DataFrame to Output Datafile" >> beam.io.WriteToText(o_file_path)
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
      help="GCS Path of the input file "
    )

    parser.add_argument(
      "--output_file_path",
      help="GCS Path of the output file "
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(known_args.project_name, known_args.input_file_path, known_args.output_file_path, pipeline_args )

#python -m read_csv_to_df --project_name data-analytics-bk --input_file_path 'gs://da_batch_pipeline/trans_six_col.csv' --output_file_path 'gs://da_batch_pipeline/dataframe_output.csv' --runner Dataflow --staging_location gs://da_batch_pipeline/stage --temp_location gs://da_batch_pipeline/temp --region europe-west2 --zone=europe-west2-b