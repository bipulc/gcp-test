#!/usr/bin/env python3

import argparse

def load_table_uri_csv(table_id, uri):

    # [START bigquery_load_table_gcs_csv]
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("jobid", "STRING"),
            bigquery.SchemaField("scenarioid", "INT64"),
            bigquery.SchemaField("migration_pl", "FLOAT64"),
            bigquery.SchemaField("default_pl", "FLOAT64"),
        ],
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    # [END bigquery_load_table_gcs_csv]

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-u", type=str, help="uri of file in GCS", required=True)
    parser.add_argument("-t", type=str, help="table_id in BQ dataset_id.table_id", required=False)

    args = parser.parse_args()

    load_table_uri_csv(args.t, args.u)