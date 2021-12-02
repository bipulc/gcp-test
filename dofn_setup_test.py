import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging

class DoFnMethods(beam.DoFn):
  def __init__(self):
    print('__init__')
    self.window = beam.window.GlobalWindow()

  def setup(self):
    print('setup')
    logging.info('in setup')

  def start_bundle(self):
    print('start_bundle')
    logging.info('in start_bundle')

  def process(self, element, window=beam.DoFn.WindowParam):
    self.window = window
    yield  element

  def finish_bundle(self):
    print('finish_bundle')
    logging.info('in finish_bundle')

  def teardown(self):
    print('teardown')
    logging.info('in teardown')

pubsub_subs = 'projects/data-analytics-bk/subscriptions/df_res_test_subs'
project_name = 'data-analytics-bk'
job_name='df-pubsub-streaming-window'

pipeline_options = PipelineOptions(
    streaming=True, save_main_session=True, project=project_name, job_name=job_name
)

with beam.Pipeline(options=pipeline_options) as pipeline:
  results = (
      pipeline
      | "Read PubSub messages from Subs" >> beam.io.ReadFromPubSub(subscription=pubsub_subs)
      | 'DoFn methods' >> beam.ParDo(DoFnMethods())
      | beam.Map(print))

