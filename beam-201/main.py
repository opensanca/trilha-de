import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
import apache_beam.transforms.trigger as trigger
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import json
import string

# Remove whitespaces and punctuation and split words
def extract_words(message):
  parsed_message = json.loads(message)
  page = parsed_message['content']
  return page.translate(page.maketrans(string.whitespace,"      ",string.punctuation)).split()

# Count the occurrences of each word.ls

def count_ones(word_ones):
  (word, ones) = word_ones
  return (word, sum(ones))

def format_for_bigquery(tuple):
  return {
    "word": tuple[0],
    "count": tuple[1]
  }

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      required=True,
                      help='Input Pub/Sub subscription to read from.')
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output BigQuery table to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  p = beam.Pipeline(options=pipeline_options)

  # Read the text file[pattern] into a PCollection.
  (p | 'read' >> ReadFromPubSub(subscription=known_args.input)
     | 'extract words' >> beam.FlatMap(extract_words)
     | 'transform to kv' >> beam.Map(lambda x: (x,1))
     | 'window per minute' >> beam.WindowInto(
                                window.FixedWindows(5),
                                trigger=trigger.AfterProcessingTime(delay=10),
                                accumulation_mode=trigger.AccumulationMode.DISCARDING)
     | 'group by words' >> beam.GroupByKey()
     | 'count ones' >> beam.Map(count_ones)
     | 'format for bq' >> beam.Map(format_for_bigquery)
     | 'write to bigquery' >> WriteToBigQuery(table=known_args.output))

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()