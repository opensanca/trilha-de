import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import string


# Remove whitespaces and punctuation and split words
def extract_words(line):
  return line.translate(line.maketrans(string.whitespace,"      ",string.punctuation)).split()

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
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
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
  (p | 'read' >> ReadFromText(known_args.input)
     | 'extract words' >> beam.FlatMap(extract_words)
     | 'transform to kv' >> beam.Map(lambda x: (x,1))
     | 'group by words' >> beam.GroupByKey()
     | 'count ones' >> beam.Map(count_ones)
     | 'format for bq' >> beam.Map(format_for_bigquery)
     | 'write to bigquery' >> WriteToBigQuery(table=known_args.output))

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()