from __future__ import absolute_import

import argparse
import logging
import re
from past.builtins import unicode

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input',
            dest='input',
            default='../_data/*',
            help='Input file to process.'
        )
        parser.add_argument(
            '--output-dir',
            dest='output_dir',
            default='output/',
            help='Output directory to write the results to.'
        )


class CountWords(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | 'Find words' >> beam.FlatMap(lambda line: re.findall(r'[a-zA-Z\']+', line))
            | 'Pair words with 1' >> beam.Map(lambda w: (w.lower(), 1))
            | 'Group by word and sum' >> beam.CombinePerKey(sum)
        )


class FilterWordsCount_LT100(beam.DoFn):
    def process(self, element):
        word, count = element
        if count < 100:
            yield element  # returns a sequence of element


class FilterWordsCount_GTE100(beam.DoFn):
    def process(self, element):
        word, count = element
        if count >= 100:
            # here we return a list/collection of element, similar to yield
            return [element]


def run(save_main_session=True):
    """main entry point"""

    opts = MyOptions()
    opts.view_as(
        SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=opts) as pipeline:
        word_counts = (
            pipeline
            | 'read lines' >> beam.io.ReadFromText(opts.input)
            | 'count words' >> CountWords()
        )
        # Branch 1
        (
            word_counts
            | 'filter count < 100' >> beam.ParDo(FilterWordsCount_LT100())
            | 'write results 1' >> beam.io.WriteToText(opts.output_dir + 'lt_100')
        )
        # Branch 2
        (
            word_counts
            | 'filter count >= 100' >> beam.ParDo(FilterWordsCount_GTE100())
            | 'write results 2' >> beam.io.WriteToText(opts.output_dir + 'gte_100')
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
