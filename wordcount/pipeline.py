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
            '--output',
            dest='output',
            default='output/wordcount',
            help='Output file to write the results to.'
        )


class CountWords(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | 'Find words' >> beam.FlatMap(lambda line: re.findall(r'[a-zA-Z\']+', line))
            | 'Pair words with 1' >> beam.Map(lambda w: (w.lower(), 1))
            # can be replaced with: beam.combiners.Count.PerElement()
            | 'Group by word and sum' >> beam.CombinePerKey(sum)
        )


class FormatAsText(beam.DoFn):
    def process(self, element):
        word, count = element
        yield '%s: %s' % (word, count)  # use yield instead of return


def run(save_main_session=True):
    """main entry point"""

    opts = MyOptions()
    opts.view_as(
        SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=opts) as pipeline:
        (
            pipeline
            | 'read lines' >> beam.io.ReadFromText(opts.input)
            | CountWords()
            | 'Format result' >> beam.ParDo(FormatAsText())
            | 'Write results' >> beam.io.WriteToText(opts.output)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
