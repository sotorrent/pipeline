import json
import logging
import os

import apache_beam as beam

from xml.etree import ElementTree
from apache_beam.coders import coders

logger = logging.getLogger(__name__)


def run_pipeline(config):
    """
    Execute the SOTorrent pipeline (either locally with limited functionality or in Google Cloud).
    :return: None
    """
    input_paths = config.input_paths
    output_dir = config.pipeline['output_dir']

    logger.info(f"Writing output of pipeline to '{output_dir}'")
    for table_name, input_path in input_paths.items():
        logger.info(f"Reading and converting XML file for table '{table_name}' from '{input_path}'...")
        with beam.Pipeline(options=config.get_pipeline_options(table_name)) as p:
            dict_elements = (p
                             | "Read XML file" >> beam.io.ReadFromText(input_path)
                             | "Ignore non-row elements" >> beam.Filter(filter_rows)
                             | "Convert XML attributes to dict elements" >> beam.Map(xml_attributes_to_dict))

            bigquery_dataset = config.pipeline['bigquery_dataset']
            logger.info(f"Writing data into BigQuery dataset '{bigquery_dataset}'")
            (dict_elements | "Write data into BigQuery table" >> beam.io.WriteToBigQuery(
                f'{bigquery_dataset}.{table_name}',
                schema=config.bigquery_schemas_with_fields[table_name],
                write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

            file_name_without_extension = os.path.join(output_dir, table_name)
            logger.info(f"Writing data to JSONL file '{file_name_without_extension}.jsonl'")
            (dict_elements | "Writing data to JSONL file" >> WriteToJson(file_name_without_extension, num_shards=1))
    logger.info(f"Pipeline finished.")


def filter_rows(input_str):
    """
    Filter matching rows, i.e. strings containing <row> XML elements.
    :param input_str: row possibly containing a <row> XML element (could also contain their root element, e.g. <post>)
    :return:
    """
    return input_str.lstrip().startswith('<row')


def xml_attributes_to_dict(xml_str):
    """
    Parse an XML <row> element and return its attributes as dict.
    :param xml_str: string containing XML <row> element
    :return:
    """
    return ElementTree.fromstring(xml_str).attrib


class JsonSink(beam.io.FileBasedSink):
    """
    An Apache Beam sink for writing JSON files.
    See also: https://stackoverflow.com/a/43185539
    """

    def __init__(self,
                 file_path_prefix,
                 file_name_suffix='.json',
                 write_jsonl=False,  # see https://jsonlines.org/
                 num_shards=0
                 ):
        super().__init__(file_path_prefix,
                         coder=coders.StrUtf8Coder(),
                         file_name_suffix=file_name_suffix,
                         num_shards=num_shards,
                         mime_type='text/plain')
        self.write_jsonl = write_jsonl
        self.previous_row = dict()

    def open(self, temp_path):
        """
        Open JSON file and initialize it with an opening square bracket, i.e. a JSON list.
        """
        file_handle = super(JsonSink, self).open(temp_path)
        if not self.write_jsonl:
            file_handle.write(self.coder.encode('[\n'))
        return file_handle

    def write_record(self, file_handle, value):
        """
        Converts a single record to an encoded JSON and writes it terminated by a comma.
        """
        # write previous encoded value and store current value (to be able to handle the last value differently)
        if self.previous_row.get(file_handle, None) is not None:
            file_handle.write(self.coder.encode(json.dumps(self.previous_row[file_handle])))
            if not self.write_jsonl:
                file_handle.write(self.coder.encode(','))
            file_handle.write(self.coder.encode('\n'))
        self.previous_row[file_handle] = value

    def write_encoded_record(self, file_handle, encoded_value):
        """Writes a single encoded record to the file handle returned by ``open()``.
        """
        raise NotImplementedError

    def close(self, file_handle):
        """
        Add closing square bracket to finalize the JSON list and close the file handle
        """
        if file_handle is not None:
            # write last row without a comma
            file_handle.write(self.coder.encode(json.dumps(self.previous_row[file_handle])))
            if not self.write_jsonl:
                # close JSON list
                file_handle.write(self.coder.encode('\n]\n'))
            # close file handle
            file_handle.close()


class WriteToJson(beam.PTransform):
    """
    A PTransform writing to a JsonSink.
    """
    def __init__(self, file_path_prefix, file_name_suffix='.jsonl', write_jsonl=True, num_shards=0):
        super().__init__()
        self._sink = JsonSink(file_path_prefix, file_name_suffix, write_jsonl, num_shards)

    def expand(self, input_or_inputs):
        return input_or_inputs | beam.io.Write(self._sink)
