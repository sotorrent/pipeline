import json
import logging

import apache_beam as beam
import xmltodict
from apache_beam.coders import coders
from apache_beam.io.gcp.gcsio import GcsIO

from sotorrent_pipeline.util.google_cloud import get_storage_client

logger = logging.getLogger(__name__)


class XmlToDict:
    def __init__(self, file):
        self.file = file  # path to XML file
        self.dict_elements = []  # list of dicts with content of XML file

    def _handle_row(self, path, _):
        self.dict_elements.append((path[1][1]))  # this accesses the attributes of a row element in the XML dump files
        return True

    def parse_xml_into_dict(self):
        """
        Parse XML file into a dict
        """
        logger.info(f"Parsing XML file '{self.file}'...")

        if str(self.file).startswith('gs://'):
            google_cloud_io = GcsIO()
            with google_cloud_io.open(self.file, 'r') as fp:
                self._parse_xml_into_dict(fp)
        else:
            with open(self.file, 'r', encoding='utf-8') as fp:
                self._parse_xml_into_dict(fp)

        logger.info(f"Parsed '{len(self.dict_elements)}' elements.")
        return self.dict_elements

    def _parse_xml_into_dict(self, fp):
        xmltodict.parse(fp.read(), item_depth=2, item_callback=self._handle_row)


class JsonSink(beam.io.FileBasedSink):
    """
    An Apache Beam sink for writing JSON files.
    See also: https://stackoverflow.com/a/43185539
    """

    def __init__(self,
                 file_path_prefix,
                 file_name_suffix='.json',
                 write_jsonl=False  # see https://jsonlines.org/
                 ):
        super().__init__(file_path_prefix,
                         coder=coders.StrUtf8Coder(),
                         file_name_suffix=file_name_suffix,
                         mime_type='application/json',)
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
    def __init__(self, file_path_prefix, file_name_suffix='.json', write_jsonl=False):
        super().__init__()
        self._sink = JsonSink(file_path_prefix, file_name_suffix, write_jsonl)

    def expand(self, input_or_inputs):
        return input_or_inputs | beam.io.Write(self._sink)
