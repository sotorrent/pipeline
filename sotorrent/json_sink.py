import json

import apache_beam as beam
from apache_beam.coders import coders


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
