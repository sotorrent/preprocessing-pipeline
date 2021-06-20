import json
import logging
import os

import apache_beam as beam

from xml.etree import ElementTree
from apache_beam.coders import coders

logger = logging.getLogger(__name__)


def run_pipeline(config):
    """
    Execute the preprocessing pipeline (locally, later also in Google Cloud).
    :return: None
    """
    logger.info(f"Writing output of pipeline to '{config.output_file}'")
    logger.info(f"Reading and converting XML file '{config.input_file}'...")
    with beam.Pipeline(options=config.get_pipeline_options()) as p:
        dict_elements = (p
                         | "Read XML file" >> beam.io.ReadFromText(config.input_file)
                         | "Ignore non-row elements" >> beam.Filter(filter_rows)
                         | "Convert XML attributes to dict elements" >> beam.Map(xml_attributes_to_dict))
        output_file_without_ext, _ = os.path.splitext(config.output_file)
        logger.info(f"Writing data to JSONL file '{config.output_file}'")
        (dict_elements  | "Filter post edits" >> beam.Filter(filter_post_edits)
                        | "Extract relevant post attributes" >> beam.Map(extract_attributes)
                        | "Group by post id" >> beam.GroupBy(get_key)
                        | "Get most recent version for each post id" >> beam.Map(get_most_recent_version)
                        | "Extract text blocks" >> beam.Map(extract_text_blocks)
                        | "Normalize text blocks" >> beam.Map(normalize_text_blocks)
                        | "Writing data to JSONL file" >> WriteToJson(output_file_without_ext))
    logger.info(f"Pipeline finished.")


def filter_rows(input_str):
    """
    Filter matching rows, i.e. strings containing <row> XML elements.
    :param input_str: row possibly containing a <row> XML element (could also contain their root element, e.g. <post>)
    :return: boolean indicating whether XML element is a <row>
    """
    return input_str.lstrip().startswith('<row')


def xml_attributes_to_dict(xml_str):
    """
    Parse an XML <row> element and return its attributes as dict.
    :param xml_str: string containing XML <row> element
    :return: dict with XML attributes
    """
    return ElementTree.fromstring(xml_str).attrib


def filter_post_edits(dict_elem):
    """
    Filter post history events that modified the body of the posts.
    :param dict_elem: dict with parsed XML attributes
    :return: boolean indicating whether element modified the body of the corresponding post
    """
    return int(dict_elem['PostHistoryTypeId']) in [2, 5, 8]


def extract_attributes(dict_elem):
    """
    Select the attributes of interest from the attribute dict.
    :param dict_elem: dict with parsed XML attributes
    :return: dict with selection of parsed XML attributes
    """
    return {
        'PostId': dict_elem['PostId'],
        'CreationDate': dict_elem['CreationDate'],
        'Text': dict_elem['Text']
    }


def get_key(dict_elem):
    """
    Returns the key property of the attribute dict.
    :param dict_elem: dict with parsed XML attributes
    :return: key element
    """
    return dict_elem['PostId']


def get_most_recent_version(grouped_dict_elem):
    """
    Sorts the dict values of the grouped post edits by creation date in descending order and return the most recent
    dict element i.e. version of the post.
    :param grouped_dict_elem:  dict elements with post id as key and list of attribute dict elements as values
    :return: most recent dict element i.e. post version
    """
    (post_id, post_edits) = grouped_dict_elem
    post_edits.sort(key=lambda dict_elem: dict_elem['CreationDate'], reverse=True)
    return post_edits[0]


def extract_text_blocks(dict_elem):
    """
    Extracts all text blocks from the Markdown source of a post version
    :param dict_elem: dict with parsed XML attributes
    :return: tuple of post_id and text_blocks
    """
    return dict_elem['PostId'], [dict_elem['Text']]


def normalize_text_blocks(text_block_tuple):
    """
    Normalizes the content of text blocks (Markdown)
    :param text_block_tuple: tuple of post_id and text_blocks
    :return: sample tuple, but with normalized text blocks
    """
    return text_block_tuple


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
