import json
import logging
import os

import apache_beam as beam

from xml.etree import ElementTree
from apache_beam.coders import coders

from preprocessing_pipeline.so.pipeline.post_blocks import CodeBlock, TextBlock
from preprocessing_pipeline.so.util.regex import *

logger = logging.getLogger(__name__)


def run_pipeline(config):
    """
    Execute the preprocessing pipeline (locally, later also in Google Cloud).
    :return: None
    """
    logger.info(f"Writing output of pipeline to '{config.output_dir}'")
    logger.info(f"Reading and converting XML files from '{config.input_dir}'...")
    with beam.Pipeline(options=config.get_pipeline_options()) as p:
        posts_elems = (p
                       | "Read posts XML file" >> beam.io.ReadFromText(config.posts_xml)
                       | "Ignore non-row post elements" >> beam.Filter(filter_rows)
                       | "Convert post XML attributes to dict elements" >> beam.Map(xml_attributes_to_dict)
                       | "Ignore posts with non-positive score" >> beam.Filter(filter_score)
                       | "Extract relevant post attributes" >> beam.Map(extract_posts_attributes)
                       | "Group posts by post id" >> beam.GroupBy(get_post_id))

        posts_dict = beam.pvalue.AsDict(posts_elems)

        comment_elems = (p
                         | "Read comment XML file" >> beam.io.ReadFromText(config.comments_xml)
                         | "Ignore non-row comment elements" >> beam.Filter(filter_rows)
                         | "Convert comment XML attributes to dict elements" >> beam.Map(xml_attributes_to_dict)
                         | "Ignore comments with non-positive score" >> beam.Filter(filter_score)
                         | "Extract relevant comment attributes" >> beam.Map(extract_comment_attributes)
                         | "Group comments by post id" >> beam.GroupBy(get_post_id)
                         | "Extract comment text" >> beam.Map(extract_comment_text))

        comment_dict = beam.pvalue.AsDict(comment_elems)

        post_history_elems = (p
                              | "Read post history XML file" >> beam.io.ReadFromText(config.post_history_xml)
                              | "Ignore non-row post history elements" >> beam.Filter(filter_rows)
                              | "Convert post history XML attributes to dict elements" >> beam.Map(xml_attributes_to_dict)
                              | "Filter post edits and ignore posts with non-positive score" >> beam.Filter(filter_post_edits_score, posts=posts_dict)
                              | "Extract relevant post history attributes" >> beam.Map(extract_post_history_attributes)
                              | "Group post history by post id" >> beam.GroupBy(get_post_id)
                              | "Get most recent version for each post id" >> beam.Map(get_most_recent_version))

        output_file_without_ext, _ = os.path.splitext(config.output_jsonl)
        logger.info(f"Writing data to JSONL file '{config.output_jsonl}'")
        (post_history_elems
         | "Extract text blocks" >> beam.Map(extract_text_blocks)
         | "Add comments" >> beam.Map(add_comments, comments=comment_dict)
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


def filter_post_edits_score(dict_elem, posts):
    """
    Filter post history events that modified the body of the posts.
    :param dict_elem: dict with parsed XML attributes
    :param posts: dict with posts (to get post score)
    :return: boolean indicating whether element modified the body of the corresponding post
    """
    post_id = int(dict_elem['PostId'])
    post_history_type_id = int(dict_elem['PostHistoryTypeId'])
    return post_history_type_id in [2, 5, 8] and post_id in posts


def filter_score(dict_elem):
    """
    Filter post/comment elements with a positive score.
    :param dict_elem: dict with parsed XML attributes
    :return: boolean indicating whether post has a positive score
    """
    return int(dict_elem['Score']) > 0


def extract_post_history_attributes(dict_elem):
    """
    Select the attributes of interest from the post history attribute dict.
    :param dict_elem: dict with parsed XML attributes
    :return: dict with selection of parsed XML attributes
    """
    return {
        'PostId': int(dict_elem['PostId']),
        'CreationDate': dict_elem['CreationDate'],
        'Text': dict_elem['Text']
    }


def extract_posts_attributes(dict_elem):
    """
    Select the attributes of interest from the post history attribute dict.
    :param dict_elem: dict with parsed XML attributes
    :return: dict with selection of parsed XML attributes
    """
    return {
        'PostId': int(dict_elem['Id']),
        'Score': dict_elem['Score']
    }


def extract_comment_attributes(dict_elem):
    """
    Select the attributes of interest from the comment attribute dict.
    :param dict_elem: dict with parsed XML attributes
    :return: dict with selection of parsed XML attributes
    """
    return {
        'PostId': int(dict_elem['PostId']),
        'Score': dict_elem['Score'],
        'Text': dict_elem['Text']
    }


def extract_comment_text(comment_pair):
    """
    Convert pair of post id, dict with parsed attributes to pair of post_id, list of comment texts
    :param comment_pair: pair of post_id, dict with parsed attributes
    :return: pair of post_id, list of comment texts
    """
    post_id, comment_dicts = comment_pair
    return post_id, list(map(lambda comment_dict: comment_dict["Text"], comment_dicts))


def get_post_id(dict_elem):
    """
    Returns the key property of the attribute dict i.e. the post id
    :param dict_elem: dict with parsed XML attributes
    :return: key element, i.e. the post id
    """
    return int(dict_elem['PostId'])


def get_most_recent_version(post_edits_pair):
    """
    Sorts the dict values of the grouped post edits by creation date in descending order and return the most recent
    dict element i.e. version of the post.
    :param post_edits_pair: pair of post id, list of attribute dict elements
    :return: most recent dict element i.e. post version
    """
    (post_id, post_edits) = post_edits_pair
    post_edits.sort(key=lambda dict_elem: dict_elem['CreationDate'], reverse=True)
    return post_id, post_edits[0]


def extract_text_blocks(post_edit_pair):
    """
    Extracts all text blocks from the Markdown source of a post version
    :param post_edit_pair: pair of post id, attribute dict element of most recent version
    :return: tuple of post_id and text_blocks
    """
    post_id, post_edit = post_edit_pair
    markdown_content = post_edit['Text']
    post_blocks = []

    lines = re.split(newline_regex, markdown_content)

    current_post_block = None
    previous_line = None
    code_block_ends_with_next_line = False

    in_code_tag_block = False
    in_stack_snippet_code_block = False
    in_script_tag_code_block = False
    in_alternative_code_block = False

    for line in lines:
        # ignore empty lines
        if not line:
            previous_line = line
            continue

        # end code block which contained a code tag in the previous line (see below)
        if code_block_ends_with_next_line:
            in_code_tag_block = False
            code_block_ends_with_next_line = False

        # check for indented code blocks (Stack Overflow's standard way)
        # even if tab is not listed here: http://stackoverflow.com/editing-help#code
        # we observed cases where it was important to check for the tab, sometimes preceded by spaces
        in_markdown_code_block = code_block_regex.match(line) is not None  # only match beginning of line
        # check if line only contains whitespaces
        # (ignore whitespaces at the beginning of posts and not end blocks with whitespace lines)
        is_whitespace_line = whitespace_line_regex.fullmatch(line) is not None  # match whole line
        # e.g. "<!-- language: lang-js -->" (see https://stackoverflow.com/editing-help#syntax-highlighting)
        is_snippet_language = snippet_language_regex.fullmatch(line) is not None  # match whole line
        # in some posts an empty XML comment ("<!-- -->") is used to divide code blocks (see, e.g., post 33058542)
        is_snippet_divider = snippet_divider_regex.fullmatch(line) is not None  # match whole line
        # in some cases, there are inline code blocks in a single line (`...`)
        is_inline_code_line = inline_code_line_regex.fullmatch(line) is not None  # match whole line

        # if line is not part of a regular Stack Overflow code block, try to detect alternative code block styles
        if not in_markdown_code_block and not is_whitespace_line and not is_snippet_language:
            # see https://stackoverflow.blog/2014/09/16/introducing-runnable-javascript-css-and-html-code-snippets/
            is_stack_snippet_begin = stack_snippet_begin_regex.match(line) is not None  # only match beginning of line
            # ignore stack snippet begin in post block version
            if is_stack_snippet_begin:
                in_stack_snippet_code_block = True
                # remove stack snippet info from code block
                line = stack_snippet_begin_regex.sub(line, "")
                if not line.strip():  # if string empty after removing leading and trailing whitespaces
                    # line only contained stack snippet begin
                    continue

            is_stack_snippet_end = stack_snippet_end_regex.match(line) is not None  # only match beginning of line
            # ignore stack snippet end in post block version
            if is_stack_snippet_end:
                in_stack_snippet_code_block = False
                # remove stack snippet info from code block
                line = is_stack_snippet_end.sub(line, "")
                if not line.strip():  # if string empty after removing leading and trailing whitespaces
                    # line only contained stack snippet begin
                    continue

            # code block that is marked by <pre><code> ... </pre></code> instead of indention
            is_code_tag_begin = code_tag_begin_regex.match(line) is not None  # only match beginning of line
            if is_code_tag_begin:
                # remove code tag from line
                line = code_tag_begin_regex.sub(line, "")
                in_code_tag_block = True
                if not line.strip():  # if string empty after removing leading and trailing whitespaces
                    # line only contained opening code tags -> skip
                    continue

            is_code_tag_end = code_tag_end_regex.match(line) is not None  # only match beginning of line
            if is_code_tag_end:
                # remove code tag from line
                line = code_tag_end_regex.sub(line, "")
                if not line.strip():  # if string empty after removing leading and trailing whitespaces
                    # line only contained closing code tags -> close code block and skip
                    in_code_tag_block = False
                    continue
                else:
                    # line also contained content -> close code block in next line
                    code_block_ends_with_next_line = True

            # code block that is marked by <script...> ... </script> instead of correct indention
            is_script_tag_begin = script_tag_begin_regex.match(line) is not None  # only match beginning of line
            if is_script_tag_begin:
                # remove opening script tag
                line = script_tag_open_regex.sub(line, "", count=1)
                in_script_tag_code_block = True
                if not line.strip():  # if string empty after removing leading and trailing whitespaces
                    # line only contained opening script tag -> skip
                    continue

            is_script_tag_end = script_tag_end_regex.match(line) is not None # only match beginning of line
            if is_script_tag_end:
                # remove closing script tag
                line = script_tag_close_regex.sub(line, "")
                if not line.strip():  # if string empty after removing leading and trailing whitespaces
                    # line only contained closing script tag -> close code block and skip
                    in_script_tag_code_block = False
                    continue
                else:
                    # line also contained content -> close script block in next line
                    code_block_ends_with_next_line = True

            # see https://meta.stackexchange.com/q/125148
            # example: https://stackoverflow.com/posts/32342082/revisions
            is_alternative_code_block_begin = alternative_code_block_begin_regex.match(line) is not None
            if is_alternative_code_block_begin:
                # remove first "```" from line
                line = alternative_code_block_begin_regex.sub(line, "", count=1)
                in_alternative_code_block = True
                # continue if line only contained "```"
                if not line.strip():  # if string empty after removing leading and trailing whitespaces
                    continue
                else:
                    if alternative_code_block_marker_regex.match(line) is not None:
                        # alternative code block was inline code block (which should be part of a text block)
                        line = alternative_code_block_marker_regex.sub(line, "")
                        in_alternative_code_block = False

            is_alternative_code_block_end = alternative_code_block_end_regex.match(line) is not None
            if is_alternative_code_block_end:
                # remove "```" from line
                line = alternative_code_block_marker_regex.sub(line, "")
                in_alternative_code_block = False

        if is_snippet_language:
            # remove snippet language information
            line = snippet_language_regex.sub(line, "")

        if is_inline_code_line:
            # replace leading and trailing backtick and HTML line break if present
            line = inline_code_line_regex.match(line).group(1)

        # decide if the current line is part of a code block
        in_non_markdown_code_block = (is_snippet_language and not line.strip()) or in_stack_snippet_code_block \
            or in_alternative_code_block or in_code_tag_block or in_script_tag_code_block or is_inline_code_line

        if not current_post_block:  # first block in post
            # ignore whitespaces at the beginning of a post
            if not is_whitespace_line:
                # first line, block element not created yet
                if in_markdown_code_block or in_non_markdown_code_block:
                    current_post_block = CodeBlock(post_id)
                else:
                    current_post_block = TextBlock(post_id)
        else:
            # current block has length > 0 => check if current line belongs to this block
            # or if it is first line of next block
            if isinstance(current_post_block, TextBlock):
                # check if line contains letters or digits (heuristic for malformed post blocks)
                previous_line_contains_letters_or_digits = \
                    contains_letter_or_digit_regex.search(previous_line) is not None

                if ((in_markdown_code_block
                    and (not previous_line.strip() or not previous_line_contains_letters_or_digits))
                        or in_non_markdown_code_block) and not is_whitespace_line:
                    # End of text block, beginning of code block.
                    # Do not end text block if next line is whitespace line
                    # see, e.g., second line of PostHistory, Id=97576027
                    if not current_post_block.is_empty():
                        post_blocks.append(current_post_block)
                    current_post_block = CodeBlock(post_id)

            elif isinstance(current_post_block, CodeBlock):
                # snippet language or snippet divider divide two code blocks ( if first block is not empty)
                if is_snippet_language or is_snippet_divider:
                    if not current_post_block.is_empty():
                        post_blocks.append(current_post_block)
                    current_post_block = CodeBlock(post_id)
                elif (not in_markdown_code_block and not in_non_markdown_code_block) and not is_whitespace_line:
                    # In a Stack Snippet, the lines do not have to be indented (see version 12 of answer
                    # 26044128 and corresponding test case).
                    # Do not close code postBlocks when whitespace line is reached
                    # see, e.g., PostHistory, Id=55158265, PostId=20991163 (-> test case).
                    # Do not end code block if next line is whitespace line
                    # see, e.g., second line of PostHistory, Id=97576027
                    if not current_post_block.is_empty():
                        post_blocks.append(current_post_block)
                    current_post_block = TextBlock(post_id)

        # ignore snippet language information (see https://stackoverflow.com/editing-help#syntax-highlighting)
        if current_post_block and not is_snippet_language:
            current_post_block.append(line)

        previous_line = line

    if current_post_block and not current_post_block.is_empty():
        # last block not added yet
        post_blocks.append(current_post_block)

    _revise_post_blocks(post_blocks)

    return post_id, list(
        map(lambda block: block.content,
            filter(lambda block: isinstance(block, TextBlock),
                   post_blocks)
            )
    )


def _revise_post_blocks(post_blocks):
    marked_for_deletion = set()

    for i in range(len(post_blocks)):
        current_post_block = post_blocks[i]

        # ignore post block if it is already marked for deletion
        if current_post_block in marked_for_deletion:
            continue

        # In some cases when a code blocks ends with a single character, the indention by 4 spaces is missing in
        # the table PostHistory (see, e.g., PostHistoryId=96888165). The following code should prevent most of
        # these cases from being recognized as text blocks.

        # remove this post block if does not contain letters or digits
        contains_letter_or_digit = contains_letter_or_digit_regex.search(current_post_block.content)

        if contains_letter_or_digit:
            continue

        if i == 0:
            # current post block is first one
            if len(post_blocks) > 1:
                next_post_block = post_blocks[i+1]
                next_post_block.prepend(current_post_block.content)
                marked_for_deletion.add(current_post_block)
        else:
            # current post block is not first one (has predecessor)
            previous_post_block = post_blocks[i-1]

            if previous_post_block in marked_for_deletion:
                continue

            previous_post_block.append(current_post_block.content)
            marked_for_deletion.add(current_post_block)

            # current post block must have successor
            if i >= (len(post_blocks) - 1):
                continue

            next_post_block = post_blocks[i+1]

            # merge predecessor and successor if they have same type
            if previous_post_block.__class__ != next_post_block.__class__:
                continue

            previous_post_block.append(next_post_block.content)
            marked_for_deletion.add(next_post_block)

    # remove post blocks marked for deletion
    for current_post_block in marked_for_deletion:
        post_blocks.remove(current_post_block)


def add_comments(text_blocks_pair, comments):
    """
    Add comments to tuple of post_id and text blocks
    :param text_blocks_pair: pair of post id and list of text blocks
    :param comments: dict with comments per post id
    :return: tuple of post_id and text_blocks
    """
    post_id, text_blocks = text_blocks_pair
    post_comments = comments[post_id]
    return post_id, {
        "text_blocks": text_blocks,
        "comments": post_comments
    }


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
