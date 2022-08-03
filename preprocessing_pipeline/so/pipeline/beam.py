import itertools
import json
import logging
import os
import sys

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

    for dataset, input_paths in config.input_paths.items():
        output_path = config.output_paths[dataset]
        logger.info(f"Reading and converting XML files of dataset '{dataset}'...")
        with beam.Pipeline(options=config.get_pipeline_options(dataset)) as p:
            question_metadata = (p
                                 | "Read posts XML file (questions)" >> beam.io.ReadFromText(input_paths["posts"])
                                 | "Ignore non-row post elements (questions)" >> beam.Filter(filter_rows)
                                 | "Convert post XML attributes to dict elements (questions)" >> beam.Map(
                        xml_attributes_to_dict)
                                 | "Filter questions" >> beam.Filter(filter_questions)
                                 | "Extract relevant question attributes" >> beam.Map(extract_question_attributes)
                                 | "Group questions by post id" >> beam.GroupBy(get_post_id)
                                 | "Filter java tags" >> beam.Filter(filter_java_tags)
                                 | "Flatten question dict" >> beam.Map(flatten_question)
                                 | "Remove tags" >> beam.Map(remove_tags))

            answer_metadata = (p
                               | "Read posts XML file (answers)" >> beam.io.ReadFromText(input_paths["posts"])
                               | "Ignore non-row post elements (answers)" >> beam.Filter(filter_rows)
                               | "Convert post XML attributes to dict elements (answers)" >> beam.Map(
                        xml_attributes_to_dict)
                               | "Filter answers" >> beam.Filter(filter_answers)
                               | "Extract relevant answer attributes" >> beam.Map(extract_answer_attributes)
                               | "Group answers by post id" >> beam.GroupBy(get_post_id)
                               | "Flatten answer dict" >> beam.Map(flatten_answer))

            post_metadata = (({
                'answer_metadata': answer_metadata, 'question_metadata': question_metadata
            })
                             | "Merge answer and question metadata" >> beam.CoGroupByKey()
                             | "Filter out non-java entries" >> beam.Filter(filter_java_post_pairs)
                             | "Flatten grouped post metadata" >> beam.Map(flatten_post_metadata)
                             | "Flatten post lists" >> beam.FlatMap(get_list_elements)
                             )

            post_code_blocks = (p
                                | "Read post history XML file" >> beam.io.ReadFromText(input_paths["post_history"])
                                | "Ignore non-row post history elements" >> beam.Filter(filter_rows)
                                | "Convert post history XML attributes to dict elements" >> beam.Map(
                        xml_attributes_to_dict)
                                | "Extract relevant post history attributes" >> beam.Map(
                        extract_post_history_attributes)
                                | "Filter post edits" >> beam.Filter(filter_post_edits)
                                | "Remove PostHistoryTypeId column" >> beam.Map(remove_post_history_id)
                                | "Extract code blocks" >> beam.Map(extract_code_blocks)
                                | "Remove empty code blocks" >> beam.Filter(filter_nonempty_blocks)
                                | "Group post history by post id" >> beam.GroupBy(get_post_id)
                                | "Get most recent version for each post id" >> beam.Map(get_most_recent_version)
                                | "Remove CreationDate column" >> beam.Map(remove_creation_date)
                                | "Strip code blocks" >> beam.Filter(strip_code_blocks)
                                )

            output_path_without_ext, _ = os.path.splitext(output_path)
            logger.info(f"Writing data to JSONL file '{output_path}'")
            (({
                'post_metadata': post_metadata, 'post_code_blocks': post_code_blocks
            })
             | "Merge post scores and code blocks" >> beam.CoGroupByKey()
             | "Filter empty post metadata" >> beam.Filter(filter_empty_post_metadata)
             | "Remove empty entries" >> beam.Filter(remove_empty_posts)
             | "Flatten grouped post data" >> beam.Map(flatten_post_data)
             | "Write code blocks to JSONL file" >> WriteToJson(output_path_without_ext))

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


def remove_post_history_id(dict_elem):
    """
    Once columns a not longer necessary, stash them
    """
    if 'Text' in dict_elem:
        text = dict_elem['Text']
    else:
        text = ""

    return {
        'PostId': int(dict_elem['PostId']),
        'CreationDate': dict_elem['CreationDate'],
        'Text': text,
    }


def filter_questions(dict_elem):
    """
    Filter question posts.
    :param dict_elem: dict with parsed XML attributes
    :return: boolean indicating whether the post is a question
    """
    return int(dict_elem['PostTypeId']) == 1


def filter_answers(dict_elem):
    """
    Filter answer posts.
    :param dict_elem: dict with parsed XML attributes
    :return: boolean indicating whether the post is an answer
    """
    return int(dict_elem['PostTypeId']) == 2


def filter_score(dict_elem):
    """
    Filter post/comment elements with a positive score.
    :param dict_elem: dict with parsed XML attributes
    :return: boolean indicating whether post has a positive score
    """
    return int(dict_elem['Score']) > 0


def filter_java_tags(post_pair):
    return '<java>' in post_pair[1][0]["Tags"]


def filter_nonempty_blocks(post_dict):
    """
    Filters out code_blocks that have no snippets
    :param post_dict: post dictionary
    :return: boolean indicating whether post has snippets
    """

    return len(post_dict['code_blocks']) != 0


def strip_code_blocks(post_pair):
    """
    Attempt to clean up data as much as possible before the heavy step
    "Merge post scores and code blocks" >> beam.CoGroupByKey() is invoked
    """
    _, code_blocks = post_pair
    code_blocks = [block.strip() for block in code_blocks]
    return _, code_blocks


def filter_java_post_pairs(post_metadata_group):
    """
    Remove question/answer pairs that are not related to Java.
    The "Merge answer and question metadata" >> beam.CoGroupByKey() step matches questions and answers. However,
    the "Filter java tags" >> beam.Filter(filter_java_tags) used to generate question_metadata causes the
    "Merge answer and question metadata" >> beam.CoGroupByKey() step to generate entries whose question_metadata list
    is empty, which is not accepted by the "Flatten grouped post metadata" >> beam.Map(flatten_post_metadata) step.
    e.g.: [5304668, {'answer_metadata': [{'post_id': 9875710, 'score': '0'}], 'question_metadata': []}] is a JS, and
    not a Java question/answer pair

    Further down the line, another transformation to remove empty post_metadata will be necessary
    """
    return len(post_metadata_group[1]["question_metadata"]) != 0


def flatten_post_metadata(post_metadata_group):
    """
    Flatten lists in grouped post metadata.
    :param post_group: List with thread metadata (parent id) and lists containing post id, post score and tags
    :return: pair of post_id and dict with post score and tags
    """
    post_metadata_group = list(post_metadata_group)

    if len(post_metadata_group) != 2:
        error_message = f"Post metadata group did not have correct format (more than two root elements): {post_metadata_group}"
        logger.error(error_message)
        sys.exit(error_message)

    parent_id = int(post_metadata_group[0])
    metadata_dict = dict(post_metadata_group[1])

    if 'answer_metadata' not in metadata_dict or 'question_metadata' not in metadata_dict:
        error_message = f"Post metadata group did not have correct format (key missing): {post_metadata_group}"
        logger.error(error_message)
        sys.exit(error_message)

    answer_metadata_list = list(metadata_dict['answer_metadata'])
    question_metadata_list = list(metadata_dict['question_metadata'])

    if len(question_metadata_list) != 1:
        error_message = f"Post metadata group did not have correct format (invalid number of questions): {post_metadata_group}"
        logger.error(error_message)
        sys.exit(error_message)

    question_metadata = dict(question_metadata_list[0])
    result_list = []

    for answer_metadata in answer_metadata_list:
        post_id = int(answer_metadata['post_id'])
        post_type = "a"
        result_list.append(
            (post_id, [parent_id, post_type, answer_metadata['score']])
        )

    result_list.append(
        (parent_id, [parent_id, "q", question_metadata['score']])
    )

    return result_list


def get_list_elements(input_list):
    return input_list


def flatten_post_data(post_group):
    """
    Flatten lists in grouped post data.
    :param post_group: List with post metadata and lists containing post score, tags, and code blocks
    :return: pair of post_id and dict with post score, tags, and code blocks.
    """
    post_group = list(post_group)

    if len(post_group) != 2:
        error_message = f"Post group did not have correct format (more than two root elements): {post_group}"
        logger.error(error_message)
        sys.exit(error_message)

    post_id = int(post_group[0])
    post_dict = dict(post_group[1])

    if len(post_dict["post_code_blocks"]) == 1:
        code_blocks = post_dict["post_code_blocks"][0]['code_blocks']
    else:
        code_blocks = []


    if 'post_metadata' not in post_dict or 'post_code_blocks' not in post_dict:
        error_message = f"Post group did not have correct format (key missing): {post_group}"
        logger.error(error_message)
        sys.exit(error_message)

    post_metadata_list = list(itertools.chain(*post_dict['post_metadata']))


    if len(post_metadata_list) != 3:
        error_message = f"Post group did not have correct format (post metadata has wrong size): {post_group}"
        logger.error(error_message)
        sys.exit(error_message)

    parent_id = int(post_metadata_list[0])
    post_type = post_metadata_list[1]
    post_score = int(post_metadata_list[2])
    # post_tags = post_metadata_list[3]

    return post_id, {
        'parent_id': parent_id,
        'post_type': post_type,
        'score': post_score,
        # 'tags': post_tags,
        'code_blocks': [block.strip() for block in code_blocks]
    }


def filter_empty_post_metadata(post_pair):
    """
    Upon merging, the data from post_history that is not related to Java gets populated with an empty post_metadata list
    so we remove them
    """
    post_id, post_data = post_pair
    return len(post_data['post_metadata']) != 0


def remove_empty_posts(post_pair):
    """
    Although it looks a bit reduntant, this step is necessary, because the merger from PostHistory data generates
    empty entries here again
    """
    return len(post_pair[1]['post_code_blocks']) != 0



def filter_score_grouped_pair(post_pair):
    """
    Filter posts with a positive score.
    :param post_pair: pair of post_id, dict with score, text blocks, and comments
    :return: boolean indicating whether post has a positive score
    """
    _, post_dict = post_pair
    post_score = post_dict['score']
    return post_score and int(post_score) > 0


def extract_post_history_attributes(dict_elem):
    """
    Select the attributes of interest from the post history attribute dict.
    :param dict_elem: dict with parsed XML attributes
    :return: dict with selection of parsed XML attributes
    """
    if 'Text' in dict_elem:
        text = dict_elem['Text']
    else:
        text = ""

    return {
        'PostId': int(dict_elem['PostId']),
        'CreationDate': dict_elem['CreationDate'],
        'Text': text,
        'PostHistoryTypeId': int(dict_elem['PostHistoryTypeId'])
    }


def extract_answer_attributes(dict_elem):
    """
    Select the attributes of interest from the post attribute dict.
    :param dict_elem: dict with parsed XML attributes
    :return: dict with selection of parsed XML attributes
    """
    return {
        'PostId': int(dict_elem['Id']),
        'ParentId': int(dict_elem['ParentId']),
        'Score': dict_elem['Score'],
    }


def extract_question_attributes(dict_elem):
    """
    Select the attributes of interest from the post attribute dict.
    :param dict_elem: dict with parsed XML attributes
    :return: dict with selection of parsed XML attributes
    """
    return {
        'PostId': int(dict_elem['Id']),
        'Tags': dict_elem['Tags'],
        'Score': dict_elem['Score'],
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
    return post_id, list(map(lambda comment_dict: comment_dict['Text'], comment_dicts))


def flatten_answer(post_pair):
    """
    Use parent id as left-hand side of the pair.
    Reduce the right-hand side of the pair to the score (without post id).
    :param post_pair: Pair of post_id, dict with parse attributes
    :return: pair of parent_id, [post_id, score]
    """
    post_id, post_dicts = post_pair
    post_dict = list(post_dicts)[0]
    return post_dict['ParentId'], {
        "post_id": post_id,
        "score": post_dict['Score']
    }


def flatten_question(post_pair):
    """
    Use post id as left-hand side of the pair.
    Reduce the right-hand side of the pair to the tags (without post id).
    :param post_pair: Pair of post_id, dict with parse attributes
    :return: pair of post_id, [tags]
    """
    post_id, post_dicts = post_pair
    post_dict = list(post_dicts)[0]
    return post_id, {
        "tags": post_dict['Tags'],
        "score": post_dict['Score']
    }


def remove_tags(post_pair):
    post_id, post_dict = post_pair
    post_dict.pop('tags')
    return post_id, post_dict


def get_post_id(dict_elem):
    """
    Returns the key property of the attribute dict i.e. the post id
    :param dict_elem: dict with parsed XML attributes
    :return: key element, i.e. the post id
    """
    return int(dict_elem['PostId'])


def get_tuple_post_id(tuple_elem):
    return int(tuple_elem[0])


def get_most_recent_version(post_edits_pair):
    """
    Sorts the dict values of the grouped post edits by creation date in descending order and return the most recent
    dict element i.e. version of the post.
    :param post_edits_pair: pair of post id, list of attribute dict elements
    :return: most recent dict element i.e. post version
    """
    (post_id, post_edits) = post_edits_pair
    post_edits = list(post_edits)
    post_edits.sort(key=lambda dict_elem: dict_elem['CreationDate'], reverse=True)
    return post_id, post_edits[0]


def remove_creation_date(post_edit_pair):
    """
    Once columns a not longer necessary, stash them
    """
    post_id, post_edit = post_edit_pair
    post_edit.pop('CreationDate')
    return post_id, post_edit


def extract_code_blocks(post_data):
    """
    Extracts all code blocks from the Markdown source of a post version
    :param post_edit_pair: pair of post id, attribute dict element of most recent version
    :return: tuple of post_id and text_blocks
    """
    # post_id, post_edit = post_edit_pair
    post_id = post_data['PostId']
    creation_date = post_data['CreationDate']
    markdown_content = post_data['Text']
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
            # ignore stack snippet begin in post block version
            if stack_snippet_begin_regex.match(line):  # only match beginning of line
                in_stack_snippet_code_block = True
                # remove stack snippet info from code block
                line = stack_snippet_begin_regex.sub("", line)
                if not line.strip():  # if string empty after removing leading and trailing whitespaces
                    # line only contained stack snippet begin
                    continue

            # ignore stack snippet end in post block version
            if stack_snippet_end_regex.match(line):  # only match beginning of line
                in_stack_snippet_code_block = False
                # remove stack snippet info from code block
                line = stack_snippet_end_regex.sub("", line)
                if not line.strip():  # if string empty after removing leading and trailing whitespaces
                    # line only contained stack snippet begin
                    continue

            # code block that is marked by <pre><code> ... </pre></code> instead of indention
            if code_tag_begin_regex.match(line):  # only match beginning of line
                # remove code tag from line
                line = code_tag_begin_regex.sub("", line)
                in_code_tag_block = True
                if not line.strip():  # if string empty after removing leading and trailing whitespaces
                    # line only contained opening code tags -> skip
                    continue

            if code_tag_end_regex.match(line):  # only match beginning of line
                # remove code tag from line
                line = code_tag_end_regex.sub("", line)
                if not line.strip():  # if string empty after removing leading and trailing whitespaces
                    # line only contained closing code tags -> close code block and skip
                    in_code_tag_block = False
                    continue
                else:
                    # line also contained content -> close code block in next line
                    code_block_ends_with_next_line = True

            # code block that is marked by <script...> ... </script> instead of correct indention
            if script_tag_begin_regex.match(line):  # only match beginning of line
                # remove opening script tag
                line = script_tag_open_regex.sub("", line, count=1)
                in_script_tag_code_block = True
                if not line.strip():  # if string empty after removing leading and trailing whitespaces
                    # line only contained opening script tag -> skip
                    continue

            if script_tag_end_regex.match(line):  # only match beginning of line
                # remove closing script tag
                line = script_tag_close_regex.sub("", line)
                if not line.strip():  # if string empty after removing leading and trailing whitespaces
                    # line only contained closing script tag -> close code block and skip
                    in_script_tag_code_block = False
                    continue
                else:
                    # line also contained content -> close script block in next line
                    code_block_ends_with_next_line = True

            # see https://meta.stackexchange.com/q/125148
            # example: https://stackoverflow.com/posts/32342082/revisions
            if alternative_code_block_begin_regex.match(line):  # only match beginning of line
                # remove first "```" from line
                line = alternative_code_block_begin_regex.sub("", line, count=1)
                in_alternative_code_block = True
                # continue if line only contained "```"
                if not line.strip():  # if string empty after removing leading and trailing whitespaces
                    continue
                else:
                    if alternative_code_block_marker_regex.match(line):
                        # alternative code block was inline code block (which should be part of a text block)
                        line = alternative_code_block_marker_regex.sub("", line)
                        in_alternative_code_block = False

            if alternative_code_block_end_regex.match(line):  # only match beginning of line
                # remove "```" from line
                line = alternative_code_block_marker_regex.sub("", line)
                in_alternative_code_block = False

        if is_snippet_language:
            # remove snippet language information
            line = snippet_language_regex.sub("", line)

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

    # return post_id, creation_date, list(
    #     map(lambda block: block.content,
    #         filter(lambda block: isinstance(block, CodeBlock),
    #                post_blocks),
    #         )
    # )
    return {
        'PostId': post_id,
        'CreationDate': creation_date,
        'code_blocks': list(map(lambda block: block.content, filter(lambda block: isinstance(block, CodeBlock),
                                                                    post_blocks),
                                )
                            )
    }


def _revise_post_blocks(post_blocks):
    post_blocks = list(post_blocks)
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
                next_post_block = post_blocks[i + 1]
                next_post_block.prepend(current_post_block.content)
                marked_for_deletion.add(current_post_block)
        else:
            # current post block is not first one (has predecessor)
            previous_post_block = post_blocks[i - 1]

            if previous_post_block in marked_for_deletion:
                continue

            previous_post_block.append(current_post_block.content)
            marked_for_deletion.add(current_post_block)

            # current post block must have successor
            if i >= (len(post_blocks) - 1):
                continue

            next_post_block = post_blocks[i + 1]

            # merge predecessor and successor if they have same type
            if previous_post_block.__class__ != next_post_block.__class__:
                continue

            previous_post_block.append(next_post_block.content)
            marked_for_deletion.add(next_post_block)

    # remove post blocks marked for deletion
    for current_post_block in marked_for_deletion:
        post_blocks.remove(current_post_block)


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
