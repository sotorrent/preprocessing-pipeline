import json
import logging
import os
import sys

from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

LOG_LEVEL = logging.INFO
logger = logging.getLogger(__name__)


class Config:
    def __init__(self, config_file):
        logger.info("Initializing configuration...")
        with open(config_file, mode='r', encoding='utf-8') as fp:
            json_config = json.loads(fp.read())
            self.setup_file = json_config['setup_file']
            if not os.path.isfile(self.setup_file):
                logger.error(f"Setup file not found: {self.setup_file}")
                logger.error("Exiting...")
                sys.exit(-1)
            self.google_credentials_json_file = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            self.save_main_session = json_config['save_main_session']
            self.pipeline = json_config['pipeline']
            self.input_dir = self.pipeline['input_dir']
            self.post_history_xml = os.path.join(self.input_dir, 'PostHistory.xml')
            self.posts_xml = os.path.join(self.input_dir, 'Posts.xml')
            self.comments_xml = os.path.join(self.input_dir, 'Comments.xml')
            self.output_dir = self.pipeline['output_dir']
            self.output_jsonl = os.path.join(self.output_dir, 'output.jsonl')
        logger.info("Configuration initialized.")

    def get_pipeline_options(self):
        """
        Get configured pipeline options
        :return:
        """
        logger.info(f"Generating pipeline options...")
        pipeline_options = PipelineOptions.from_dictionary(self.pipeline['pipeline_options'])
        pipeline_options.view_as(SetupOptions).setup_file = self.setup_file
        if self.save_main_session:
            pipeline_options.view_as(SetupOptions).save_main_session = True
        logger.info(f"Pipeline options generated.")
        return pipeline_options
