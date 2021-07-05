import copy
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
            self.output_dir = self.pipeline['output_dir']
            self.input_paths = dict()
            self.output_paths = dict()
            self.datasets = json_config['datasets']
            self._generate_file_paths()
        logger.info("Configuration initialized.")

    def _generate_file_paths(self):
        """
        Generate file paths for configured input and output table files.
        :return: None
        """
        for dataset in self.datasets:
            logger.info(f"Generating input and output paths for dataset '{dataset}'...")
            self.input_paths[dataset] = {
                "posts": os.path.join(self.pipeline['input_dir'], dataset, 'Posts.xml'),
                "comments": os.path.join(self.pipeline['input_dir'], dataset, 'Comments.xml'),
                "post_history": os.path.join(self.pipeline['input_dir'], dataset, 'PostHistory.xml')
            }
            self.output_paths[dataset] = os.path.join(self.pipeline['output_dir'], f'{dataset}.jsonl')
        logger.info(f"Generated {len(self.input_paths)} input paths and {len(self.output_paths)} output paths.")

    def get_pipeline_options(self, dataset):
        """
        Get pipeline options for active pipeline
        :param dataset: Name of currently processed dataset
        :return:
        """
        logger.info(f"Generating pipeline options for dataset '{dataset}'...")
        pipeline_options_dict = copy.deepcopy(self.pipeline['pipeline_options'])
        pipeline_options_dict['job_name'] = f"{pipeline_options_dict['job_name']}-{str(dataset).lower()}"
        pipeline_options = PipelineOptions.from_dictionary(pipeline_options_dict)
        pipeline_options.view_as(SetupOptions).setup_file = self.setup_file
        if self.save_main_session:
            pipeline_options.view_as(SetupOptions).save_main_session = True
        logger.info(f"Pipeline options for dataset '{dataset}' generated.")
        return pipeline_options
