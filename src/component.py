'''
Template Component main class.

'''

import datetime  # noqa
import logging
import os
import sys
from pathlib import Path

import dateparser
import logging_gelf.formatters
import logging_gelf.handlers
from azure.storage.blob import ContainerClient
from kbc.env_handler import KBCEnvHandler
from kbc.result import KBCTableDef  # noqa
from kbc.result import ResultWriter  # noqa

# configuration variables
KEY_ACCOUNT_NAME = 'account_name'
KEY_ACCOUNT_KEY = '#account_key'
KEY_CONTAINER_NAME = 'container_name'
KEY_DESTINATION_PATH = 'destination_path'
KEY_APPEND_DATE_TO_FILE = 'append_date_to_file'
KEY_BLOB_DOMAIN = 'blob_domain'

DEFAULT_BLOB_DOMAIN = 'blob.core.windows.net'

MANDATORY_PARS = [
    KEY_ACCOUNT_NAME,
    KEY_ACCOUNT_KEY,
    KEY_CONTAINER_NAME,
    KEY_DESTINATION_PATH,
    KEY_APPEND_DATE_TO_FILE
]
MANDATORY_IMAGE_PARS = []

# Default Table Output Destination
DEFAULT_TABLE_SOURCE = "/data/in/tables/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/files/"
DEFAULT_FILE_SOURCE = "/data/in/files/"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-8s : [line:%(lineno)3s] %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

# Disabling list of libraries you want to output in the logger
disable_libraries = [
    'azure.storage.common.storageclient'
]
for library in disable_libraries:
    logging.getLogger(library).disabled = True

if 'KBC_LOGGER_ADDR' in os.environ and 'KBC_LOGGER_PORT' in os.environ:
    logger = logging.getLogger()
    logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
        host=os.getenv('KBC_LOGGER_ADDR'), port=int(os.getenv('KBC_LOGGER_PORT')))
    logging_gelf_handler.setFormatter(
        logging_gelf.formatters.GELFFormatter(null_character=True))
    logger.addHandler(logging_gelf_handler)

    # remove default logging to stdout
    logger.removeHandler(logger.handlers[0])

APP_VERSION = '0.0.4'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS, data_path=self._get_data_folder_override_path())
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config()
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.error(e)
            exit(1)

    def validate_config_params(self, params, in_tables):
        '''
        Validating if input configuration contain everything needed
        '''

        # Validate if config is blank
        if params == {}:
            logging.error(
                'Configurations are missing. Please configure your component.')
            sys.exit(1)
        elif params[KEY_ACCOUNT_KEY] == '' and params[KEY_ACCOUNT_NAME] == '' and params[KEY_CONTAINER_NAME] == '':
            logging.error(
                'Configurations are missing. Please configure your component.')
            sys.exit(1)

        # Credentials Conditions
        if params[KEY_ACCOUNT_KEY] == '' or params[KEY_ACCOUNT_NAME] == '':
            logging.error(
                "Please enter your credentials: Account Name, Account Key")
            sys.exit(1)
        if params[KEY_CONTAINER_NAME] == '':
            logging.error("Please enter your Container Name")
            sys.exit(1)
        if len(in_tables) == 0:
            logging.error(
                "There are not tables found in the Input Mapping. " +
                "Please add tables you would like to export into Azure Blob Storage."
            )
            sys.exit(1)

    def validate_blob_container(self, blob_obj, container_name):
        '''
        Validating if input container exists in the Blob Storage
        '''

        # List all containers for this account
        # & Determine if the input container is available
        # & Validate if the entered account has the right credentials and privileges
        try:
            blob_obj.get_account_information()
        except Exception:
            logging.error(
                'Authorization Error. Please validate your credentials.')
            sys.exit(1)

        return True

    def get_tables(self, tables, mapping):
        """
        Evaluate input and output table names.
        Only taking the first one into consideration!
        mapping: input_mapping, output_mappings
        """
        # input file
        table_list = []
        for table in tables:
            # name = table["full_path"]
            if mapping == "input_mapping":
                destination = table["destination"]
            elif mapping == "output_mapping":
                destination = table["source"]
            table_list.append(destination)

        return table_list

    def run(self):
        '''
        Main execution code
        '''
        # Loading input mapping
        in_tables = self.configuration.get_input_tables()
        in_table_names = self.get_tables(in_tables, 'input_mapping')
        logging.info("IN tables mapped: " + str(in_table_names))

        # Loading input configurations
        params = self.cfg_params  # noqa
        # Validate configuration parameters
        self.validate_config_params(params, in_tables)

        # Get proper list of parameters
        account_name = params.get(KEY_ACCOUNT_NAME)
        account_key = params.get(KEY_ACCOUNT_KEY)
        container_name = params.get(KEY_CONTAINER_NAME)

        # Append date parameters into the output file name
        # Destination parameter
        path_destination = params.get('destination_path')
        if path_destination != '' and '/' not in path_destination and path_destination[-1] != '/':
            # Adding '/' backslash if not found at the end of the path_destination
            path_destination = '{}/'.format(path_destination)
        # Date parameter
        append_value = ''  # will be used to append into the output file name
        append_date_to_file = params.get('append_date_to_file')
        if append_date_to_file:
            logging.info('Append date to file: Enabled')
            today_raw = dateparser.parse('today')
            today_formatted = today_raw.strftime('%Y_%m_%d')
            append_value = '-{}'.format(today_formatted)

        '''
        AZURE BLOB STORAGE
        '''
        blob_domain = params.get(KEY_BLOB_DOMAIN, DEFAULT_BLOB_DOMAIN)
        account_url = f'{account_name}.{blob_domain}'
        # Create the BlocklobService that is used to call the Blob service for the storage account
        block_blob_service = ContainerClient(
            account_url=account_url,
            container_name=container_name,
            credential=account_key
        )

        # Validate input container name
        self.validate_blob_container(blob_obj=block_blob_service, container_name=container_name)

        # Uploading files to Blob Storage
        for table in in_tables:
            table_name = '{}{}{}.csv'.format(
                path_destination,  # folder path
                table['destination'].split('.csv')[0],  # file name
                append_value)  # custom date value
            logging.info('Uploading [{}]...'.format(table_name))
            try:
                block_blob_service.upload_blob(
                    # blob_name=table['destination'],
                    name=table_name,
                    data=open(table['full_path'], 'rb')
                )
            except Exception as e:
                logging.error('There is an issue with uploading [{}]'.format(
                    table['destination']))
                logging.error('Error message: {}'.format(e))
                sys.exit(1)

        logging.info("Blob Storage Writer finished")

    def _get_default_data_path(self) -> str:
        """
        Returns default data_path, by default `../data` is used, relative to working directory.
        This helps with local development.
        Returns:
        """
        return Path(os.getcwd()).resolve().parent.joinpath('data').as_posix()

    def _get_data_folder_override_path(self, data_path_override: str = None) -> str:
        """
        Returns overridden value of the data_folder_path in case the data_path_override variable
        or `KBC_DATADIR` environment variable is defined. The `data_path_override` variable takes precendence.
        Returns null if override is not in place.
        Args:
            data_path_override:
        Returns:
        """
        data_folder_path = None
        if data_path_override:
            data_folder_path = data_path_override
        elif not os.environ.get('KBC_DATADIR'):
            data_folder_path = self._get_default_data_path()
        return data_folder_path


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug = sys.argv[1]
    else:
        debug = True
    comp = Component(debug)
    comp.run()
