'''
Template Component main class.

'''

import logging
import logging_gelf.handlers
import logging_gelf.formatters
import sys
import os
import datetime  # noqa
import dateparser

from kbc.env_handler import KBCEnvHandler
from kbc.result import KBCTableDef  # noqa
from kbc.result import ResultWriter  # noqa

from azure.storage.blob import BlockBlobService, PublicAccess  # noqa


# configuration variables
KEY_ACCOUNT_NAME = 'account_name'
KEY_ACCOUNT_KEY = 'account_key'
KEY_SAS_TOKEN = 'sas_token'
KEY_CONTAINER_NAME = 'container_name'
KEY_DESTINATION_PATH = 'destination_path'
KEY_APPEND_DATE_TO_FILE = 'append_date_to_file'

MANDATORY_PARS = [
    KEY_ACCOUNT_NAME,
    KEY_ACCOUNT_KEY,
    KEY_SAS_TOKEN,
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

if 'KBC_LOGGER_ADDR' in os.environ and 'KBC_LOGGER_PORT' in os.environ:

    logger = logging.getLogger()
    logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
        host=os.getenv('KBC_LOGGER_ADDR'), port=int(os.getenv('KBC_LOGGER_PORT')))
    logging_gelf_handler.setFormatter(
        logging_gelf.formatters.GELFFormatter(null_character=True))
    logger.addHandler(logging_gelf_handler)

    # remove default logging to stdout
    logger.removeHandler(logger.handlers[0])

APP_VERSION = '0.0.1'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config()
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.error(e)
            exit(1)

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
        logging.info("IN tables mapped: "+str(in_table_names))

        # Loading input configurations
        params = self.cfg_params  # noqa
        account_name = params.get('account_name')
        account_key = params.get('#account_key')
        container_name = params.get('container_name')

        # Credentials Conditions
        if account_key == '' or account_name == '':
            logging.error(
                "Please enter your credentials: Account Name, Account Key...")
            sys.exit(1)
        if container_name == '':
            logging.error("Please enter your Container Name...")
            sys.exit(1)
        if len(in_tables) == 0:
            logging.error(
                "There are not tables founf in the Input Mapping. " +
                "Please add tables you would like to export into Azure Blob Storage."
            )
            sys.exit(1)

        # Append date parameters into the output file name
        # destination
        path_destination = params.get('destination_path')
        if path_destination != '' and '/' not in path_destination and path_destination[-1] != '/':
            logging.error(
                'Please validate [Path Destination]. Backslash [/] is not found.')
            sys.exit(1)
        # date
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
        # Create the BlocklobService that is used to call the Blob service for the storage account
        block_blob_service = BlockBlobService(
            account_name=account_name, account_key=account_key)

        # List all containers for this account
        # & Determine if the input container is available
        container_generator = block_blob_service._list_containers()
        list_of_containers = []
        for i in container_generator:
            list_of_containers.append(i.name)
        logging.info("Available Containers: {}".format(list_of_containers))
        if container_name not in list_of_containers:
            logging.error(
                "Entered Container does not exist: {}".format(container_name))
            logging.error(
                "Please validate your input container or create a new container in Blob Storage.")
            sys.exit(1)

        # Uploading files to Blob Storage
        for table in in_tables:
            table_name = '{}{}{}.csv'.format(
                path_destination,  # folder path
                table['destination'].split('.csv')[0],  # file name
                append_value)  # custom date value
            logging.info('Uploading [{}]...'.format(table_name))
            try:
                block_blob_service.create_blob_from_path(
                    container_name=container_name,
                    # blob_name=table['destination'],
                    blob_name=table_name,
                    file_path=table['full_path']
                )
            except Exception as e:
                logging.error('There is an issue with uploading [{}]'.format(
                    table['destination']))
                logging.error('Error message: {}'.format(e))
                sys.exit(1)

        logging.info("Blob Storage Writer finished")


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
