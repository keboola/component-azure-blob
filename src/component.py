'''
Template Component main class.

'''

import datetime  # noqa
import logging
import os
import sys
from pathlib import Path

import dateparser
from azure.storage.blob import ContainerClient
from kbc.env_handler import KBCEnvHandler
from kbc.result import KBCTableDef  # noqa
from kbc.result import ResultWriter  # noqa
from kbcstorage.workspaces import Workspaces

# configuration variables
KEY_AUTH_TYPE = "auth_type"

KEY_ACCOUNT_NAME = 'account_name'
KEY_ACCOUNT_KEY = '#account_key'

KEY_WORKSPACE_ID = "workspace_id"
KEY_STORAGE_TOKEN = "#storage_token"

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

WORKSPACE_AUTH_TYPE = "Workspace Credentials"
AZURE_AUTH_TYPE = "Azure Credentials"


class UserException(Exception):
    pass


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS, data_path=self._get_data_folder_override_path())
        logging.info('Loading configuration...')

        try:
            self.validate_config()
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.error(e)
            exit(1)

    def run(self):
        '''
        Main execution code
        '''
        # Loading input mapping
        in_tables = self.configuration.get_input_tables()
        in_table_names = self.get_tables(in_tables, 'input_mapping')
        logging.info(f"IN tables mapped: {str(in_table_names)}")

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
            path_destination = f'{path_destination}/'
        # Date parameter
        append_value = ''  # will be used to append into the output file name
        if append_date_to_file := params.get('append_date_to_file'):
            logging.info('Append date to file: Enabled')
            today_raw = dateparser.parse('today')
            today_formatted = today_raw.strftime('%Y_%m_%d')
            append_value = f'-{today_formatted}'

        '''
        AZURE BLOB STORAGE
        '''
        blob_domain = params.get(KEY_BLOB_DOMAIN, DEFAULT_BLOB_DOMAIN)
        account_url = f'{account_name}.{blob_domain}'
        # Create the BlocklobService that is used to call the Blob service for the storage account
        logger = logging.getLogger("empty_logger")
        logger.disabled = True

        if params.get(KEY_AUTH_TYPE, AZURE_AUTH_TYPE) == WORKSPACE_AUTH_TYPE:
            workspace_token = params.get(KEY_STORAGE_TOKEN)
            workspace_id = params.get(KEY_WORKSPACE_ID)
            workspace_client = Workspaces(f'https://{os.environ.get("KBC_STACKID")}', workspace_token)
            account_key = self._refresh_abs_container_token(workspace_client, workspace_id)

        block_blob_service = ContainerClient(
            account_url=account_url,
            container_name=container_name,
            credential=account_key,
            logger=logger
        )

        # Validate input container name
        self.validate_blob_container(blob_obj=block_blob_service, container_name=container_name)

        # Uploading files to Blob Storage
        for table in in_tables:
            table_name = f"{path_destination}{table['destination'].split('.csv')[0]}{append_value}.csv"

            logging.info(f'Uploading [{table_name}]...')
            try:
                block_blob_service.upload_blob(
                    # blob_name=table['destination'],
                    name=table_name,
                    data=open(table['full_path'], 'rb'),
                    overwrite=True
                )
            except Exception as e:
                logging.error(f"There is an issue with uploading [{table['destination']}]")
                logging.error(f'Error message: {e}')
                sys.exit(1)
        logging.info("Blob Storage Writer finished")

    @staticmethod
    def validate_config_params(params, in_tables):
        """
        Validating if input configuration contain everything needed
        """

        # Validate if config is blank
        if params == {}:
            raise UserException('Configurations are missing. Please configure your component.')

        # Validating config parameters
        if not params.get(KEY_ACCOUNT_NAME):
            raise UserException("Credientials missing: Account Name")

        if params.get(KEY_AUTH_TYPE, AZURE_AUTH_TYPE) == AZURE_AUTH_TYPE and not params.get(KEY_ACCOUNT_KEY):
            raise UserException("Credientials missing: Access Key.")

        if not params.get(KEY_CONTAINER_NAME):
            raise UserException("Blob Container name is missing, check your configuration.")
        if len(in_tables) == 0:
            raise UserException(
                "There are not tables found in the Input Mapping. " +
                "Please add tables you would like to export into Azure Blob Storage."
            )

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
            else:
                raise UserException("Mapping set incorrectly")
            table_list.append(destination)

        return table_list

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

    @staticmethod
    def _refresh_abs_container_token(workspace_client: Workspaces, workspace_id: str) -> str:
        ps = workspace_client.reset_password(workspace_id)
        return ps['connectionString'].split('SharedAccessSignature=')[1]


"""
        Main entrypoint
"""

if __name__ == "__main__":
    debug = sys.argv[1] if len(sys.argv) > 1 else True
    try:
        comp = Component(debug)
        comp.run()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
