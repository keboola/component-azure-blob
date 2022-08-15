import datetime  # noqa
import logging
import os
import sys

import dateparser
from azure.storage.blob import ContainerClient
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

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


class Component(ComponentBase):

    def __init__(self):
        super().__init__()

    def run(self):
        """
        Main execution code
        """
        self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        # Loading input configurations
        params = self.configuration.parameters

        # Loading input mapping
        in_tables = self.get_input_tables_definitions()
        in_table_names = [x.name for x in in_tables]
        logging.info("IN tables mapped: " + str(in_table_names))

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
        if params.get('append_date_to_file'):
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
                    data=open(table.full_path, 'rb'),
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
        """
        Validating if input container exists in the Blob Storage
        """

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


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
