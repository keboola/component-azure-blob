"""
Template Component main class.

"""
import datetime  # noqa
import logging
import os
from typing import List

import dateparser
from azure.storage.blob import ContainerClient
from kbcstorage.workspaces import Workspaces
from keboola.component.base import ComponentBase
from keboola.component.dao import TableDefinition
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
    [KEY_ACCOUNT_NAME,
     KEY_ACCOUNT_KEY],
    KEY_CONTAINER_NAME,
    KEY_DESTINATION_PATH,
    KEY_APPEND_DATE_TO_FILE
]

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
        logging.info("Initializing component.")
        self.validate_configuration_parameters(MANDATORY_PARS)
        params = self.configuration.parameters

        in_tables = self.get_input_tables_definitions()

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
        logger = logging.getLogger("empty_logger")
        logger.disabled = True

        if params.get(KEY_AUTH_TYPE, AZURE_AUTH_TYPE) == WORKSPACE_AUTH_TYPE:
            workspace_token = params.get(KEY_STORAGE_TOKEN)
            workspace_id = params.get(KEY_WORKSPACE_ID)
            workspace_client = Workspaces(f'https://{os.environ.get("KBC_STACKID")}', workspace_token)
            account_key = self._refresh_abs_container_token(workspace_client, workspace_id)

        max_block_size = self._get_max_block_size(in_tables)
        logging.debug(f"Using block size {max_block_size}")

        block_blob_service = ContainerClient(
            account_url=account_url,
            container_name=container_name,
            credential=account_key,
            logger=logger,
            # adjust block size for extremely large files
            max_block_size=max_block_size
        )

        # Validate input container name
        self.validate_blob_container(block_blob_service)

        # Uploading files to Blob Storage
        for table in in_tables:
            table_name = '{}{}{}.csv'.format(
                path_destination,  # folder path
                table.name.split('.csv')[0],  # file name
                append_value)  # custom date value
            logging.info('Uploading [{}]...'.format(table_name))
            try:
                block_blob_service.upload_blob(
                    # blob_name=table['destination'],
                    name=table_name,
                    data=open(table.full_path, 'rb'),
                    overwrite=True
                )
            except Exception as e:
                raise UserException(f'There is an issue with uploading [{table.name}]. {e}') from e

        logging.info("Blob Storage Writer finished")

    def _get_max_block_size(self, tables: List[TableDefinition]):
        max_size = 0
        for t in tables:
            size = os.path.getsize(t.full_path)
            if size > max_size:
                max_size = size
        logging.debug(f'Max file size to upload is: {max_size}')
        return 4 * 1024 * 1024 if max_size < 1073741824 else 100 * 1024 * 1024

    @staticmethod
    def validate_blob_container(blob_obj: ContainerClient) -> None:
        """
        Validating if input container exists in the Blob Storage
        """

        # List all containers for this account
        # & Determine if the input container is available
        # & Validate if the entered account has the right credentials and privileges
        try:
            blob_obj.get_account_information()
        except Exception:
            raise UserException('Authorization Error. Please validate your credentials.')

    @staticmethod
    def _refresh_abs_container_token(workspace_client: Workspaces, workspace_id: str) -> str:
        ps = workspace_client.reset_password(workspace_id)
        return ps['connectionString'].split('SharedAccessSignature=')[1]


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
