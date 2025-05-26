"""
Template Component main class.

"""
import datetime  # noqa
import logging
import os
from typing import List, BinaryIO
import uuid

import dateparser
from azure.storage.blob import ContainerClient, BlobBlock
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
KEY_STAGE_AND_COMMIT = 'stage_and_commit'
KEY_BLOCK_SIZE = 'block_size'
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

DEFAULT_BLOCK_SIZE = 4194304

WORKSPACE_AUTH_TYPE = "Workspace Credentials"
AZURE_AUTH_TYPE = "Azure Credentials"


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self.container_client = None

    def run(self):
        """
        Main execution code
        """
        logging.info("Initializing component.")
        self.validate_configuration_parameters(MANDATORY_PARS)
        params = self.configuration.parameters

        in_tables = self.get_input_tables_definitions()
        in_files = self.get_input_files_definitions()

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
        # Create the BlockBlobService that is used to call the Blob service for the storage account
        logger = logging.getLogger("empty_logger")
        # Disable the logger by default to avoid unnecessary output
        logger.disabled = False if params.get('debug', False) in params else True

        if params.get(KEY_AUTH_TYPE, AZURE_AUTH_TYPE) == WORKSPACE_AUTH_TYPE:
            workspace_token = params.get(KEY_STORAGE_TOKEN)
            workspace_id = params.get(KEY_WORKSPACE_ID)
            workspace_client = Workspaces(f'https://{os.environ.get("KBC_STACKID")}', workspace_token)
            account_key = self._refresh_abs_container_token(workspace_client, workspace_id)

        self.container_client = ContainerClient(
            account_url=account_url,
            container_name=container_name,
            credential=account_key,
            logger=logger,
            # adjust block size for extremely large files
            max_block_size=self._get_max_block_size(in_tables)
        )

        self.validate_container_client(self.container_client)

        upload_method = self.stage_and_commit_upload if params.get(KEY_STAGE_AND_COMMIT) else self.standard_upload

        # Uploading tables and files to Blob Storage
        for definition in in_tables + in_files:
            if '.' in definition.name:
                file_name, _, extension = definition.name.rpartition(".")
                destination_name = f'{path_destination}{file_name}{append_value}.{extension}'
            else:
                destination_name = f'{path_destination}{definition.name}{append_value}'

            try:
                with open(file=definition.full_path, mode="rb") as file_stream:
                    logging.info(f"Uploading `{definition.full_path}` to `{destination_name}`")
                    upload_method(file_stream, destination_name, block_size=params.get(KEY_BLOCK_SIZE))
            except Exception as e:
                raise UserException(f'There is an issue with uploading [{definition.name}]. {e}') from e

        logging.info("Blob Storage Writer finished")

    @staticmethod
    def _get_max_block_size(tables: List[TableDefinition]):
        max_size = 0
        for t in tables:
            size = os.path.getsize(t.full_path)
            if size > max_size:
                max_size = size

        return 4 * 1024 * 1024 if max_size < 1073741824 else 100 * 1024 * 1024

    @staticmethod
    def validate_container_client(blob_obj: ContainerClient) -> None:
        """
        Validating if input container exists in the Blob Storage
        """

        # List all containers for this account
        # & Determine if the input container is available
        # & Validate if the entered account has the right credentials and privileges
        try:
            blob_obj.get_account_information()
        except Exception as e:
            raise UserException(f'Authorization Error. Please validate your credentials. Error: {e}') from e

    @staticmethod
    def _refresh_abs_container_token(workspace_client: Workspaces, workspace_id: str) -> str:
        ps = workspace_client.reset_password(workspace_id)
        return ps['connectionString'].split('SharedAccessSignature=')[1]

    def standard_upload(self, file_stream: BinaryIO, destination_name: str, **kwargs):
        self.container_client.upload_blob(name=destination_name, data=file_stream, overwrite=True)

    def stage_and_commit_upload(self, file_stream: BinaryIO, destination_name: str, block_size: int = None):
        block_size = block_size or DEFAULT_BLOCK_SIZE
        blob_client = self.container_client.get_blob_client(destination_name)

        block_id_list = []
        i = 0

        while True:
            buffer = file_stream.read(block_size)
            if not buffer:
                break

            block_id = uuid.uuid4().hex
            block_id_list.append(BlobBlock(block_id=block_id))

            i += 1
            logging.info(f'Staging block {i}')
            blob_client.stage_block(block_id=block_id, data=buffer, length=len(buffer))

        logging.info(f'Committing {i} blocks for [{destination_name}]')
        blob_client.commit_block_list(block_id_list)


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
