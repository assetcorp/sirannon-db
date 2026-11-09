"""Generated from packages/ts/openapi.json and packages/ts/asyncapi.json by packages/python/generator. Do not edit."""

from sirannon_client._operations.backups import BackupsOperations
from sirannon_client._operations.device_sync import DeviceSyncOperations
from sirannon_client._operations.encryption import EncryptionOperations
from sirannon_client._operations.operations import OperationsOperations
from sirannon_client._operations.server import ServerOperations
from sirannon_client._operations.statements import StatementsOperations


class Operations(
    ServerOperations,
    StatementsOperations,
    OperationsOperations,
    BackupsOperations,
    EncryptionOperations,
    DeviceSyncOperations,
):
    """These methods call every route of a Sirannon server, one method for each route."""
