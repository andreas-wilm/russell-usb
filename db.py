# standard library
import sys
import os
import logging
import uuid

# https://docs.microsoft.com/en-us/azure/cosmos-db/table-storage-how-to-use-python
# https://docs.microsoft.com/en-us/python/api/azure-cosmosdb-table/azure.cosmosdb.table.tableservice.tableservice?view=azure-python
from azure.cosmosdb.table.tableservice import TableService
#from azure.cosmosdb.table.models import Entity
from azure.common import AzureMissingResourceHttpError


__author__ = "Andreas Wilm"
__email__ = "andreas.wilm@microsoft.com"
__copyright__ = "2019 Microsoft Corp"
__license__ = "The MIT License"


LOG = logging.getLogger("")
logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s [%(asctime)s]: %(message)s')

# Table storage
#
# Basic:
# - Partition-key gets distributed across nodes
# - Row-key is a unique id within partition
# - primary key = partition-key + row-key
# Here:
# - use "status" and "visits" as partition-keys
# - NFC id as row-key
# - And lazily put everything into the same table...*cough
#
# Table storage account credentials
# In Azure Webapp use 'Application Settings' to set
try:
    TBL_ACCT_NAME = os.environ['TBL_ACCT_NAME']
    TBL_ACCT_KEY = os.environ['TBL_ACCT_KEY']
except KeyError:
    sys.stderr.write("FATAL: Missing TBL_ACCT_NAME or TBL_ACCT_KEY as environment variable\n")
    sys.exit(1)
TBL_SRV = TableService(account_name=TBL_ACCT_NAME,
                       account_key=TBL_ACCT_KEY)

TBL_NAME = "tapevents"

VALID_STATUS = ['entering', 'exiting']


def drop_table():
    """as the name implies..."""
    print("WARNING: Dropping table")
    TBL_SRV.delete_table(TBL_NAME)


def list_nfc_status():
    """list latest status for all nfc-ids
    """
    LOG.info("Last status for each NFC-id:")
    events = TBL_SRV.query_entities(
        TBL_NAME, filter="PartitionKey eq 'status'")
    for event in events:
        LOG.info("%s %s %s", event.RowKey, event.status, event.datetime)


def get_nfcid_status(nfcid):
    """retrieves status and event time for nfcid.
    throws exception if not existant"""
    try:
        event = TBL_SRV.get_entity(TBL_NAME, 'status', nfcid)
        assert event.status in VALID_STATUS, (
            "Invalid status {}".format(event.status))
    except AzureMissingResourceHttpError:
        return None, None
    else:
        return event.status, event.datetime


def set_nfcid_status(nfcid, status, now):
    """sets or updates nfcid status and uses `now` as event time"""
    assert status in VALID_STATUS, (
        "Invalid status {}".format(status.status))
    tapevent = {'PartitionKey': 'status',
                'RowKey': nfcid,
                'status': status,
                'datetime': now}
    TBL_SRV.insert_or_replace_entity(TBL_NAME, tapevent)


def save_visit(nfcid, durationsec, visitorip, now):
    """save a `visit` which means card entered before and just exited"""
    tapevent = {'PartitionKey': 'visit',
                'RowKey': "{}-{}".format(nfcid, uuid.uuid4()),# needs to be uniq
                'datetime': now,# strictly not needed
                'durationsec': durationsec,
                'ip': visitorip}
    TBL_SRV.insert_entity(TBL_NAME, tapevent)


if not TBL_SRV.exists(TBL_NAME):
    LOG.info("Creating table %s", TBL_NAME)
    TBL_SRV.create_table(TBL_NAME)
