"""
Quick and dirty webapp for visit time tracking for the Microsoft Tech Lab Bus Tour 2019

Run with FLASK_APP=application.py flask run

See requirements.txt for dependencies.
"""

# standard library
from datetime import datetime, timezone
import sys
import os
import logging

# third party imports
from flask import Flask
from flask import request
from flask import render_template
# https://docs.microsoft.com/en-us/azure/cosmos-db/table-storage-how-to-use-python
# https://docs.microsoft.com/en-us/python/api/azure-cosmosdb-table/azure.cosmosdb.table.tableservice.tableservice?view=azure-python

# project specific imports
from db import set_nfcid_status, get_nfcid_status, list_nfc_status, save_visit


__author__ = "Andreas Wilm"
__email__ = "andreas.wilm@microsoft.com"
__copyright__ = "2019 Microsoft Corp"
__license__ = "The MIT License"


app = Flask(__name__)


LOG = logging.getLogger("")
logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s [%(asctime)s]: %(message)s')


list_nfc_status()


@app.route("/", methods=['GET'])
def root():
    """one and only route
    """

    nfcid = request.args.get("nfcid")
    if not nfcid:
        LOG.info("No NFC-ID found")
        body = "I didn't recognize you (got no NFC-ID)"
        return render_template('index.html', body=body)

    LOG.info("Retrieving status for NFC-ID %s", nfcid)
    oldstatus, oldtimestamp = get_nfcid_status(nfcid)
    if not oldstatus and not oldtimestamp:
        LOG.info("First time I see NFC ID %s", nfcid)

    if not oldstatus or oldstatus == "exiting":
        newstatus = "entering"
    elif oldstatus == "entering":
        newstatus = "exiting"
    else:
        raise ValueError(oldstatus)

    #https://stackoverflow.com/questions/3759981/get-ip-address-of-visitors-using-flask-for-python
    #https://www.pythonanywhere.com/forums/topic/2673/
    visitorip = request.environ.get('HTTP_X_FORWARDED_FOR',
                                    request.environ['REMOTE_ADDR'])

    now = datetime.utcnow()

    LOG.info("Processing request from %s for NFC-ID %s, which is %s at %s (UTC)",
             visitorip, nfcid, newstatus, now)

    set_nfcid_status(nfcid, newstatus, now)

    if newstatus == "exiting":
        duration = now.replace(tzinfo=timezone.utc) - oldtimestamp.replace(tzinfo=timezone.utc)
        save_visit(nfcid, duration.total_seconds(), visitorip, now)
        staymin, staysec = divmod(duration.total_seconds(), 60)
        body = "you stayed for {:d} minutes and {:d} seconds. Thanks for visiting!".format(
            int(staymin), int(staysec))
    else:
        body = "enjoy your stay!"

    return render_template('index.html', user=nfcid, body=body)


if __name__ == "__main__":
    list_nfc_status()
    app.run()
