"""
Quick and dirty webapp for visit time tracking for the Microsoft Tech Lab Bus Tour 2019

Run with FLASK_APP=application.py flask run

See requirements.txt for dependencies.
"""

# standard library
from datetime import datetime
import sys
import os
import logging

# third party imports
from flask import Flask
from flask import request
from flask import render_template
import sqlalchemy


__author__ = "Andreas Wilm"
__email__ = "andreas.wilm@microsoft.com"
__copyright__ = "2019 Microsoft Corp"
__license__ = "The MIT License"


app = Flask(__name__)


# http://pymssql.org/en/stable/faq.html#cannot-connect-to-sql-server
#import os
#os.environ['TDSDUMP'] = 'stdout'


# SQL credential
# set via Azure Webapp's Application Settings
try:
    MSSQL_PWD = os.environ['MSSQL_PWD']
    MSSQL_USER = os.environ['MSSQL_USER']
    MSSQL_DBSERVERNAME = os.environ['MSSQL_DBSERVERNAME']
except KeyError:
    sys.stderr.write("FATAL: Missing MSSQL_USER, MSSQL_PWD or MSSQL_DBSERVERNAME as environment variable\n")
    mssql_env_vars = [(k, v) for (k, v) in os.environ.items() if k.startswith("MSSQL")]
    sys.stderr.write("List of MSSQL prefixed env vars: %s" % list(zip(mssql_env_vars)))
    sys.exit(1)
MSSQL_DB = "tap"
MSSQL_PORT = "1433"
MSSQL_HOST = "{}.database.windows.net".format(MSSQL_DBSERVERNAME)


LOG = logging.getLogger("")
logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s [%(asctime)s]: %(message)s')

# FIXME put all the below into init function

# https://blog.hbldh.se/2016/10/connecting-to-sql-server-on-azure-with-python/
connection_string = 'mssql+pymssql://{}:{}@{}:{}/{}'.format(
    '{0}@{1}'.format(MSSQL_USER, MSSQL_DBSERVERNAME),
    #MSSQL_USER,
    MSSQL_PWD, MSSQL_HOST, MSSQL_PORT, MSSQL_DB)
LOG.info("Connecting using %s", connection_string.replace(MSSQL_PWD, "******"))
ENGINE = sqlalchemy.create_engine(connection_string, pool_pre_ping=True)#, echo="debug")
# make sure to open up access to client ip:
# https://docs.microsoft.com/en-us/azure/sql-database/sql-database-firewall-configure
CONNECTION = ENGINE.connect()
metadata = sqlalchemy.MetaData()


# if starting from scratch, create the table
# FIXME shouldn't this go into init somewhere?
if not ENGINE.dialect.has_table(ENGINE, 'tap'):
    LOG.info("Creating table")
    TAPDB = sqlalchemy.Table(
        'tap', metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('nfcid', sqlalchemy.String, nullable=False),
        sqlalchemy.Column('datetime', sqlalchemy.DateTime, nullable=False),
        sqlalchemy.Column('status', sqlalchemy.String, nullable=False),
        sqlalchemy.Column('ip', sqlalchemy.String, nullable=False),
        )
    metadata.create_all(ENGINE)
else:
    LOG.info("Loading existing table")
    TAPDB = sqlalchemy.Table('tap', metadata, autoload=True, autoload_with=ENGINE)

LOG.debug("Table meta-data: %s", repr(metadata.tables['tap']))


def droptable():
    """as the name implies..."""
    print("WARNING: Dropping table")
    TAPDB.drop(ENGINE, checkfirst=False)
    sys.exit(1)


def list_nfc_status():
    """list latest status for nfc-ids
    """
    s = sqlalchemy.select([TAPDB.c.nfcid.distinct()])
    LOG.info("Last status for each NFC-id:")
    nfcids = [row[0] for row in CONNECTION.execute(s)]
    for n in nfcids:
        s = sqlalchemy.select([TAPDB]).where(TAPDB.c.nfcid == n).order_by(
            TAPDB.c.datetime.desc()).limit(1)
        LOG.info(CONNECTION.execute(s).fetchone())

#droptable()

list_nfc_status()


@app.route("/", methods=['GET'])
def root():
    """main route"""
    nfcid = request.args.get("nfcid")
    if not nfcid:
        LOG.info("No NFC-ID found")
        body = "I didn't recognize you (got no NFC-ID)"
        return render_template('index.html', user="nobody", body=body)

    #https://stackoverflow.com/questions/3759981/get-ip-address-of-visitors-using-flask-for-python
    #visitorip = request.remote_addr
    #visitorip = request.environ['REMOTE_ADDR']
    #https://www.pythonanywhere.com/forums/topic/2673/
    visitorip = request.environ.get('HTTP_X_FORWARDED_FOR',
                                    request.environ['REMOTE_ADDR'])

    now = datetime.now()
    status = "entering"# unless the last entry for this nfcid says "entering" already
    try:
        s = sqlalchemy.select([TAPDB]).where(TAPDB.c.nfcid == nfcid).order_by(
            TAPDB.c.datetime.desc()).limit(1)
        row = CONNECTION.execute(s).fetchone()
    except:
        body = "Whoa...couldn't fetch status for {}.".format(nfcid)
        body += " Error was {}".format(sys.exc_info()[0])
        return render_template('index.html', user="nobody", body=body)

    if row:
        LOG.info("First time I see NFC ID %s", nfcid)
    if row and row["status"] == "entering":
        status = "exiting"
    # else: nfcid never seen before, hence "entering"

    LOG.info("Processing request from %s for NFC-ID %s, which is %s",
             visitorip, nfcid, status)
    if status == "exiting":
        stay = now - row['datetime']
        LOG.info("Length of stay: %s - %s", now, row['datetime'])
        staymin, staysec = divmod(stay.total_seconds(), 60)
        staymin = int(staymin)
        staysec = int(staysec)
        body = "you stayed for {:d} minutes and {:d} seconds. Thanks for visiting!".format(
            staymin, staysec)
    else:
        body = "enjoy your stay!"

    # insert
    try:
        ins = TAPDB.insert().values(
            nfcid=nfcid, datetime=now, status=status, ip=visitorip)
        _ = CONNECTION.execute(ins)
        LOG.info("DB insert: %s %s %s %s", nfcid, now, status, visitorip)
    except:
        body = "Whoa...couldn't insert status for {}.".format(nfcid)
        body += " Error was {}".format(sys.exc_info()[0])
        return render_template('index.html', user="nobody", body=body)

    return render_template('index.html', user=nfcid, body=body)



if __name__ == "__main__":
    app.run()
