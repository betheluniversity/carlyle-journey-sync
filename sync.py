import sys
import os
import logging
import dataset
from datetime import datetime
from datetime import timedelta

from createsend import Client
from createsend import CreateSend
from createsend import Journey
from createsend import JourneyEmail

os.environ['ORACLE_HOME'] = '/usr/lib/oracle/12.1/client64'

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

output_file_handler = logging.FileHandler("cm_journey_sync.log")
stdout_handler = logging.StreamHandler(sys.stdout)

logger.addHandler(output_file_handler)
logger.addHandler(stdout_handler)

# local
import config

# set up variables
db = dataset.connect(config.DATABASE_URL, engine_kwargs={'max_identifier_length': 128})
tracking_table = db.load_table('CM_FA_TRACKING_EMAIL')
users_table = db.load_table('CARLYLE_USERS')

auth = {'api_key': config.API_KEY}
cs = CreateSend(auth)

sync_date = datetime.today()
sync_date = (sync_date - timedelta(days=14)).strftime('%Y-%m-%d')


def get_db_info():

    db_info = dict()

    banner_query = """
        select CU.pidm, CU.email, C_F_EIGHT as AID_YEAR_1, C_F_TEN as AID_YEAR_2
        from CARLYLE_USERS CU
        join CARLYLE_SEGMENT_USERS CSU on CU.PIDM = CSU.PIDM
        where CSU.CARLYLE_SEGMENT_ID='59464'
    """

    sf_query = """
        select email, SFCU.SF_ID AS CONTACT_ID, SFCSU.CF_45 as OPPORTUNITY_ID
        from SF_CARLYLE_USERS SFCU
        join SF_CARLYLE_SEGMENT_USERS SFCSU on SFCU.SF_ID = SFCSU.SF_ID
        where SEGID='6282'
    """

    banner_rows = db.query(banner_query)
    for r in banner_rows:
        db_info[r.get('email')] = r

    sf_rows = db.query(sf_query)
    for r in sf_rows:
        sf = r
        banner = db_info.get(r.get('email'), {})
        db_info[r.get('email')] = {**banner, **sf}

    return db_info


def upsert_email_record(client_info, journey_info, email, recipient, open_date, click_date):

    info = db_info.get(recipient.EmailAddress, {})

    data = {
        'pidm': info.get('pidm'),
        'email': recipient.EmailAddress,
        'opportunity_id': info.get('opportunity_id'),
        'contact_id': info.get('contact_id'),
        'aid_year_1': info.get('aid_year_1'),
        'aid_year_2': info.get('aid_year_2'),
        'client_name': client_info.get('name'),
        'client_id': client_info.get('id'),
        'journey_name': journey_info.Name,
        'journey_id': journey_info.JourneyID,
        'email_name': email.Name,
        'email_id': email.EmailID,
        'sent_date': datetime.strptime(recipient.SentDate, '%Y-%m-%d %H:%M:%S'),
        'open_date': open_date,
        'click_date': click_date,
    }
    try:
        tracking_table.upsert(row=data, keys=list(data.keys()))
    except Exception as e:
        logging.info('-----UPSERT ERROR-------')
        logging.info(str(data))
        logging.info(str(e))
        logging.info('------------------------')
        exit()

def build_email_opens(email):
    opens = email.opens(date=sync_date)
    email_opens = {}
    page = 1
    while page <= opens.NumberOfPages:
        logging.info('opens page ' + str(page) + ' of ' + str(opens.NumberOfPages))
        for o in opens.Results:
            if o.EmailAddress not in email_opens.keys():
                email_opens[o.EmailAddress] = datetime.strptime(o.Date, '%Y-%m-%d %H:%M:%S')
        page += 1
        opens = email.opens(date=sync_date, page=page)

    return email_opens


def build_email_clicks(email):
    email_clicks = {}
    clicks = email.clicks(date=sync_date)
    page = 1
    while page <= clicks.NumberOfPages:
        logging.info('clicks page ' + str(page) + ' of ' + str(clicks.NumberOfPages))
        for c in clicks.Results:
            # only want the first one for each email
            if c.EmailAddress not in email_clicks.keys():
                email_clicks[c.EmailAddress] = datetime.strptime(c.Date, '%Y-%m-%d %H:%M:%S')
        page += 1
        clicks = email.clicks(date=sync_date, page=page)

    return email_clicks


db_info = get_db_info()

for key in config.CLIENT_KEYS:
    client = Client(auth=auth, client_id=key)

    client_info = {'id': key, 'name': client.details().BasicDetails.CompanyName}

    for j in client.journeys():

        journey = Journey(auth=auth, journey_id=j.JourneyID)
        journey_info = journey.summary()
        emails = journey_info.Emails

        _journey_key = '{}:::{}'.format(client_info.get('name'), journey_info.Name)
        logging.info('checking ' + _journey_key)
        if _journey_key in config.JOURNEY_SYNC_LIST:
            logging.info('syncing ' + _journey_key)
            for e in emails:
                email = JourneyEmail(auth=auth, journey_email_id=e.EmailID)
                recipients = email.recipients(date=sync_date)

                opens = build_email_opens(email)
                clicks = build_email_clicks(email)

                page = 1
                while page <= recipients.NumberOfPages:
                    logging.info('syncing recipients page ' + str(page) + ' of ' + str(recipients.NumberOfPages) + ' ---- ' + e.Name)

                    for recipient in recipients.Results:
                        open_date = opens.get(recipient.EmailAddress)
                        click_date = opens.get(recipient.EmailAddress)
                        upsert_email_record(client_info, journey_info, e, recipient, open_date, click_date)
                    page += 1
                    recipients = email.recipients(date=sync_date, page=page)
