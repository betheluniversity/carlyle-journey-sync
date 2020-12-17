import dataset

from createsend import Client
from createsend import CreateSend
from createsend import Journey
from createsend import JourneyEmail

from datetime import datetime

# local
import config

# set up variables
db = dataset.connect(config.DATABASE_URL, engine_kwargs={'max_identifier_length': 128})
tracking_table = db.load_table('CM_FA_TRACKING_EMAIL')
users_table = db.load_table('CARLYLE_USERS')

auth = {'api_key': config.API_KEY}
cs = CreateSend(auth)


def get_pidm_by_email(email):
    # todo remove
    email = 'e-jameson@bethel.edu'

    row = db.query('select pidm, email from CARLYLE_USERS where email=:email', email=email)
    result = [r for r in row]
    if len(result) == 0:
        return None
    return result[0].get('pidm')


def upsert_email_record(client_info, journey_info, email, recipient):
    data = {
        'pidm': get_pidm_by_email(recipient.EmailAddress),
        'journey_name': journey_info.Name,
        'journey_id': journey_info.JourneyID,
        'email_name': email.Name,
        'email_id': email.EmailID,
        'email': recipient.EmailAddress,
        'sent_date': datetime.strptime(recipient.SentDate, '%Y-%m-%d %H:%M:%S'),
        'client_name': client_info.get('name'),
        'client_id': client_info.get('id')
    }
    tracking_table.upsert(row=data, keys=list(data.keys()))


for key in config.CLIENT_KEYS:
    client = Client(auth=auth, client_id=key)

    client_info = {'id': key, 'name': client.details().BasicDetails.CompanyName}

    for j in client.journeys():

        journey = Journey(auth=auth, journey_id=j.JourneyID)
        journey_info = journey.summary()
        emails = journey_info.Emails

        for e in emails:
            email = JourneyEmail(auth=auth, journey_email_id=e.EmailID)
            recipients = email.recipients()

            for recipient in recipients.Results:
                upsert_email_record(client_info, journey_info, e, recipient)
