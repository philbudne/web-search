"""
Background tasks for 'download_all_content_csv'
"""

from ..users.models import QuotaHistory
from .utils import parsed_query_from_dict, ParsedQuery
import logging
import collections
import datetime as dt
import logging
import time
from io import StringIO, BytesIO
import zipfile
import csv
from background_task import background
from ..sources.tasks import _return_task
from util.send_emails import send_zipped_large_download_email

import mc_providers as providers


logger = logging.getLogger(__name__)


def download_all_large_content_csv(queryState, user_id, user_isStaff, email):
    task = _download_all_large_content_csv(
        queryState, user_id, user_isStaff, email)
    return {'task': _return_task(task)}

@background(remove_existing_tasks=True)
def _download_all_large_content_csv(queryState, user_id, user_isStaff, email):
    data = []
    for query in queryState:
        pq = parsed_query_from_dict(query)
        provider = providers.provider_by_name(pq.provider_name, pq.api_key, pq.base_url, pq.caching)
        data.append(provider.all_items(
            pq.query_str, pq.start_date, pq.end_date, **pq.provider_props))

    # iterator function
    def data_generator():
        for result in data:
            first_page = True
            for page in result:
                QuotaHistory.increment(user_id, user_isStaff, pq.provider_name)
                if first_page:  # send back column names, which differ by platform
                    yield sorted(list(page[0].keys()))
                for story in page:
                    ordered_story = collections.OrderedDict(
                        sorted(story.items()))
                    yield [v for k, v in ordered_story.items()]
                first_page = False

    # code from: https://stackoverflow.com/questions/17584550/attach-generated-csv-file-to-email-and-send-with-django
    
    # Create an in-memory byte stream
    zipstream = BytesIO()

    # Create a ZipFile object using the in-memory byte stream
    zipfile_obj = zipfile.ZipFile(zipstream, 'w', zipfile.ZIP_DEFLATED)

    # Create a StringIO object to store the CSV data
    csvfile = StringIO()
    csvwriter = csv.writer(csvfile)
    
    filename = "mc-{}-{}-content.csv".format(
        pq.provider_name, _filename_timestamp())
   
    zip_filename = "mc-{}-{}-content.zip".format(
        pq.provider_name, _filename_timestamp())
    
    # Generate and write data to the CSV
    for data in data_generator():
        csvwriter.writerow(data)
   
    # Convert the CSV data from StringIO to bytes
    csv_data = csvfile.getvalue()
    # Add the CSV data to the zip file
    zipfile_obj.writestr(filename, csv_data)
    # Close the zip file
    zipfile_obj.close()
    # Get the zip data
    zipped_data = zipstream.getvalue()

    send_zipped_large_download_email(zip_filename, zipped_data, email)
    logger.info("Sent Email to %s (%d bytes)", email, len(zipped_data))

def download_all_queries_csv_task(queries: list[ParsedQuery], request):
    task = _download_all_queries_csv(queries, request.user.id, request.user.is_staff, request.user.email)
    return {'task': _return_task(task)}


# PB: I don't think the code here could have worked (was passed an unnamed tuple, but
# accessed the fields by .name), looped for a list of queries, but only returned
# data from the last one (and would fail if there were no queries), so I replaced it with
# my interpretation, which is to put each query's results in a numbered csv file,
# with one row per language.  The only place I could find that references
# the /api/download-all-queries endpoint is the downloadAllQueries JS method,
# and I didn't see any use of that.
@background(remove_existing_tasks=True)
def _download_all_queries_csv(queries: list[ParsedQuery], user_id, is_staff, email):
    # Create an in-memory byte stream
    zipstream = BytesIO()

    # Create a ZipFile object using the in-memory byte stream
    zipfile_obj = zipfile.ZipFile(zipstream, 'w', zipfile.ZIP_DEFLATED)

    ts = _filename_timestamp()
    zip_filename = f"mc-languages-{ts}-content.zip"

    qnum = 1
    for query in queries:
        provider = providers.provider_by_name(query.provider_name,  query.api_key, query.base_url, query.caching)
    
        data = provider.languages(f"({query.query_str})", query.start_date, query.end_date, **query.provider_props)

        QuotaHistory.increment(user_id, is_staff, query.provider_name)

        # code from: https://stackoverflow.com/questions/17584550/attach-generated-csv-file-to-email-and-send-with-django
    
        # Create a StringIO object to store the CSV data
        csvfile = StringIO()
        csvwriter = csv.writer(csvfile)

        filename = f"mc-{qnum}-{ts}-content.csv"
        qnum += 1

        # "languages" returns [{'language': 'en', 'value': nnn, 'ratio': 0.xyz}, ...]
        # each language will be a row in the CSV
        for row in data:
            csvwriter.writerow(data)
   
        # Convert the CSV data from StringIO to bytes
        csv_data = csvfile.getvalue()

        # Add the CSV data to the zip file
        zipfile_obj.writestr(filename, csv_data)

    # Close the zip file
    zipfile_obj.close()

    # Get the zip data
    zipped_data = zipstream.getvalue()

    send_zipped_large_download_email(zip_filename, zipped_data, email)
    logger.info("Sent Email to %s (%d bytes)", email, len(zipped_data))


def _filename_timestamp() -> str:
    return time.strftime("%Y%m%d%H%M%S", time.localtime())
