"""
Background tasks for 'download_all_content_csv'
"""

import logging
import collections
import datetime as dt
import logging
import time
from io import StringIO, BytesIO
import zipfile
import csv

# PyPI
import mc_providers
from background_task import background

# mcweb/backend/search (local directorty)
from .utils import parsed_query_from_dict, pq_provider, ParsedQuery

# mcweb/backend
from ..users.models import QuotaHistory
from ..sources.tasks import _return_task

# mcweb/util
from util.send_emails import send_zipped_large_download_email

logger = logging.getLogger(__name__)


def download_all_large_content_csv(queryState, user_id, user_isStaff, email):
    task = _download_all_large_content_csv(
        queryState, user_id, user_isStaff, email)
    return {'task': _return_task(task)}

@background(remove_existing_tasks=True)
def _download_all_large_content_csv(queryState, user_id, user_isStaff, email):
    data = []

    # code from: https://stackoverflow.com/questions/17584550/attach-generated-csv-file-to-email-and-send-with-django
    
    # Create an in-memory byte stream
    zipstream = BytesIO()

    # Create a ZipFile object using the in-memory byte stream
    zipfile_obj = zipfile.ZipFile(zipstream, 'w', zipfile.ZIP_DEFLATED)

    ts = _filename_timestamp()  # once, to link files together

    zip_filename = f"mc-{ts}-content.zip"
    
    qnum = 1
    for query in queryState:
        pq = parsed_query_from_dict(query)
        provider = pq_provider(pq)

        # Generate and write data to the CSV
        result = provider.all_items(pq.query_str, pq.start_date, pq.end_date, **pq.provider_props)

        # qnum first, zero padded, so it doesn't look like it's per-provider:
        csv_filename = f"mc-{qnum:>03}-{pq.provider_name}-{ts}-content.csv"
        qnum += 1

        # Create a StringIO object to store the CSV data
        csvfile = StringIO()
        csvwriter = csv.writer(csvfile)

        # UM..... (c/sh)ouldn't this use common code with views (CsvWriterHelper)?
        # WOULD need to pass a generator that flips thru the pages....

        first_page = True
        for page in result:
            QuotaHistory.increment(user_id, user_isStaff, pq.provider_name)
            if first_page:
                csvwriter.writerow(sorted(page[0].keys()))
                first_page = False
            for story in page:
                csvwriter.writerow([v for k, v in sorted(story.items())])

        # get CSV data from StringIO as bytes
        csv_data = csvfile.getvalue()

        # Add the CSV data to the zip file
        zipfile_obj.writestr(csv_filename, csv_data)

    # Close the zip file
    zipfile_obj.close()
    # Get the zip data
    zipped_data = zipstream.getvalue()

    # WISH this took a file object, so we didn't need to keep it all in memory!
    # (since it could be big!)
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

    # single timestamp to tie zip and files together
    ts = _filename_timestamp()
    zip_filename = f"mc-languages-{ts}-content.zip"

    qnum = 1                    # per-query/file number
    for pq in queries:
        provider = pq_provider(pq)

        data = provider.languages(f"({pq.query_str})", pq.start_date, pq.end_date, **pq.provider_props)
        QuotaHistory.increment(user_id, is_staff, pq.provider_name)

        # code from: https://stackoverflow.com/questions/17584550/attach-generated-csv-file-to-email-and-send-with-django
    
        # Create a StringIO object to store the CSV data
        csvfile = StringIO()
        csvwriter = csv.writer(csvfile)

        # qnum first, zero padded, so it doesn't look like it's per-provider:
        csv_filename = f"mc-{qnum:>03}-{pq.provider_name}-{ts}-langs.csv"
        qnum += 1

        # UM..... (c/sh)ouldn't this use common code with views (CsvWriterHelper)?
        # WOULD need to pass a generator that flips thru the pages....

        # "languages" returns [{'language': 'en', 'value': nnn, 'ratio': 0.xyz}, ...]
        # each language will be a row in the CSV
        for row in data:
            csvwriter.writerow(data)
   
        # Convert the CSV data from StringIO to bytes
        csv_data = csvfile.getvalue()

        # Add the CSV data to the zip file
        zipfile_obj.writestr(csv_filename, csv_data)

    # Close the zip file
    zipfile_obj.close()

    # Get the zip data
    zipped_data = zipstream.getvalue()

    # WISH this took a file object, so we didn't need to keep it all in memory!
    send_zipped_large_download_email(zip_filename, zipped_data, email)
    logger.info("Sent Email to %s (%d bytes)", email, len(zipped_data))


def _filename_timestamp() -> str:
    return time.strftime("%Y%m%d%H%M%S", time.localtime())
