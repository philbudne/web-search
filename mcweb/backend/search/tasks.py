"""
Background tasks for 'download_all_content_csv'
"""

# Python
import csv
import datetime as dt
import logging
import zipfile
from io import StringIO, BytesIO

# PyPI
import mc_providers
from background_task import background

# mcweb/backend/search (local directorty)
from .utils import parsed_query_from_dict, pq_provider, ParsedQuery, filename_timestamp, all_content_csv_generator

# mcweb/backend
from ..users.models import QuotaHistory
from ..sources.tasks import _return_task

# mcweb/util
from util.send_emails import send_zipped_large_download_email

logger = logging.getLogger(__name__)


# called from /api/search/send-email-large-download-csv endpoint
# by frontend sendTotalAttentionDataEmail
def download_all_large_content_csv(parsed_queries: list[ParsedQuery], user_id, user_isStaff, email):
    task = _download_all_large_content_csv(
        parsed_queries, user_id, user_isStaff, email)
    return {'task': _return_task(task)}

@background(remove_existing_tasks=True)
def _download_all_large_content_csv(parsed_queries: list[ParsedQuery], user_id, user_isStaff, email):
    # Phil: maybe catch exception, and send email?

    logger.info("starting large_content_csv for %s; %d query/ies",
                email, len(parsed_queries))

    # if the uncompressed data size is ever an issue
    # (taking too much memory) do:
    # try:
    #    with open("/var/tmp/" + csv_filename, "w") as csvfile:
    #       write to file....
    # and after:
    #    zipfile_obj.write(csv_filename, ....)
    #    send email....
    # finally:
    #    os.unlink(csv_filename)

    filename, generator = all_content_csv_generator(parsed_queries, user_id, user_isStaff)

    # always make matching filenames
    csv_filename = filename + ".csv"
    zip_filename = filename + ".zip"

    # Create a StringIO object to store the CSV data
    csvfile = StringIO()
    csvwriter = csv.writer(csvfile)

    # Generate and write data to the CSV
    stories = 0                 # for logging
    for row in generator():     # generator handles quota
        csvwriter.writerow(row)
        stories += 1

    # code from: https://stackoverflow.com/questions/17584550/attach-generated-csv-file-to-email-and-send-with-django
    
    # Create an in-memory byte stream, and wrap ZipFile object around it
    zipstream = BytesIO()
    zipfile_obj = zipfile.ZipFile(zipstream, 'w', zipfile.ZIP_DEFLATED)

    # Convert the CSV data from StringIO to bytes
    csv_data = csvfile.getvalue()

    # Add the CSV data to the zip file
    zipfile_obj.writestr(csv_filename, csv_data)

    # Close the zip file
    zipfile_obj.close()

    # Get the zip data
    zipped_data = zipstream.getvalue()

    send_zipped_large_download_email(zip_filename, zipped_data, email)
    logger.info("Sent Email to %s (%d stories, csv: %d, zip: %d)",
                email, stories, len(csv_data), len(zipped_data))

def download_all_queries_csv_task(data, request):
    task = _download_all_queries_csv(data, request.user.id, request.user.is_staff, request.user.email)
    return {'task': _return_task(task)}

# Phil writes: As I found it, this function used query.thing, which I
# don't think could have worked (was a regular tuple)!  It also (and
# still) only outputs data for the last query, and passes raw "data"
# to csvwriter.writerow *AND* it does a top languages query!
#
# I'm also unconvinced this can be called
# frontend/src/features/search/util/CSVDialog.jsx has:
#   const [downloadAll, { isLoading }] = useDownloadAllQueriesMutation();
# but the call to downloadAll is commented out?

# All of the above makes me think this is dead code!

@background(remove_existing_tasks=True)
def _download_all_queries_csv(data: list[ParsedQuery], user_id, is_staff, email):
    for pq in data:
        provider = pq_provider(pq)
        data = provider.languages(f"({pq.query_str})", pq.start_date, pq.end_date, **pq.provider_props)
        QuotaHistory.increment(user_id, is_staff, pq.provider_name)

    # code from: https://stackoverflow.com/questions/17584550/attach-generated-csv-file-to-email-and-send-with-django
    
    # Create an in-memory byte stream
    zipstream = BytesIO()

    # Create a ZipFile object using the in-memory byte stream
    zipfile_obj = zipfile.ZipFile(zipstream, 'w', zipfile.ZIP_DEFLATED)

    # Create a StringIO object to store the CSV data
    csvfile = StringIO()
    csvwriter = csv.writer(csvfile)

    # once, so filenames match up
    prefix = "mc-{}-{}-content".format(pq.provider_name, filename_timestamp())
    csv_filename = f"{prefix}.csv"
    zip_filename = f"{prefix}.zip"

    # Generate and write data to the CSV
    csvwriter.writerow(data)
   
    # Convert the CSV data from StringIO to bytes
    csv_data = csvfile.getvalue()
    # Add the CSV data to the zip file
    zipfile_obj.writestr(csv_filename, csv_data)
    # Close the zip file
    zipfile_obj.close()
    # Get the zip data
    zipped_data = zipstream.getvalue()

    send_zipped_large_download_email(zip_filename, zipped_data, email)
    logger.info("Sent Email to %s (csv: %d, zip: %d)", email, len(csv_data), len(zipped_data))
