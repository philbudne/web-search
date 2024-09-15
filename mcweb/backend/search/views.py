import json
import logging
import csv
import time
import collections
import requests
from typing import Optional

# PyPi
import mc_providers
from django.contrib.auth.decorators import login_required
from django.http import HttpResponseBadRequest, HttpResponseForbidden, HttpResponse
from django.views.decorators.http import require_http_methods
from mc_providers.exceptions import UnsupportedOperationException, QueryingEverythingUnsupportedQuery
from mc_providers.exceptions import ProviderException
from requests.adapters import HTTPAdapter
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from rest_framework.decorators import api_view, action, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from urllib3.util.retry import Retry

# mcweb/util
from util.cache import cache_by_kwargs, mc_providers_cacher
from util.csvwriter import CSVWriterHelper

# mcweb/backend/search (local dir)
from .utils import parse_query, parsed_query_from_dict, pq_provider, ParsedQuery
from .tasks import download_all_large_content_csv, download_all_queries_csv_task

# mcweb/backend/users
from ..users.models import QuotaHistory
from backend.users.exceptions import OverQuotaException

# mcweb/backend/util
import backend.util.csv_stream as csv_stream

logger = logging.getLogger(__name__)

# enable caching for mc_providers results (explicitly referencing pkg for clarity)
mc_providers.cache.CachingManager.cache_function = mc_providers_cacher

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)


def error_response(msg: str, response_type: Optional[HttpResponse]) -> HttpResponse:
    ResponseClass = response_type or HttpResponseBadRequest
    return ResponseClass(json.dumps(dict(
        status="error",
        note=msg,
    )))


def handle_provider_errors(func):
    """
    Decorator for view functions.

    If a provider-related method returns a JSON error we want to send it back to the client with information
    that can be used to show the user some kind of error.
    """
    def _handler(request):
        try:
            return func(request)
        except (ProviderException, OverQuotaException) as e:
            # these are expected errors, so just report the details msg to the user
            return error_response(str(e), HttpResponseBadRequest)
        except Exception as e:
            # these are internal errors we care about, so handle them as true errors
            logger.exception(e)
            return error_response(str(e), HttpResponseBadRequest)
    return _handler


@handle_provider_errors
@api_view(['GET', 'POST'])
@authentication_classes([TokenAuthentication, SessionAuthentication])
@permission_classes([IsAuthenticated])
def total_count(request):
    pq = parse_query(request)
    provider = pq_provider(pq)
    relevant_count = provider.count(f"({pq.query_str})", pq.start_date, pq.end_date, **pq.provider_props)
    try:
        total_content_count = provider.count(provider.everything_query(), pq.start_date, pq.end_date, **pq.provider_props)
    except QueryingEverythingUnsupportedQuery as e:
        total_content_count = None
    QuotaHistory.increment(request.user.id, request.user.is_staff, pq.provider_name)
    return HttpResponse(json.dumps({"count": {"relevant": relevant_count, "total": total_content_count}}),
                        content_type="application/json", status=200)



@handle_provider_errors
@api_view(['GET', 'POST'])
@authentication_classes([TokenAuthentication, SessionAuthentication])
@permission_classes([IsAuthenticated])
# @cache_by_kwargs()
def count_over_time(request):
    pq = parse_query(request)
    provider = pq_provider(pq)
    try:
        results = provider.normalized_count_over_time(f"({pq.query_str})", pq.start_date, pq.end_date, **pq.provider_props)
    except UnsupportedOperationException:
        # for platforms that don't support querying over time
        results = provider.count_over_time(f"({pq.query_str})", pq.start_date, pq.end_date, **pq.provider_props)
    response = results
    QuotaHistory.increment(
        request.user.id, request.user.is_staff, pq.provider_name)
    return HttpResponse(json.dumps({"count_over_time": response}, default=str), content_type="application/json",
                        status=200)

@handle_provider_errors
@api_view(['GET', 'POST'])
@authentication_classes([TokenAuthentication, SessionAuthentication])
@permission_classes([IsAuthenticated])
# @cache_by_kwargs()
def sample(request):
    pq = parse_query(request)
    provider = pq_provider(pq)
    try:
        response = provider.sample(f"({pq.query_str})", pq.start_date, pq.end_date, **pq.provider_props)
    except requests.exceptions.ConnectionError:
        response = {'error': 'Max Retries Exceeded'}
    QuotaHistory.increment(request.user.id, request.user.is_staff, pq.provider_name)
    return HttpResponse(json.dumps({"sample": response}, default=str), content_type="application/json",
                        status=200)

@handle_provider_errors
@api_view(['GET'])
@authentication_classes([TokenAuthentication, SessionAuthentication])
@permission_classes([IsAuthenticated])
# @cache_by_kwargs()
def story_detail(request):
    pq = parse_query(request)
    story_id = request.GET.get("storyId")
    platform = request.GET.get("platform")
    provider = pq_provider(pq, platform)
    story_details = provider.item(story_id)
    # PB: uses "platform" to create provider, but pq.provider_name for QuotaHistory??
    QuotaHistory.increment(request.user.id, request.user.is_staff, pq.provider_name)
    return HttpResponse(json.dumps({"story": story_details}, default=str), content_type="application/json",
                        status=200)

@handle_provider_errors
@api_view(['GET', 'POST'])
@authentication_classes([TokenAuthentication, SessionAuthentication])
@permission_classes([IsAuthenticated])
# @cache_by_kwargs()
def sources(request):
    pq = parse_query(request)
    provider = pq_provider(pq)
    try:
        response = provider.sources(f"({pq.query_str})", pq.start_date, pq.end_date, 10, **pq.provider_props)
    except requests.exceptions.ConnectionError:
        response = {'error': 'Max Retries Exceeded'}
    QuotaHistory.increment(request.user.id, request.user.is_staff, pq.provider_name, 4)
    return HttpResponse(json.dumps({"sources": response}, default=str), content_type="application/json",
                        status=200)

@require_http_methods(["GET"])
@action(detail=False)
def download_sources_csv(request):
    query = json.loads(request.GET.get("qS"))
    pq = parsed_query_from_dict(query[0])
    provider = pq_provider(pq)
    try:
        data = provider.sources(f"({pq.query_str})", pq.start_date,
                    pq.end_date, **pq.provider_props, sample_size=5000, limit=100)
    except Exception as e:
        logger.exception(e)
        return error_response(str(e), HttpResponseBadRequest)
    QuotaHistory.increment(request.user.id, request.user.is_staff, pq.provider_name, 2)
    filename = "mc-{}-{}-top-sources".format(pq.provider_name, _filename_timestamp())
    response = HttpResponse(
        content_type='text/csv',
        headers={'Content-Disposition': f"attachment; filename={filename}.csv"},
    )
    writer = csv.writer(response)
    # TODO: extract into a constant (global)
    cols = ['source', 'count']
    CSVWriterHelper.write_top_sources(writer, data, cols)
    return response


@handle_provider_errors
@api_view(['GET', 'POST'])
@authentication_classes([TokenAuthentication, SessionAuthentication])
@permission_classes([IsAuthenticated])
# @cache_by_kwargs()
def languages(request):
    pq = parse_query(request)
    provider = pq_provider(pq)
    try:
        response = provider.languages(f"({pq.query_str})", pq.start_date, pq.end_date, **pq.provider_props)
    except requests.exceptions.ConnectionError:
        response = {'error': 'Max Retries Exceeded'}
    QuotaHistory.increment(request.user.id, request.user.is_staff, pq.provider_name, 2)
    return HttpResponse(json.dumps({"languages": response}, default=str), content_type="application/json",
                        status=200)


@require_http_methods(["GET"])
@action(detail=False)
def download_languages_csv(request):
    query = json.loads(request.GET.get("qS"))
    pq = parsed_query_from_dict(query[0])
    provider = pq_provider(pq)
    try:
        data = provider.languages(f"({pq.query_str})", pq.start_date,
                    pq.end_date, **pq.provider_props, sample_size=5000, limit=100)
    except Exception as e: 
        logger.exception(e)
        return error_response(str(e), HttpResponseBadRequest)
    QuotaHistory.increment(request.user.id, request.user.is_staff, pq.provider_name, 2)
    filename = "mc-{}-{}-top-languages".format(pq.provider_name, _filename_timestamp())
    response = HttpResponse(
        content_type='text/csv',
        headers={'Content-Disposition': f"attachment; filename={filename}.csv"},
    )
    writer = csv.writer(response)
    cols = ['language', 'count', 'ratio']
    CSVWriterHelper.write_top_langs(writer, data, cols)
    return response


@handle_provider_errors
@api_view(['GET'])
@authentication_classes([TokenAuthentication])  # API-only method for now
@permission_classes([IsAuthenticated])
def story_list(request):
    pq = parse_query(request)
    provider = pq_provider(pq)
    # support returning text content for staff only
    if pq.provider_props.get('expanded') is not None:
        pq.provider_props['expanded'] = pq.provider_props['expanded'] == '1'
        if not request.user.is_staff:
            raise error_response("You are not permitted to fetch `expanded` stories.", HttpResponseForbidden)
    page, pagination_token = provider.paged_items(f"({pq.query_str})", pq.start_date, pq.end_date, **pq.provider_props, sort_field="indexed_date")
    QuotaHistory.increment(request.user.id, request.user.is_staff, pq.provider_name, 1)
    return HttpResponse(json.dumps({"stories": page, "pagination_token": pagination_token}, default=str),
                        content_type="application/json",
                        status=200)


@handle_provider_errors
@api_view(['GET', 'POST'])
@authentication_classes([TokenAuthentication, SessionAuthentication])
@permission_classes([IsAuthenticated])
# @cache_by_kwargs()
def words(request):
    pq = parse_query(request)
    provider = pq_provider(pq)
    try:
        words = provider.words(f"({pq.query_str})", pq.start_date, pq.end_date, **pq.provider_props)
    except requests.exceptions.ConnectionError:
        response = {'error': 'Max Retries Exceeded'}
    response = add_ratios(words)
    QuotaHistory.increment(request.user.id, request.user.is_staff, pq.provider_name, 4)
    return HttpResponse(json.dumps({"words": response}, default=str), content_type="application/json",
                        status=200)
                        


@require_http_methods(["GET"])
@action(detail=False)
def download_words_csv(request):
    query = json.loads(request.GET.get("qS"))
    pq = parsed_query_from_dict(query[0])
    provider = pq_provider(pq)
    try:
        words = provider.words(f"({pq.query_str})", pq.start_date,
                                pq.end_date, **pq.provider_props, sample_size=5000)
        words = add_ratios(words)
    except Exception as e:
        logger.exception(e)
        return error_response(str(e), HttpResponseBadRequest)
    QuotaHistory.increment(request.user.id, request.user.is_staff, pq.provider_name, 4)
    filename = "mc-{}-{}-top-words".format(pq.provider_name, _filename_timestamp())
    response = HttpResponse(
        content_type='text/csv',
        headers={'Content-Disposition': f"attachment; filename={filename}.csv"},
    )
    writer = csv.writer(response)
    cols = ['term', 'count', 'ratio']
    CSVWriterHelper.write_top_words(writer, words, cols)
    return response


@require_http_methods(["GET"])
@action(detail=False)
def download_counts_over_time_csv(request):
    query = json.loads(request.GET.get("qS"))
    pq = parsed_query_from_dict(query[0])
    provider = pq_provider(pq)
    try:
        data = provider.normalized_count_over_time(
            f"({pq.query_str})", pq.start_date, pq.end_date, **pq.provider_props)
        normalized = True
    except UnsupportedOperationException:
        data = provider.count_over_time(pq.query_str, pq.start_date, pq.end_date, **pq.provider_props)
        normalized = False
    QuotaHistory.increment(request.user.id, request.user.is_staff, pq.provider_name, 2)
    filename = "mc-{}-{}-counts".format(
        pq.provider_name, _filename_timestamp())
    response = HttpResponse(
        content_type='text/csv',
        headers={'Content-Disposition': f"attachment; filename={filename}.csv"},
    )
    writer = csv.writer(response)
    cols = ['date', 'count', 'total_count',
            'ratio'] if normalized else ['date', 'count']
    CSVWriterHelper.write_attn_over_time(writer, data, cols)
    return response


@login_required(redirect_field_name='/auth/login')
@require_http_methods(["GET"])
@action(detail=False)
def download_all_content_csv(request):
    queryState = json.loads(request.GET.get("qS"))
    data = []
    for query in queryState:
        pq = parsed_query_from_dict(query)
        provider = pq_provider(pq)
        data.append(provider.all_items(
            f"({pq.query_str})", pq.start_date, pq.end_date, **pq.provider_props))

    def data_generator():
        for result in data:
            first_page = True
            for page in result:
                QuotaHistory.increment(
                    request.user.id, request.user.is_staff, pq.provider_name)
                if first_page:  # send back column names, which differ by platform
                    yield sorted(list(page[0].keys()))
                for story in page:
                    ordered_story = collections.OrderedDict(
                        sorted(story.items()))
                    yield [v for k, v in ordered_story.items()]
                first_page = False

    filename = "mc-{}-{}-content".format(
        pq.provider_name, _filename_timestamp())
    streamer = csv_stream.CSVStream(filename, data_generator)
    return streamer.stream()


@login_required(redirect_field_name='/auth/login')
@handle_provider_errors
@require_http_methods(["POST"])
def send_email_large_download_csv(request):
    # get queryState and email
    payload = json.loads(request.body)
    queryState = payload.get('prepareQuery', None)
    email = payload.get('email', None)

    # follows similiar logic from download_all_content_csv, get information and send to tasks
    for query in queryState:
        pq = parsed_query_from_dict(query)
        provider = pq_provider(pq)
        try:
            count = provider.count(f"({pq.query_str})", pq.start_date, pq.end_date, **pq.provider_props)
            if count >= 25000 and count <= 200000:
                download_all_large_content_csv(queryState, request.user.id, request.user.is_staff, email)
        except UnsupportedOperationException:
            return error_response("Can't count results for download in {}... continuing anyway".format(pq.provider_name))
    return HttpResponse(content_type="application/json", status=200)


@login_required(redirect_field_name='/auth/login')
@require_http_methods(["POST"])
@action(detail=False)
def download_all_queries_csv(request):
    # get data from request (JSON encoded document with "queryState" in object/dict)
    payload = json.loads(request.body)
    queryState = payload.get('queryState', None)
    queries = [parsed_query_from_dict(query) for query in queryState]

    # make background task to fetch each query and zip into file then send email
    download_all_queries_csv_task(queries, request)
    return HttpResponse(content_type="application/json", status=200)


def add_ratios(words_data):
    for word in words_data:
        word["ratio"] = word['count'] / 1000
    return words_data


def _filename_timestamp() -> str:
    return time.strftime("%Y%m%d%H%M%S", time.localtime())
