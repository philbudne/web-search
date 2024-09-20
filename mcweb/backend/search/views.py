import json
import logging
import csv
import time
import collections
import requests
from typing import Optional

# PyPI
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
from .utils import ParsedQuery, all_content_csv_basename, all_content_csv_generator, filename_timestamp, pq_provider
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

def parse_date_str(date_str: str) -> dt.datetime:
    """
    accept both YYYY-MM-DD and MM/DD/YYYY
    (was accepting former in JSON and latter in GET/query-str)
    """
    if '-' in date_str:
        return dt.datetime.strptime(date_str, '%Y-%m-%d')
    else:
        return dt.datetime.strptime(date_str, '%m/%d/%Y')


def listify(input: str) -> List[str]:
    if input:
        return input.split(',')
    return []

def _get_api_key(provider: str) -> Optional[str]:
    # no system-level API keys right now
    return None

def parse_query(request) -> ParsedQuery:
    if request.method == 'POST':
        payload = json.loads(request.body).get("queryObject")
        return parsed_query_from_dict(payload)

    provider_name = request.GET.get("p", 'onlinenews-mediacloud')
    query_str = request.GET.get("q", "*")
    collections = listify(request.GET.get("cs", None))
    sources = listify(request.GET.get("ss", None))
    provider_props = search_props_for_provider(
        provider_name,
        collections,
        sources,
        request.GET
    )
    start_date = parse_date_str(request.GET.get("start", "2010-01-01"))
    end_date = parse_date_str(request.GET.get("end", "2030-01-01"))
    api_key = _get_api_key(provider_name)
    base_url = _BASE_URL.get(provider_name)

    # caching is enabled unless cache is passed ONCE with "f" or "0" as value
    caching = request.GET.get("cache", "1") not in ["f", "0"]

    return ParsedQuery(start_date=start_date, end_date=end_date,
                       query_str=query_str, provider_props=provider_props,
                       provider_name=provider_name, api_key=api_key,
                       base_url=base_url, caching=caching)


def parsed_query_from_dict(payload) -> ParsedQuery:
    """
    Takes a queryObject dict, returns ParsedQuery
    """
    provider_name = payload["platform"]
    query_str = payload["query"]
    collections = payload["collections"]
    sources = payload["sources"]
    provider_props = search_props_for_provider(provider_name, collections, sources, payload)
    start_date = parse_date_str(payload["startDate"])
    end_date = parse_date_str(payload["endDate"])
    api_key = _get_api_key(provider_name)
    base_url = _BASE_URL.get(provider_name)
    caching = payload.get("caching", True)
    return ParsedQuery(start_date=start_date, end_date=end_date,
                       query_str=query_str, provider_props=provider_props,
                       provider_name=provider_name, api_key=api_key,
                       base_url=base_url, caching=caching)

def parsed_query_state_and_params(request, qs_key="queryState") -> Tuple[List[ParsedQuery], Dict]:
    """
    this to handle views.send_email_large_download_csv (queries + email)
    and the more usual case of just a set of queries
    """
    if request.method == 'POST':
        params = json.loads(request.body)
        queries = params.get(qs_key)
    else:
        params = request.GET
        queries = json.loads(params.get("qS"))

    pqs = [parsed_query_from_dict(q) for q in queries]
    return (pqs, params)

def parsed_query_state(request) -> List[ParsedQuery]:
    """
    return list of parsed queries from "queryState" (list of dicts).
    Expects POST with JSON object with a "queryState" element (download-all-queries)
    or GET with qs=JSON_STRING (many)
    """
    pqs, params = parsed_query_state_and_params(request)
    return pqs

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
    queries = parsed_query_state(request) # handles POST!
    pq = queries[0]

    provider = pq_provider(pq)
    try:
        data = provider.sources(f"({pq.query_str})", pq.start_date,
                    pq.end_date, **pq.provider_props, sample_size=5000, limit=100)
    except Exception as e:
        logger.exception(e)
        return error_response(str(e), HttpResponseBadRequest)
    QuotaHistory.increment(request.user.id, request.user.is_staff, pq.provider_name, 2)
    filename = "mc-{}-{}-top-sources".format(pq.provider_name, filename_timestamp())
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
    queries = parsed_query_state(request) # handles POST!
    pq = queries[0]
    provider = pq_provider(pq)
    try:
        data = provider.languages(f"({pq.query_str})", pq.start_date,
                    pq.end_date, **pq.provider_props, sample_size=5000, limit=100)
    except Exception as e: 
        logger.exception(e)
        return error_response(str(e), HttpResponseBadRequest)
    QuotaHistory.increment(request.user.id, request.user.is_staff, pq.provider_name, 2)
    filename = "mc-{}-{}-top-languages".format(pq.provider_name, _ilename_timestamp())
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
    queries = parsed_query_state(request) # handles POST!
    pq = queries[0]
    provider = pq_provider(pq)
    try:
        words = provider.words(f"({pq.query_str})", pq.start_date,
                                pq.end_date, **pq.provider_props, sample_size=5000)
        words = add_ratios(words)
    except Exception as e:
        logger.exception(e)
        return error_response(str(e), HttpResponseBadRequest)
    QuotaHistory.increment(request.user.id, request.user.is_staff, pq.provider_name, 4)
    filename = "mc-{}-{}-top-words".format(pq.provider_name, filename_timestamp())
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
    queries = parsed_query_state(request) # handles POST!
    pq = queries[0]
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
        pq.provider_name, filename_timestamp())
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
    parsed_queries = parsed_query_state(request) # handles POST!
    data_generator = all_content_csv_generator(parsed_queries, request.user.id, request.user.is_staff)
    filename = all_content_csv_basename(parsed_queries)
    streamer = csv_stream.CSVStream(filename, data_generator)
    return streamer.stream()


# called by frontend sendTotalAttentionDataEmail
@login_required(redirect_field_name='/auth/login')
@handle_provider_errors
@require_http_methods(["POST"])
def send_email_large_download_csv(request):
    # get queries and email
    pqs, payload = parsed_query_state_and_params(request, 'prepareQuery')
    email = payload.get('email', None)

    # follows similiar logic from download_all_content_csv, get information and send to tasks
    total = 0
    for pq in pqs:
        provider = pq_provider(pq)
        try:
            total += provider.count(f"({pq.query_str})", pq.start_date, pq.end_date, **pq.provider_props)
            # NOTE! The same limit numbers appear (twice) in
            # mcweb/frontend/src/features/search/util/TotalAttentionEmailModal.jsx
            # gives no indication that count wasn't in range!!!
            if count >= 25000 and count <= 200000:
                # WHOA! this calls download_all_large_content_csv with the full list of (unparsed) queries
                # for each query in the list?! maybe desire is to pass list with single query????

                download_all_large_content_csv(pqs, request.user.id, request.user.is_staff, email)
        except UnsupportedOperationException:
            # says "continuing anyway", but doesn't?!
            return error_response("Can't count results for download in {}... continuing anyway".format(pq.provider_name))
    return HttpResponse(content_type="application/json", status=200)


@login_required(redirect_field_name='/auth/login')
@require_http_methods(["POST"])
@action(detail=False)
def download_all_queries_csv(request):
    queries = parsed_query_state(request) # handles GET with qS=JSON

    # make background task to fetch each query and zip into file then send email
    download_all_queries_csv_task(queries, request)
    return HttpResponse(content_type="application/json", status=200)


def add_ratios(words_data):
    for word in words_data:
        word["ratio"] = word['count'] / 1000
    return words_data


