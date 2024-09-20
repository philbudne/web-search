# Python
import datetime as dt
import json
import time
from typing import Callable, Dict, Generator, List, NamedTuple, Optional, Tuple

# PyPI
from django.apps import apps
from mc_providers import provider_by_name, provider_name, ContentProvider, \
    PLATFORM_TWITTER, PLATFORM_SOURCE_TWITTER, PLATFORM_YOUTUBE,\
    PLATFORM_SOURCE_YOUTUBE, PLATFORM_REDDIT, PLATFORM_SOURCE_PUSHSHIFT, PLATFORM_SOURCE_MEDIA_CLOUD,\
    PLATFORM_SOURCE_WAYBACK_MACHINE, PLATFORM_ONLINE_NEWS

# mcweb
from settings import ALL_CONTENT_CSV_EMAIL_MAX, ALL_CONTENT_CSV_EMAIL_MIN, NEWS_SEARCH_API_URL

class ParsedQuery(NamedTuple):
    start_date: dt.datetime
    end_date: dt.datetime
    query_str: str
    provider_props: dict
    provider_name: str
    api_key: str | None
    base_url: str | None
    caching: bool = True

_BASE_URL = {
    'onlinenews-mediacloud': NEWS_SEARCH_API_URL,
}


def pq_provider(pq: ParsedQuery, platform: Optional[str] = None) -> ContentProvider:
    """
    take parsed query, return mc_providers ContentProvider.
    (one place to pass new things to mc_providers)
    """
    return provider_by_name(platform or pq.provider_name, pq.api_key, pq.base_url, pq.caching)

# not used?
def fill_in_dates(start_date, end_date, existing_counts):
    delta = (end_date + dt.timedelta(1)) - start_date
    date_count_dict = {k['date']: k['count'] for k in existing_counts}

    # whether or not the dates in existing_counts are string types
    dates_as_strings = (len(date_count_dict.keys()) == 0) or (isinstance(next(iter(date_count_dict.keys())), str))
    if not dates_as_strings:
        date_count_dict = {dt.datetime.strftime(k, "%Y-%m-%d %H:%M:%S"): v for k, v in date_count_dict.items()}

    filled_counts = []
    for i in range(delta.days):
        day = start_date + dt.timedelta(days=i)
        day_string = dt.datetime.strftime(day, "%Y-%m-%d %H:%M:%S")
        if day_string not in date_count_dict.keys():
            filled_counts.append({"count": 0, "date": day_string})
        else:
            filled_counts.append({'count': date_count_dict[day_string], 'date': day_string})
    return filled_counts

def search_props_for_provider(provider, collections: List, sources: List, all_params: Dict) -> Dict:
    if provider == provider_name(PLATFORM_TWITTER, PLATFORM_SOURCE_TWITTER):
        return _for_twitter_api(collections, sources)
    if provider == provider_name(PLATFORM_YOUTUBE, PLATFORM_SOURCE_YOUTUBE):
        return _for_youtube_api(collections, sources)
    if provider == provider_name(PLATFORM_REDDIT, PLATFORM_SOURCE_PUSHSHIFT):
        return _for_reddit_pushshift(collections, sources)
    if provider == provider_name(PLATFORM_ONLINE_NEWS, PLATFORM_SOURCE_WAYBACK_MACHINE):
        return _for_wayback_machine(collections, sources)
    if provider == provider_name(PLATFORM_ONLINE_NEWS, PLATFORM_SOURCE_MEDIA_CLOUD):
        return _for_media_cloud(collections, sources, all_params)
    return {}


def _for_youtube_api(collections: List, sources: List) -> Dict:
    # TODO: filter by a list of channels
    return dict()


def _for_twitter_api(collections: List, sources: List) -> Dict:
    # pull these in at runtime, rather than outside class, so we can make sure the models are loaded
    Source = apps.get_model('sources', 'Source')
    usernames = []
    # turn media ids into list of usernames
    selected_sources = Source.objects.filter(id__in=sources)
    usernames += [s.name for s in selected_sources]
    # turn collections ids into list of usernames
    selected_sources = Source.objects.filter(collections__id__in=collections)
    usernames += [s.name for s in selected_sources]
    return dict(usernames=usernames)


def _for_reddit_pushshift(collections: List, sources: List) -> Dict:
    # pull these in at runtime, rather than outside class, so we can make sure the models are loaded
    Source = apps.get_model('sources', 'Source')
    subreddits = []
    # turn media ids into list of subreddits
    selected_sources = Source.objects.filter(id__in=sources)
    subreddits += [s.name for s in selected_sources]
    # turn collections ids into list of subreddits
    selected_sources = Source.objects.filter(collections__id__in=collections)
    subreddits += [s.name for s in selected_sources]
    # clean up names
    subreddits = [s.replace('/r/', '') for s in subreddits]
    return dict(subreddits=subreddits)


def _for_wayback_machine(collections: List, sources: List) -> Dict:
    # pull these in at runtime, rather than outside class, so we can make sure the models are loaded
    Source = apps.get_model('sources', 'Source')
    # 1. pull out all unique domains that don't have url_search_strs
    domains = []
    # turn media ids into list of domains
    selected_sources = Source.objects.filter(id__in=sources)
    domains += [s.name for s in selected_sources if s.url_search_string is None]
    # turn collections ids into list of domains
    selected_sources_in_collections = Source.objects.filter(collections__id__in=collections)
    selected_sources_in_collections = [s for s in selected_sources_in_collections if s.name is not None]
    domains += [s.name for s in selected_sources_in_collections if bool(s.url_search_string) is False]
    # 2. pull out all the domains that have url_search_strings and turn those into search clauses
    # CURRENTLY URL_SEARCH_STRINGS ARE NOT IMPLEMENTED IN WB SYSTEM
    # sources_with_url_search_strs = []
    # sources_with_url_search_strs += [s for s in selected_sources if bool(s.url_search_string) is not False]
    # sources_with_url_search_strs += [s for s in selected_sources_in_collections if bool(s.url_search_string) is not False]
    # domain_url_filters = ["(domain:{} AND url:*{}*)".format(s.name, s.url_search_string) for s in sources_with_url_search_strs]
    return dict(domains=domains)

def _for_media_cloud(collections: List, sources: List, all_params: Dict) -> Dict:
    # pull these in at runtime, rather than outside class, so we can make sure the models are loaded
    Source = apps.get_model('sources', 'Source')
    # 1. pull out all unique domains that don't have url_search_strs
    domains = []
    # turn media ids into list of domains
    selected_sources = Source.objects.filter(id__in=sources)
    domains += [s.name for s in selected_sources if s.url_search_string is None]
    # turn collections ids into list of domains
    selected_sources_in_collections = Source.objects.filter(collections__id__in=collections)
    selected_sources_in_collections = [s for s in selected_sources_in_collections if s.name is not None]
    domains += [s.name for s in selected_sources_in_collections if bool(s.url_search_string) is False]
    # 2. pull out all the domains that have url_search_strings and turn those into search clauses
    #    note: ignore sources whose domain is in the list of domains that don't have a url_search_string (e.g. if
    #    parent bizjournals.com is in domain list then ignore town-specific bizjournals.com to reduce query length)
    sources_with_url_search_strs = []
    sources_with_url_search_strs += [s for s in selected_sources if bool(s.url_search_string) is not False
                                     and s.name not in domains]
    sources_with_url_search_strs += [s for s in selected_sources_in_collections if bool(s.url_search_string) is not False
                                     and s.name not in domains]
    domain_url_filters = ["(canonical_domain:{} AND url:*{}*)".format(s.name, s.url_search_string)
                          for s in sources_with_url_search_strs]
    # 3. assemble and add in other supported params
    supported_extra_props = ['pagination_token', 'page_size', 'sort_field', 'sort_order',
                             'expanded']  # make sure nothing nefarious gets through
    extra_props = dict(domains=domains, filters=domain_url_filters, chunk=True) 
    for prop_name in supported_extra_props:
        if prop_name in all_params:
            extra_props[prop_name] = all_params.get(prop_name)
    return extra_props

def filename_timestamp() -> str:
    """
    used for CSV & ZIP filenames in both views.py and tasks.py
    """
    return time.strftime("%Y%m%d%H%M%S", time.localtime())

def all_content_csv_generator(pqs: list[ParsedQuery], user_id, is_staff) -> Callable[[],Generator[list, None, None]]:
    """
    returns function returning generator for "total attention" CSV file
    with rows from all queries.
    used for both immediate CSV download (download_all_content_csv)
    and emailed CSV (download_all_large_content_csv)
    """
    def data_generator() -> Generator[list, None, None]:
        # phil: moved outside per-query loop (so headers appear once)
        first_page = True
        for pq in pqs:
            provider = pq_provider(pq)
            result = provider.all_items(f"({pq.query_str})", pq.start_date, pq.end_date, **pq.provider_props)
            for page in result:
                QuotaHistory.increment(user_id, is_staff, pq.provider_name)
                if first_page:  # send back column names, which differ by platform
                    yield sorted(page[0].keys())
                    first_page = False
                for story in page:
                    yield [v for k, v in sorted(story.items())]
    return data_generator

def all_content_csv_basename(pqs: list[ParsedQuery]) -> str:
    """
    returns a base filename for CSV and ZIP filenames
    """
    base_filename = "mc-{}-{}-content".format(pqs[-1].provider_name, filename_timestamp())
    return base_filename
