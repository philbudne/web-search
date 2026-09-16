import logging

from django.conf import settings
from rest_framework.authentication import SessionAuthentication

logger = logging.getLogger(__name__) # for debug

def query_rate(request) -> str | None:
    """
    Runtime rate limit
    """
    # check staff first to sidestep possible additional database access for groups:
    if request.user.is_staff or request.user.groups.filter(name=settings.GROUPS.HIGH_RATE_LIMIT).exists():
        return "100/m"

    # TEMPORARY: until api-client library no longer has hardwired 2 per minute;
    # then can raise limit above 2/m and law abiding citizens will benefit.
    if isinstance(request.successful_authenticator, SessionAuthentication):
        return "3/m"

    return "2/m"



