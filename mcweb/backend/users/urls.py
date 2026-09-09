from django.urls import path
from rest_framework import routers
from .api import RequestReset, ConfirmedEmail
from . import views

router = routers.DefaultRouter() 
router.register('request-reset', RequestReset, 'request-reset')
router.register('confirmed-email', ConfirmedEmail, 'confirmed-email')

urlpatterns = [
    path('login', views.login),
    path('logout', views.logout),
    path('register', views.register),
    path('profile', views.profile),
    path('reset-password', views.reset_password),
    path('request-reset', RequestReset.as_view()),
    path('delete-user', views.delete_user),
    path('reset-token', views.reset_token),
    path('email-from-token', views.email_from_token),
    path('users-quotas', views.users_quotas),
    path('email-confirmed', ConfirmedEmail.as_view()),
]
