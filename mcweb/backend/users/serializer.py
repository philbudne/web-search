# credit to https://medium.com/@manjongmanka/password-resets-with-django-rest-framework-7122ffeadb6a
from rest_framework import serializers

class ResetRequestSerializer(serializers.Serializer):
    email = serializers.EmailField(required=True)
    reset_type = serializers.CharField(required=True)

class GiveAPIAccessSerializer(serializers.Serializer):
    token = serializers.CharField(write_only=True, required=True)
