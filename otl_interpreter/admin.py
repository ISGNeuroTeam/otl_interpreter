from django.contrib.admin import ModelAdmin, register
from .models import *


@register(OtlQuery)
class OtlQueryAdmin(ModelAdmin):
    pass


@register(NodeJob)
class NodeJobAdmin(ModelAdmin):
    pass
