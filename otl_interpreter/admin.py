from django.contrib.admin import ModelAdmin, register
from mptt.admin import MPTTModelAdmin

from .models import *


@register(OtlJob)
class OtlQueryAdmin(ModelAdmin):
    pass


@register(NodeJob)
class NodeJobAdmin(MPTTModelAdmin):
    list_display = ['computing_node_type', 'uuid', 'status']


@register(ComputingNode)
class ComputingNodeAdmin(ModelAdmin):
    list_display = ['type', 'guid']


@register(NodeCommand)
class NodeCommandAdmin(ModelAdmin):
    list_display = ['name', 'node', ]


@register(NodeJobResult)
class NodeJobResultAdmin(ModelAdmin):
    list_display = ['path']




