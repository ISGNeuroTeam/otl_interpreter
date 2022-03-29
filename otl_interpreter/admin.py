import json

from django.db.models import JSONField
from django.forms import widgets
from django.contrib.admin import ModelAdmin, register
from mptt.admin import MPTTModelAdmin

from otl_interpreter.models import *


class PrettyJSONWidget(widgets.Textarea):

    def format_value(self, value):
        try:
            value = json.dumps(json.loads(value), indent=2, sort_keys=False)
            # these lines will try to adjust size of TextArea to fit to content
            row_lengths = [len(r) for r in value.split('\n')]
            self.attrs['rows'] = min(max(len(row_lengths) + 2, 10), 30)
            self.attrs['cols'] = min(max(max(row_lengths) + 2, 40), 120)
            return value
        except Exception as e:
            return super(PrettyJSONWidget, self).format_value(value)


class JsonAdmin(ModelAdmin):
    formfield_overrides = {
        JSONField: {'widget': PrettyJSONWidget}
    }


@register(OtlJob)
class OtlQueryAdmin(ModelAdmin):
    list_display = ['query', 'uuid']


@register(NodeJob)
class NodeJobAdmin(MPTTModelAdmin):
    list_display = ['computing_node_type', 'result', 'status']

    search_fields = ['otl_job__uuid', ]

    formfield_overrides = {
        JSONField: {'widget': PrettyJSONWidget}
    }

@register(ComputingNode)
class ComputingNodeAdmin(ModelAdmin):
    list_display = ['type', 'uuid']


@register(NodeCommand)
class NodeCommandAdmin(ModelAdmin):
    formfield_overrides = {
        JSONField: {'widget': PrettyJSONWidget}
    }
    list_display = ['name', 'node', ]


@register(NodeJobResult)
class NodeJobResultAdmin(ModelAdmin):
    list_display = ['path']




