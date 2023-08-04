from django.apps import AppConfig
from django.db.models.signals import post_delete
from django.dispatch import receiver


class SimpleRestJobProxyConfig(AppConfig):
    name = 'ot_simple_rest_job_proxy'

    def ready(self):
        from otl_interpreter.models import OtlJob
        from ot_simple_rest_job_proxy.job_proxy_manager import JobProxyManager
        job_proxy_manager = JobProxyManager()

        @receiver(post_delete, sender=OtlJob)
        def delete_info_from_redis(sender, instance, using, **kwargs):
            job_proxy_manager.delete_query_info(instance.uuid.hex)

        import logging
        log = logging.getLogger('otl_interpreter')
        try:
            from otl_interpreter.setup import __version__
            log.error(f'Plugin otl_interpreter is ready. Version is {__version__}')
        except ImportError as err:
            log.error(f'Plugin otl_interpreter is ready. Version is unknown')


