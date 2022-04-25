import datetime

from uuid import uuid4
from rest.test import TestCase
from otl_interpreter.interpreter_db.models import NodeJobResult
from otl_interpreter.interpreter_db.enums import ResultStorage, ResultStatus
from otl_interpreter.interpreter_db import node_job_manager


class TestNodeJobManager(TestCase):
    def test_set_not_exist_status(self):
        # create results with old timestamp
        for i in range(10):
            old_result = NodeJobResult(
                storage=ResultStorage.INTERPROCESSING,
                path=uuid4(),
                status=ResultStatus.CALCULATED,
                last_touched_timestamp=datetime.datetime.now() - datetime.timedelta(seconds=61)
            )
            old_result.save()

            not_old_result = NodeJobResult(
                storage=ResultStorage.SHARED_POST_PROCESSING,
                path=uuid4(),
                status=ResultStatus.CALCULATED,
                last_touched_timestamp=datetime.datetime.now()
            )
            not_old_result.save()

        l = node_job_manager.set_not_exist_status_for_expired_results()
        self.assertEqual(len(l), 10)

        for storage, path in l:
            self.assertEqual(l[0][0], ResultStorage.INTERPROCESSING)
