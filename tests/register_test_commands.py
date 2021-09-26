import uuid
from otl_interpreter.interpreter_db import node_commands_manager


def register_test_commands():
    spark_node_id1 = uuid.uuid4().hex
    eep_node_id1 = uuid.uuid4().hex
    post_processing_node_id1 = uuid.uuid4().hex

    node_commands_manager.register_node('spark', spark_node_id1)
    node_commands_manager.register_node('eep', eep_node_id1)
    node_commands_manager.register_node('post_processing', post_processing_node_id1)

    node_commands_manager.register_required_commands(
        {
            "join": [{"type": "subsearch"}, ],
            "async": [{"type": "kwarg", "key": "name"}, {"type": "subsearch"}, ],
            "await": [{"type": "kwarg", "key": "name"},],
        }
    )
    node_commands_manager.register_node_commands(
        spark_node_id1,
        {
            'otstats':
                [
                    {
                        "type": "kwarg",
                        "key": "index",
                        "required": True,
                    },
                ],
            'collect':
                [
                    {
                        "type": "kwarg",
                        "key": "index",
                        "required": True,
                    }
                ],
            "readfile": [{"type": "arg"}, {"type": "arg"}, {"type": "arg"}],
        }
    )

    node_commands_manager.register_node_commands(
        eep_node_id1,
        {
            'sum':
                [
                    {
                        "type": "arg",
                        "inf": True,
                    }
                ],
            'merge_dataframes':
                [
                    {
                        "type": 'subsearch',
                        "inf": True
                    }

                ]
        }
    )

    node_commands_manager.register_node_commands(
        eep_node_id1,
        {
            'table':
                [
                    {"type": "arg"},
                    {"type": "arg"},
                    {"type": "arg"},
                    {"type": "kwarg"}
                ],
        }
    )

    node_commands_manager.register_node_commands(
        post_processing_node_id1,
        {
            'pp_command1':
                [
                    {
                        "type": "arg",
                    },
                ]
        }
    )