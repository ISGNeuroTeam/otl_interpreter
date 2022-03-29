import uuid
from otl_interpreter.interpreter_db import node_commands_manager


def register_test_commands():
    spark_node_id1 = uuid.uuid4().hex
    spark_node_id2 = uuid.uuid4().hex

    eep_node_id1 = uuid.uuid4().hex
    post_processing_node_id1 = uuid.uuid4().hex

    default_resources = {
        'job_capacity': 1000000
    }
    node_commands_manager.register_node('SPARK', spark_node_id1, 'test_host_id', default_resources)
    node_commands_manager.register_node('SPARK', spark_node_id2, 'test_host_id', default_resources)

    node_commands_manager.register_node('EEP', eep_node_id1, 'test_host_id', default_resources)
    node_commands_manager.register_node('POST_PROCESSING', post_processing_node_id1, 'test_host_id', default_resources)

    spark_commands = {
        'otstats': {
            "rules":
            [
                {
                    "name": "index",
                    "type": "kwarg",
                    "key": "index",
                    "required": True,
                },
            ]},
        'collect': {
            "rules":
            [
                {
                    "name": "index",
                    "type": "kwarg",
                    "key": "index",
                    "required": True,
                }
            ]},
        "readfile": {"rules": [
            {"name": "file1", "type": "arg"},
            {"name": "file2", "type": "arg"},
            {"name": "file3", "type": "arg"}
        ]},
        "join": {"rules": [

            {
                "name": "subsearch",
                "type": "subsearch",
                "required": True
            },
        ]},
    }

    eep_commands = {
        'sum': {
            "rules":
            [
                {
                    "name": "argument",
                    "type": "arg",
                    "inf": True,
                }
            ]},
        'merge_dataframes': {
            "rules":
            [
                {
                    "name": 'subsearch',
                    "type": 'subsearch',
                    "inf": True
                }

            ]
        },
        "join": {"rules": [
            {
                "name": "subsearch",
                "type": "subsearch",
                "required": True
            },
        ]}
    }

    node_commands_manager.register_node_commands(
        spark_node_id1,
        spark_commands
    )

    node_commands_manager.register_node_commands(
        spark_node_id2,
        spark_commands
    )

    node_commands_manager.register_node_commands(
        eep_node_id1,
        eep_commands
    )

    node_commands_manager.register_node_commands(
        eep_node_id1,
        {
            'table': {
                "rules":
                [
                    {"name": "positional_arg1", "type": "arg"},
                    {"name": "positional_arg2", "type": "arg"},
                    {"name": "positional_arg3", "type": "arg"},
                    {"name": "last_argument", "type": "kwarg"}
                ]},
        }
    )

    node_commands_manager.register_node_commands(
        post_processing_node_id1,
        {
            'pp_command1': {
                "rules":
                [
                    {
                        "name": "argument1",
                        "type": "arg",
                        "required": True,
                    },
                ]
            },
            "join": {"rules": [

                {
                    "name": "subsearch",
                    "type": "subsearch",
                    "required": True
                },
            ]}
        }
    )