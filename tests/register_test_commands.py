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
    node_commands_manager.register_node('SPARK', spark_node_id1, default_resources)
    node_commands_manager.register_node('SPARK', spark_node_id2, default_resources)

    node_commands_manager.register_node('EEP', eep_node_id1, default_resources)
    node_commands_manager.register_node('POST_PROCESSING', post_processing_node_id1, default_resources)

    node_commands_manager.register_node_commands(
        spark_node_id1,
        {
            'otstats': {
                "rules":
                [
                    {
                        "type": "kwarg",
                        "key": "index",
                        "required": True,
                    },
                ]},
            'collect': {
                "rules":
                [
                    {
                        "type": "kwarg",
                        "key": "index",
                        "required": True,
                    }
                ]},
            "readfile": {"rules": [{"type": "arg"}, {"type": "arg"}, {"type": "arg"}]},
            "join": {"rules": [

                {
                    "type": "subsearch",
                    "required": True
                },
            ]},
        }
    )

    node_commands_manager.register_node_commands(
        spark_node_id2,
        {
            'otstats': {
                "rules":
                [
                    {
                        "type": "kwarg",
                        "key": "index",
                        "required": True,
                    },
                ]},
            'collect': {
                "rules":
                [
                    {
                        "type": "kwarg",
                        "key": "index",
                        "required": True,
                    }
                ]},
            "readfile": {"rules": [{"type": "arg"}, {"type": "arg"}, {"type": "arg"}]},
            "join": {"rules": [

                {
                    "type": "subsearch",
                    "required": True
                },
            ]},
        }
    )

    node_commands_manager.register_node_commands(
        eep_node_id1,
        {
            'sum': {
                "rules":
                [
                    {
                        "type": "arg",
                        "inf": True,
                    }
                ]},
            'merge_dataframes': {
                "rules":
                [
                    {
                        "type": 'subsearch',
                        "inf": True
                    }

                ]
            },
            "join": {"rules": [

                {
                    "type": "subsearch",
                    "required": True
                },
            ]}
        }
    )

    node_commands_manager.register_node_commands(
        eep_node_id1,
        {
            'table': {
                "rules":
                [
                    {"type": "arg"},
                    {"type": "arg"},
                    {"type": "arg"},
                    {"type": "kwarg"}
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
                        "type": "arg",
                        "required": True,
                    },
                ]
            },
            "join": {"rules": [

                {
                    "type": "subsearch",
                    "required": True
                },
            ]}
        }
    )