job_planer_commands = {
    "async": {"rules": [
        {
            "name": "name",
            "type": "kwarg",
            "key": "name",
            "required": True
        },

        {
            "name": "subsearch",
            "type": "subsearch",
            "required": True
        },
    ]},
    "await": {"rules": [
        {
            "name": "name",
            "type": "kwarg",
            "key": "name"
        },
        {
            "name": "override",
            "type": "kwarg",
            "key": "override", "required": False
        },
    ]},
    "sys_cache": {"rules": [
        {
            "name": "ttl",
            "type": "kwarg",
            "key": "ttl",
            "required": False,
        }]
    }
}

sys_computing_node_commands = {
    "sys_write_result": {"rules": [
        {
            "name": "path",
            "type": "arg",
            "required": True
        },
        {
            "name": "storage_type",
            "type": "arg",
            "required": True
        }
    ]},
    "sys_write_interproc": {"rules": [
        {
            "name": "path",
            "type": "arg",
            "required": True
        },
        {
            "name": "storage_type",
            "type": "arg",
            "required": True
        }
    ]},
    "sys_read_interproc": {"rules": [
        {
            "name": "path",
            "type": "arg",
            "required": True
        },
        {
            "name": "storage_type",
            "type": "arg",
            "required": True
        }
    ]},
}
