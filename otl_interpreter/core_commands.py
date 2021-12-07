job_planer_commands = {
    "async": {"rules": [
        {
            "type": "kwarg", "key": "name", "required": True
        },

        {
            "type": "subsearch",
            "required": True
        },
    ]},
    "await": {"rules": [
        {"type": "kwarg", "key": "name"},
        {
            "type": "kwarg", "key": "override", "required": False
        },
    ]},
}

sys_computing_node_commands = {
    "sys_write_result": {"rules": [
        {
            "type": "arg",
            "required": True
        },
        {
            "type": "arg",
            "required": True
        }
    ]},
    "sys_write_interproc": {"rules": [
        {
            "type": "arg",
            "required": True
        },
        {
            "type": "arg",
            "required": True
        }
    ]},
    "sys_read_interproc": {"rules": [
        {
            "type": "arg",
            "required": True
        },
        {
            "type": "arg",
            "required": True
        }
    ]},
}
