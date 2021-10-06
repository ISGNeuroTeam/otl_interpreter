from uuid import uuid4
from .command import Command


def make_sys_write_result_command(address):
    return Command(
        name='sys_write_result',
        args=[
            {'value': {'value': address,
                       'type': 'string',
                       'leaf_type': 'simple'},
             'type': 'arg',
             'leaf_type': 'complex'}
        ]
    )


def make_sys_read_interproc(address):
    return Command(
        name='sys_read_interproc',
        args=[
            {'value': {'value': address,
                       'type': 'string',
                       'leaf_type': 'simple'},
             'type': 'arg',
             'leaf_type': 'complex'}
        ]
    )


def make_sys_write_interproc(address):
    return Command(
        name='sys_write_interproc',
        args=[
            {'value': {'value': address,
                       'type': 'string',
                       'leaf_type': 'simple'},
             'type': 'arg',
             'leaf_type': 'complex'}
        ]
    )
