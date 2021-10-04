from uuid import uuid4
from .command import Command, Arg, ArgType


def make_sys_write_result_command():
    return Command(
        name='sys_write_result',
        args=[
            Arg(
                arg_type=ArgType.NAMED,
                arg_data_type='string',
                arg_value=uuid4().hex,
                arg_name='address',

            )
        ]
    )


def make_sys_read_interproc(address):
    return Command(
        name='sys_read_interproc',
        args=[
            Arg(
                arg_type=ArgType.NAMED,
                arg_data_type='string',
                arg_value=address,
                arg_name='address',

            )
        ]
    )


def make_sys_write_interproc(address):
    return Command(
        name='sys_write_interproc',
        args=[
            Arg(
                arg_type=ArgType.NAMED,
                arg_data_type='string',
                arg_value=address,
                arg_name='address',

            )
        ]
    )
