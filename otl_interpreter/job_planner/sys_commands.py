from .command import Command


class SysReadWriteCommand(Command):
    def set_path(self, path, storage_type=None):
        self._add_positional_arg(path)
        if storage_type is not None:
            self._add_positional_arg(storage_type)


class SysWriteResultCommand(SysReadWriteCommand):
    def __init__(self):
        super().__init__('sys_write_result')


class SysReadInterprocCommand(SysReadWriteCommand):
    def __init__(self):
        super().__init__('sys_read_interproc')


class SysWriteInterprocCommand(SysReadWriteCommand):
    def __init__(self):
        super().__init__('sys_write_interproc')



