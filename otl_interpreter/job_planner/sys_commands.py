from otlang.sdk.argument import Command


class SysReadWriteCommand(Command):
    def set_path(self, path, storage_type=None):
        self.add_argument('path', value=path)
        if storage_type is not None:
            self.add_argument('storage_type', value=storage_type)


class SysWriteResultCommand(SysReadWriteCommand):
    def __init__(self):
        super().__init__(name='sys_write_result')


class SysReadInterprocCommand(SysReadWriteCommand):
    def __init__(self):
        super().__init__(name='sys_read_interproc')


class SysWriteInterprocCommand(SysReadWriteCommand):
    def __init__(self):
        super().__init__(name='sys_write_interproc')



