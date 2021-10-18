class Command:
    def __init__(self, name=None, args=None):
        self.name = name or None
        self.args = args or []

    @staticmethod
    def make_command_from_translated_otl(translated_command):
        name = Command._get_name_from_translated_otl(translated_command)
        args = Command._get_args_from_translated_otl(translated_command)
        command = Command(name, args)
        return command

    @staticmethod
    def _get_name_from_translated_otl(translated_command):
        return translated_command['command']['value']

    @staticmethod
    def _get_args_from_translated_otl(translated_command):
        return translated_command['commandargs']

    def _add_positional_arg(self, arg_value):
        self.args.append(
            {
                'value': {'value': arg_value,
                       'type': 'string',
                       'leaf_type': 'simple'},
             'type': 'arg',
             'leaf_type': 'complex'
             }
        )





