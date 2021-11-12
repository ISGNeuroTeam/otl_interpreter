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

    def set_timewindow(self, tws, twf):
        """
        Appends two named argument 'earliest' and 'latest' to the command args
        :param tws: timewindow start timestamp
        :param twf: timewindow finish timestamp
        :return:
        """
        self._add_named_arg('earliest', int(tws.timestamp()))
        self._add_named_arg('latest', int(twf.timestamp()))

    @staticmethod
    def _get_name_from_translated_otl(translated_command):
        return translated_command['command']['value']

    @staticmethod
    def _get_args_from_translated_otl(translated_command):
        return translated_command['commandargs']

    def _add_positional_arg(self, arg_value):
        self.args.append([
            {
                'value': {'value': arg_value,
                       'type': 'string',
                       'leaf_type': 'simple'},
             'type': 'arg',
             'leaf_type': 'complex'
             }, ]
        )

    def _add_named_arg(self, arg_name, arg_value):
        self.args.append(
            [
                {
                    "key": {
                        "type": "term",
                        "value": arg_name,
                        "leaf_type": "simple"
                    },
                    "type": "kwarg",
                    "value": {
                        "type": "arg",
                        "value": {
                            "type": "term",
                            "value": arg_value,
                            "leaf_type": "simple"
                        },
                        "leaf_type": "complex"
                    },
                    "leaf_type": "complex"
                },
            ]
        )




