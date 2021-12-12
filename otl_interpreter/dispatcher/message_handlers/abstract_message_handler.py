from abc import abstractmethod, ABC
from message_broker import Message


class MessageHandler(ABC):
    @abstractmethod
    async def process_message(self, message: Message) -> None:
        raise NotImplementedError

    @abstractmethod
    async def __aenter__(self):
        raise NotImplementedError

    @abstractmethod
    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        raise NotImplementedError
