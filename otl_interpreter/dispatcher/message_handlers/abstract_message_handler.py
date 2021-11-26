from abc import abstractmethod, ABC
from message_broker import Message


class MessageHandler(ABC):
    @abstractmethod
    async def process_message(self, message: Message) -> None:
        raise NotImplementedError
