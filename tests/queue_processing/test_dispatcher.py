from gainy.queue_processing.models import QueueMessage
from gainy.trading.drivewealth.queue_message_handler import DriveWealthQueueMessageHandler
from gainy.queue_processing.dispatcher import QueueMessageDispatcher


def test_drivewealth(monkeypatch):
    record = {
        "messageId": "messageId",
        "eventSourceARN": "eventSourceARN",
        "body": "body",
    }
    message = QueueMessage()

    handle_called = False
    handler = DriveWealthQueueMessageHandler(None, None, None, None, None)

    def mock_handle(_message: QueueMessage):
        nonlocal handle_called
        handle_called = True
        assert _message == message

    monkeypatch.setattr(handler, "handle", mock_handle)

    def mock_supports(_message: QueueMessage):
        assert _message == message
        return True

    monkeypatch.setattr(handler, "supports", mock_supports)

    queue_message_dispatcher = QueueMessageDispatcher([handler])
    queue_message_dispatcher.handle(message)

    assert handle_called
