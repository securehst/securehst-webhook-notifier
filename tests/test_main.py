from unittest.mock import MagicMock

import pytest
import requests
import responses

from securehst_webhook_notifier.main import (
    PREFECT_AVAILABLE,
    create_state_hooks,
    notify_webhook,
    prefect_notify_webhook,
    send_prefect_notification,
    send_start_notification,
    send_webhook_message,
)


@pytest.fixture
def mock_webhook_url():
    return "https://test.webhook.url/path"


@responses.activate
def test_send_webhook_message_discord(mock_webhook_url):
    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Test function
    send_webhook_message(mock_webhook_url, "Test message", "discord")

    # Verify
    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == mock_webhook_url
    assert responses.calls[0].request.body == b'{"content": "Test message"}'


@responses.activate
def test_send_webhook_message_mattermost(mock_webhook_url):
    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Test function
    send_webhook_message(mock_webhook_url, "Test message", "mattermost")

    # Verify
    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == mock_webhook_url
    assert responses.calls[0].request.body == b'{"text": "Test message"}'


@responses.activate
def test_send_webhook_message_slack(mock_webhook_url):
    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Test function
    send_webhook_message(mock_webhook_url, "Test message", "slack")

    # Verify
    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == mock_webhook_url
    assert responses.calls[0].request.body == b'{"text": "Test message"}'


def test_send_webhook_message_invalid_platform(mock_webhook_url):
    with pytest.raises(ValueError, match=r"Unsupported platform 'invalid' for webhook messaging."):
        send_webhook_message(mock_webhook_url, "Test message", "invalid")


@responses.activate
def test_send_webhook_message_request_exception(mock_webhook_url):
    # Setup response to fail
    responses.add(responses.POST, mock_webhook_url, json={"error": "Internal server error"}, status=500)

    with pytest.raises(requests.RequestException):
        send_webhook_message(mock_webhook_url, "Test message", "slack")


@responses.activate
def test_notify_webhook_success(mock_webhook_url):
    # Setup responses for start and end notifications
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Create decorated function
    @notify_webhook(mock_webhook_url, "test_function")
    def sample_function():
        return "Function executed successfully"

    # Execute function
    result = sample_function()

    # Verify function result
    assert result == "Function executed successfully"

    # Verify webhook calls
    assert len(responses.calls) == 2
    start_body = responses.calls[0].request.body.decode()
    end_body = responses.calls[1].request.body.decode()

    # Use Unicode escape sequence or match partial text without emoji
    assert "Automation has started" in start_body
    assert "Automation has completed successfully" in end_body
    assert "Function Caller: test_function" in start_body
    assert "Function Caller: test_function" in end_body


@responses.activate
def test_notify_webhook_with_custom_message(mock_webhook_url):
    # Setup responses
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Create decorated function
    @notify_webhook(mock_webhook_url, "test_function", custom_message="Custom message!")
    def sample_function():
        return "Done"

    # Execute function
    sample_function()

    # Verify webhook calls
    assert len(responses.calls) == 2
    assert "Return Message: Done" in responses.calls[1].request.body.decode()


@responses.activate
def test_notify_webhook_with_exception(mock_webhook_url):
    # Setup responses
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Create decorated function that raises an exception
    @notify_webhook(mock_webhook_url, "test_function_error")
    def failing_function():
        raise ValueError("Test error message")

    # Execute function and expect exception
    with pytest.raises(ValueError, match="Test error message"):
        failing_function()

    # Verify webhook calls
    assert len(responses.calls) == 2
    start_body = responses.calls[0].request.body.decode()
    error_body = responses.calls[1].request.body.decode()

    assert "Automation has started" in start_body
    assert "Automation has crashed" in error_body
    assert "Error: Test error message" in error_body


@responses.activate
def test_notify_webhook_with_sql_error(mock_webhook_url):
    # Setup responses
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Create decorated function that raises an SQL error
    @notify_webhook(mock_webhook_url, "test_sql_error")
    def sql_error_function():
        raise ValueError("Database error [SQL: SELECT * FROM table WHERE id = 123] details")

    # Execute function and expect exception
    with pytest.raises(ValueError):
        sql_error_function()

    # Verify webhook calls
    assert len(responses.calls) == 2
    error_message = responses.calls[1].request.body.decode()

    # More precise assertions
    assert "Automation has crashed" in error_message

    # Check for sanitized error - it might include "Database error" and "details" separately
    assert "Database error" in error_message
    assert "details" in error_message
    # Verify SQL statement is removed
    assert "[SQL:" not in error_message
    assert "SELECT * FROM table WHERE id = 123" not in error_message


@responses.activate
def test_notify_webhook_with_user_mention(mock_webhook_url):
    # Setup responses
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Test with Mattermost
    @notify_webhook(mock_webhook_url, "test_function", platform="mattermost", user_id="user123")
    def failing_mattermost_function():
        raise ValueError("Test error")

    with pytest.raises(ValueError):
        failing_mattermost_function()

    assert "@user123" in responses.calls[1].request.body.decode()

    # Reset responses
    responses.reset()

    # Test with Slack
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    @notify_webhook(mock_webhook_url, "test_function", platform="slack", user_id="user123")
    def failing_slack_function():
        raise ValueError("Test error")

    with pytest.raises(ValueError):
        failing_slack_function()

    assert "<@user123>" in responses.calls[1].request.body.decode()


def test_notify_webhook_invalid_platform():
    with pytest.raises(ValueError, match=r"Unsupported platform 'invalid'"):

        @notify_webhook("https://example.com", "test", platform="invalid")
        def test_func():
            pass


# Prefect Webhook Tests
@responses.activate
def test_send_prefect_notification_success(mock_webhook_url):
    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Test function
    send_prefect_notification(mock_webhook_url, "Test message")

    # Verify
    assert len(responses.calls) == 1
    assert responses.calls[0].request.body == b'{"text": "Test message"}'


@responses.activate
def test_send_prefect_notification_error_swallowed(mock_webhook_url, capsys):
    # Setup response to fail
    responses.add(responses.POST, mock_webhook_url, json={"error": "Internal server error"}, status=500)

    # Test function - should not raise exception
    send_prefect_notification(mock_webhook_url, "Test message")

    # Verify error was logged but not raised
    captured = capsys.readouterr()
    assert "Failed to send Prefect webhook notification" in captured.out


@responses.activate
def test_send_prefect_notification_discord(mock_webhook_url):
    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Test function with discord platform
    send_prefect_notification(mock_webhook_url, "Test message", "discord")

    # Verify discord payload format
    assert len(responses.calls) == 1
    assert responses.calls[0].request.body == b'{"content": "Test message"}'


@responses.activate
def test_send_start_notification_with_timing(mock_webhook_url):
    from datetime import datetime

    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Test function
    start_time = datetime(2025, 1, 15, 10, 30, 0)
    send_start_notification(mock_webhook_url, "Test Flow", start_time)

    # Verify message includes timing
    import json

    body = json.loads(responses.calls[0].request.body.decode())
    assert "Automation has started" in body["text"]
    assert "Start Time: 2025-01-15 10:30:00" in body["text"]
    assert "Function Caller: Test Flow" in body["text"]


@responses.activate
def test_send_start_notification_custom_message(mock_webhook_url):
    from datetime import datetime

    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Test function with custom message
    start_time = datetime(2025, 1, 15, 10, 30, 0)
    send_start_notification(mock_webhook_url, "Test Flow", start_time, start_message="Custom start!")

    # Verify custom message
    import json

    body = json.loads(responses.calls[0].request.body.decode())
    assert body["text"] == "Custom start!"


@pytest.mark.skipif(not PREFECT_AVAILABLE, reason="Prefect not available")
def test_create_state_hooks():
    # Test creating state hooks
    start_time_holder = {}
    hooks = create_state_hooks("http://example.com", "Test Flow", "user", True, start_time_holder)

    # Verify structure
    assert "on_completion" in hooks
    assert "on_failure" in hooks
    assert "on_crashed" in hooks
    assert "on_cancellation" in hooks

    # Verify each hook contains a function
    assert len(hooks["on_completion"]) == 1
    assert callable(hooks["on_completion"][0])


@pytest.mark.skipif(not PREFECT_AVAILABLE, reason="Prefect not available")
@responses.activate
def test_prefect_notify_webhook_basic(mock_webhook_url):
    # Setup response for start notification
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Create decorated function
    @prefect_notify_webhook(mock_webhook_url, "Test Flow")
    def sample_flow():
        return "Flow executed"

    # Execute function
    result = sample_flow()

    # Verify function result
    assert result == "Flow executed"

    # Verify start notification was sent with detailed format
    assert len(responses.calls) == 1
    import json

    start_data = json.loads(responses.calls[0].request.body.decode())
    assert "Automation has started" in start_data["text"]
    assert "Start Time:" in start_data["text"]
    assert "Function Caller: Test Flow" in start_data["text"]


@pytest.mark.skipif(not PREFECT_AVAILABLE, reason="Prefect not available")
@responses.activate
def test_prefect_notify_webhook_custom_messages(mock_webhook_url):
    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Create decorated function with custom messages
    @prefect_notify_webhook(
        mock_webhook_url, "Custom Flow", start_message="Custom start message", success_message="Custom success message"
    )
    def custom_flow():
        return "Done"

    # Execute function
    custom_flow()

    # Verify custom start message was used
    import json

    start_data = json.loads(responses.calls[0].request.body.decode())
    assert start_data["text"] == "Custom start message"


@pytest.mark.skipif(not PREFECT_AVAILABLE, reason="Prefect not available")
@responses.activate
def test_state_hooks_completion(mock_webhook_url):
    from datetime import datetime

    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Create state hooks with start_time_holder
    start_time_holder = {"start_time": datetime(2025, 1, 15, 10, 30, 0)}
    hooks = create_state_hooks(mock_webhook_url, "Test Flow", "testuser", True, start_time_holder)
    completion_hook = hooks["on_completion"][0]

    # Mock flow objects
    mock_flow = MagicMock()
    mock_flow_run = MagicMock()
    mock_state = MagicMock()
    mock_state.result = MagicMock(return_value=None)

    # Call the completion handler
    completion_hook(mock_flow, mock_flow_run, mock_state)

    # Verify success message doesn't mention user (silent_success=True) and has timing
    import json

    success_data = json.loads(responses.calls[0].request.body.decode())
    assert "Automation has completed successfully" in success_data["text"]
    assert "Start Time: 2025-01-15 10:30:00" in success_data["text"]
    assert "End Time:" in success_data["text"]
    assert "Duration:" in success_data["text"]
    assert "Function Caller: Test Flow" in success_data["text"]
    assert "@testuser" not in success_data["text"]


@pytest.mark.skipif(not PREFECT_AVAILABLE, reason="Prefect not available")
@responses.activate
def test_state_hooks_non_silent_success(mock_webhook_url):
    from datetime import datetime

    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Create state hooks with silent_success=False
    start_time_holder = {"start_time": datetime(2025, 1, 15, 10, 30, 0)}
    hooks = create_state_hooks(mock_webhook_url, "Test Flow", "testuser", False, start_time_holder)
    completion_hook = hooks["on_completion"][0]

    # Mock flow objects
    mock_flow = MagicMock()
    mock_flow_run = MagicMock()
    mock_state = MagicMock()
    mock_state.result = MagicMock(return_value=None)

    # Call the completion handler
    completion_hook(mock_flow, mock_flow_run, mock_state)

    # Verify success message mentions user (silent_success=False)
    import json

    success_data = json.loads(responses.calls[0].request.body.decode())
    assert "@testuser" in success_data["text"]
    assert "Automation has completed successfully" in success_data["text"]


@pytest.mark.skipif(not PREFECT_AVAILABLE, reason="Prefect not available")
@responses.activate
def test_state_hooks_failure_notification(mock_webhook_url):
    from datetime import datetime

    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Create state hooks
    start_time_holder = {"start_time": datetime(2025, 1, 15, 10, 30, 0)}
    hooks = create_state_hooks(mock_webhook_url, "Test Flow", "testuser", True, start_time_holder)
    failure_hook = hooks["on_failure"][0]

    # Mock flow objects
    mock_flow = MagicMock()
    mock_flow_run = MagicMock()
    mock_state = MagicMock()
    mock_state.type = "FAILED"  # Mock StateType.FAILED
    mock_state.message = "Something went wrong"

    # Call the failure handler
    failure_hook(mock_flow, mock_flow_run, mock_state)

    # Verify failure message with timing and error
    import json

    failure_data = json.loads(responses.calls[0].request.body.decode())
    assert "@testuser" in failure_data["text"]
    assert "Automation has failed" in failure_data["text"]
    assert "Start Time: 2025-01-15 10:30:00" in failure_data["text"]
    assert "Duration:" in failure_data["text"]
    assert "Error: Something went wrong" in failure_data["text"]


@pytest.mark.skipif(not PREFECT_AVAILABLE, reason="Prefect not available")
@responses.activate
def test_state_hooks_crashed_notification(mock_webhook_url):
    from datetime import datetime

    from prefect.states import StateType

    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Create state hooks
    start_time_holder = {"start_time": datetime(2025, 1, 15, 10, 30, 0)}
    hooks = create_state_hooks(mock_webhook_url, "Test Flow", "testuser", True, start_time_holder)
    failure_hook = hooks["on_crashed"][0]

    # Mock flow objects
    mock_flow = MagicMock()
    mock_flow_run = MagicMock()
    mock_state = MagicMock()
    mock_state.type = StateType.CRASHED
    mock_state.message = "Flow timed out after 300 seconds"

    # Call the crashed handler
    failure_hook(mock_flow, mock_flow_run, mock_state)

    # Verify crash message
    import json

    crash_data = json.loads(responses.calls[0].request.body.decode())
    assert "@testuser" in crash_data["text"]
    assert "Automation has crashed" in crash_data["text"]
    assert "Start Time: 2025-01-15 10:30:00" in crash_data["text"]
    assert "Duration:" in crash_data["text"]
    assert "Error: Flow timed out after 300 seconds" in crash_data["text"]


@pytest.mark.skipif(not PREFECT_AVAILABLE, reason="Prefect not available")
@responses.activate
def test_state_hooks_failure_with_sql_error(mock_webhook_url):
    from datetime import datetime

    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Create state hooks
    start_time_holder = {"start_time": datetime(2025, 1, 15, 10, 30, 0)}
    hooks = create_state_hooks(mock_webhook_url, "Test Flow", "testuser", True, start_time_holder)
    failure_hook = hooks["on_failure"][0]

    # Mock flow objects with SQL error
    mock_flow = MagicMock()
    mock_flow_run = MagicMock()
    mock_state = MagicMock()
    mock_state.type = "FAILED"
    mock_state.message = "Database error [SQL: SELECT * FROM users WHERE id = 123] connection lost"

    # Call the failure handler
    failure_hook(mock_flow, mock_flow_run, mock_state)

    # Verify SQL is stripped
    import json

    failure_data = json.loads(responses.calls[0].request.body.decode())
    assert "SELECT * FROM users" not in failure_data["text"]
    assert "[SQL:" not in failure_data["text"]
    assert "Database error" in failure_data["text"]
    assert "connection lost" in failure_data["text"]


@pytest.mark.skipif(not PREFECT_AVAILABLE, reason="Prefect not available")
@responses.activate
def test_state_hooks_failure_slack_mention(mock_webhook_url):
    from datetime import datetime

    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Create state hooks with slack platform
    start_time_holder = {"start_time": datetime(2025, 1, 15, 10, 30, 0)}
    hooks = create_state_hooks(mock_webhook_url, "Test Flow", "U12345", True, start_time_holder, platform="slack")
    failure_hook = hooks["on_failure"][0]

    # Mock flow objects
    mock_flow = MagicMock()
    mock_flow_run = MagicMock()
    mock_state = MagicMock()
    mock_state.type = "FAILED"
    mock_state.message = "Test error"

    # Call the failure handler
    failure_hook(mock_flow, mock_flow_run, mock_state)

    # Verify Slack mention format
    import json

    failure_data = json.loads(responses.calls[0].request.body.decode())
    assert "<@U12345>" in failure_data["text"]
    assert "@U12345" not in failure_data["text"].replace("<@U12345>", "")


@pytest.mark.skipif(not PREFECT_AVAILABLE, reason="Prefect not available")
@responses.activate
def test_state_hooks_completion_with_result(mock_webhook_url):
    from datetime import datetime

    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Create state hooks
    start_time_holder = {"start_time": datetime(2025, 1, 15, 10, 30, 0)}
    hooks = create_state_hooks(mock_webhook_url, "Test Flow", "testuser", True, start_time_holder)
    completion_hook = hooks["on_completion"][0]

    # Mock flow objects with result
    mock_flow = MagicMock()
    mock_flow_run = MagicMock()
    mock_state = MagicMock()
    mock_state.result = MagicMock(return_value="Task completed with data")

    # Call the completion handler
    completion_hook(mock_flow, mock_flow_run, mock_state)

    # Verify result is included
    import json

    success_data = json.loads(responses.calls[0].request.body.decode())
    assert "Return Message: Task completed with data" in success_data["text"]


@pytest.mark.skipif(not PREFECT_AVAILABLE, reason="Prefect not available")
@responses.activate
def test_state_hooks_cancellation_notification(mock_webhook_url):
    from datetime import datetime

    from prefect.states import StateType

    # Setup response
    responses.add(responses.POST, mock_webhook_url, json={"success": True}, status=200)

    # Create state hooks
    start_time_holder = {"start_time": datetime(2025, 1, 15, 10, 30, 0)}
    hooks = create_state_hooks(mock_webhook_url, "Test Flow", "testuser", True, start_time_holder)
    cancellation_hook = hooks["on_cancellation"][0]

    # Mock flow objects
    mock_flow = MagicMock()
    mock_flow_run = MagicMock()
    mock_state = MagicMock()
    mock_state.type = StateType.CANCELLED
    mock_state.message = "Flow was cancelled by user"

    # Call the cancellation handler
    cancellation_hook(mock_flow, mock_flow_run, mock_state)

    # Verify cancellation message
    import json

    cancel_data = json.loads(responses.calls[0].request.body.decode())
    assert "@testuser" in cancel_data["text"]
    assert "Automation has was cancelled" in cancel_data["text"]
    assert "Error: Flow was cancelled by user" in cancel_data["text"]


@pytest.mark.skipif(PREFECT_AVAILABLE, reason="Prefect is available")
def test_prefect_notify_webhook_prefect_not_available(mock_webhook_url):
    # Test when Prefect is not available
    with pytest.raises(ImportError, match="Prefect is required to use prefect_notify_webhook"):

        @prefect_notify_webhook(mock_webhook_url, "Test Flow")
        def test_flow():
            pass


@pytest.mark.skipif(not PREFECT_AVAILABLE, reason="Prefect not available")
def test_prefect_notify_webhook_import_available():
    # Test that we can import when Prefect is available
    from securehst_webhook_notifier import prefect_notify_webhook

    assert prefect_notify_webhook is not None


@pytest.mark.skipif(not PREFECT_AVAILABLE, reason="Prefect not available")
def test_prefect_notify_webhook_webhook_config_storage(mock_webhook_url):
    # Test that webhook config is stored on wrapper function
    @prefect_notify_webhook(mock_webhook_url, "Test Flow", user_id="testuser", platform="slack")
    def test_flow():
        return "Done"

    # Verify webhook config is stored
    assert hasattr(test_flow, "_webhook_config")
    config = test_flow._webhook_config
    assert config["display_name"] == "Test Flow"
    assert config["user_id"] == "testuser"
    assert config["webhook_url"] == mock_webhook_url
    assert config["platform"] == "slack"
    assert "start_time_holder" in config
