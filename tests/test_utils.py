import pytest
import sys
import logging
import shutil
import tempfile
import os
import time
import subprocess
import signal

import ukbutils.utils as utils


@pytest.fixture
def mock_exit_script(monkeypatch):
    def mock_exit_script_func(message, log_function=logging.error, status=1, exit_message=None):
        mock_exit_script_func.called = True
        mock_exit_script_func.message = message
        mock_exit_script_func.log_function = log_function
        mock_exit_script_func.status = status
        mock_exit_script_func.exit_message = exit_message

    mock_exit_script_func.called = False
    return mock_exit_script_func
    

def test_exit_script_success(monkeypatch, caplog):
    # With monkeypatch change behaviour of sys.exit
    monkeypatch.setattr(sys, 'exit', lambda x: None)
    with caplog.at_level(logging.INFO):
        utils.exit_script("Success message")
        assert "Success message" in caplog.text


def test_exit_script_error(caplog, monkeypatch):
    # Initialize a variable to store the exit message
    exit_message = None

    # Mocking sys.exit to capture exit status and exit message
    def mock_exit(x):
        nonlocal exit_message
        exit_message = x

    # Mocking sys.exit
    monkeypatch.setattr(sys, 'exit', mock_exit)

    # Triggering exit_script with an error message and exit message
    with caplog.at_level(logging.ERROR):
        utils.exit_script("Error message", log_function=logging.error, status=1, exit_message="Exit message")
        

    # Asserting that the log message is captured
    assert "Error message" in caplog.text

    # Asserting that the exit message is set to "Exit message"
    assert exit_message == "Exit message"


def test_signal_handling(mock_exit_script, monkeypatch):
    # Patch the exit_script function with the mock
    monkeypatch.setattr(utils, 'exit_script', mock_exit_script)

    signal.signal(signal.SIGTERM, utils.terminate_signal_handler)
    
    # Send the SIGTERM signal to the subprocess
    os.kill(os.getpid(), signal.SIGTERM)

    # Set SIGTERM signal back to default
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    # Verify that exit_script was called with the correct parameters
    assert mock_exit_script.called
    assert mock_exit_script.status == 1
    assert mock_exit_script.log_function == logging.error
    assert mock_exit_script.message == "Process was prematurely terminated"
    assert mock_exit_script.exit_message == signal.SIGTERM

    
    



def test_capture_output():
    with utils.capture() as captured_streams:
        print("Hello, captured world!")
        sys.stderr.write("This is an error.")

    captured_stdout, captured_stderr = captured_streams["stdout"], captured_streams["stderr"]

    # Check if the expected output is captured
    assert captured_stdout == "Hello, captured world!\n"
    assert captured_stderr == "This is an error."

    
def test_capture_does_not_affect_prior_input_stream(capfd):
    stout_msg = "hello"
    stderr_msg = "world"
    
    print(stout_msg)
    sys.stderr.write(stderr_msg)

    out, err = capfd.readouterr()

    with utils.capture() as captured_streams:
        print("Hello, captured world!")
        sys.stderr.write("This is an error.")
        
    # Check if the captured output is also available via capfd
    assert stout_msg == out.strip()
    assert stderr_msg == err.strip()


def test_capture_does_not_affect_subsequent_input_stream(capfd):
    stout_msg = "hello"
    stderr_msg = "world"

    with utils.capture() as captured_streams:
        print("Hello, captured world!")
        sys.stderr.write("This is an error.")

    print(stout_msg)
    sys.stderr.write(stderr_msg)
    
    out, err = capfd.readouterr()
        
    # Check if the captured output is also available via capfd
    assert stout_msg == out.strip()
    assert stderr_msg == err.strip()


def test_capture_does_not_remove_unread_stream(capfd):
    stout_msg = "hello"
    stderr_msg = "world"

    print(stout_msg)
    sys.stderr.write(stderr_msg)
    
    with utils.capture() as captured_streams:
        print("Hello, captured world!")
        sys.stderr.write("This is an error.")
    
    out, err = capfd.readouterr()
        
    # Check if the captured output is also available via capfd
    assert out.strip() == stout_msg
    assert err.strip() == stderr_msg


def test_capture_capture_empty_output():
    with utils.capture() as captured_streams:
        # No output in this block
        pass
        
    captured_stdout, captured_stderr = captured_streams["stdout"], captured_streams["stderr"]

    assert captured_stdout == ""
    assert captured_stderr == ""


def test_capture_unicode_characters():
    with utils.capture() as captured_streams:
        print("Hello, captured world! 🌍")
        sys.stderr.write("This is an error with Unicode 😊.")

    captured_stdout, captured_stderr = captured_streams["stdout"], captured_streams["stderr"]

    assert captured_stdout.strip() == "Hello, captured world! \U0001F30D"
    assert captured_stderr.strip() == "This is an error with Unicode \U0001F60A."


def test_capture_large_output():
    with utils.capture() as captured_streams:
        for _ in range(10000):
            print("This is a line.")

    captured_stdout, captured_stderr = captured_streams["stdout"], captured_streams["stderr"]

    assert captured_stdout.count("This is a line.") == 10000
    assert captured_stderr == ""


def test_nested_captures():
    with utils.capture() as outer_capture:
        print("Outer capture stdout.")
        sys.stderr.write("Outer error.")
        
        with utils.capture() as inner_capture:
            print("Inner capture stdout.")
            sys.stderr.write("Inner error.")

    outer_captured_stdout = outer_capture["stdout"]
    outer_captured_stderr = outer_capture["stderr"]
    inner_captured_stdout = inner_capture["stdout"]
    inner_captured_stderr = inner_capture["stderr"]

    assert outer_captured_stdout.strip() == "Outer capture stdout."
    assert outer_captured_stderr.strip() == "Outer error."
    assert inner_captured_stdout.strip() == "Inner capture stdout."
    assert inner_captured_stderr.strip() == "Inner error."


def test_capture_stdout_before_exception(capsys):
    try:
        with utils.capture() as captured_streams:
            print("Inside capture.")
            raise ValueError("An exception occurred.")
    except ValueError as e:
        pass
       
    captured_stdout, captured_stderr = captured_streams["stdout"], captured_streams["stderr"]
    
    assert captured_stdout.strip() == "Inside capture."


def test_capture_multiple_times():
    with utils.capture() as first_capture:
        print("First capture.")

    with utils.capture() as second_capture:
        print("Second capture.")

    first_captured_stdout = first_capture["stdout"]
    second_captured_stdout = second_capture["stdout"]

    assert first_captured_stdout.strip() == "First capture."
    assert second_captured_stdout.strip() == "Second capture."
