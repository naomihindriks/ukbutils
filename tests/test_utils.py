import pytest
import sys

import ukbutils.utils as utils

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
