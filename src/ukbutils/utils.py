import logging
import sys
import contextlib
import io


def exit_script(log_message, log_function=logging.info, status=0, exit_message=None):
    if status == 0:
        eval("log_function")(log_message)
        sys.exit(0)
    else:
        eval("log_function")(log_message)
        if exit_message:
            sys.exit(exit_message)
        else:
            print(logging.getLoggerClass().root.handlers[0])
            log_file_name = logging.getLoggerClass().root.handlers[0].baseFilename
            sys.exit(
                "An error occured while the script was running,"
                "more information can be found in the log file"
                f"({log_file_name})"
            )


def terminate_signal_handler(signal, frame):
    """
    Signal handler for termination signal.
    """
    exit_script(
        "Process was prematurely terminated",
        log_function=logging.error,
        status=1,
        exit_message=signal,
    )


@contextlib.contextmanager
def capture():
    """
    Context manager for capturing and redirecting standard output and
    standard error streams.

    This context manager captures the output written to the standard output
    (stdout) and standard error (stderr) streams during its execution, and
    provides them as captured strings after exiting the context.

    Usage:
        with capture() as captured_streams:
            print("This will be captured.")
            sys.stderr.write("And so will this.")

        # After exiting the context block:
        captured_stdout = captured_streams["stdout"]
        captured_stderr = captured_streams["stderr"]


    Returns:
        dict: A dictionary containing the captured stdout and stderr streams
              as strings. The dictionary has two keys: "stdout" and "stderr".

    Example:
        import sys

        with capture() as captured_streams:
            print("Hello, captured world!")
            sys.stderr.write("This is an error.")

        print("Captured stdout:", captured_streams["stdout"])
        print("Captured stderr:", captured_streams["stderr"])
    """
    "TEST"
    oldout, olderr = sys.stdout, sys.stderr
    try:
        out = {"stdout": io.StringIO(), "stderr": io.StringIO()}
        sys.stdout, sys.stderr = [out["stdout"], out["stderr"]]
        yield out
    finally:
        sys.stdout, sys.stderr = oldout, olderr
        out["stdout"] = out["stdout"].getvalue()
        out["stderr"] = out["stderr"].getvalue()
