#!/usr/bin/env python
import signal
import time
import ukbutils.utils as utils

if __name__ == "__main__":
    # Set up the signal handler
    signal.signal(signal.SIGTERM, utils.terminate_signal_handler)
    
    print("Mock program started. Waiting for signal...")
    while True:
        time.sleep(1)