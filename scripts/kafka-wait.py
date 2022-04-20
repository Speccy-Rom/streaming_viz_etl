"""Uses kafkacat: https://github.com/edenhill/kcat to see if Kafka is ready"""

import os
import subprocess
import time

RETRY_TIME_SECONDS = os.getenv("RETRY_TIME_SECONDS", 10)
TOTAL_TIME_SECONDS = os.getenv("TOTAL_TIME_SECONDS", 60 * 3)
KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER', "localhost:9092")


def main():
    can_connect = False
    start_time = time.time()

    while time.time() - TOTAL_TIME_SECONDS < start_time:
        process = subprocess.Popen(
            ["kcat", "-b", KAFKA_BOOTSTRAP_SERVER, "-L"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stdout, stderr = process.communicate()
        if "metadata for all topics" not in str(stdout).lower():
            print(str(stderr))
            print(f"Sleeping for {RETRY_TIME_SECONDS} seconds")
            time.sleep(RETRY_TIME_SECONDS)
        else:
            can_connect = True
            break

    if not can_connect:
        exit(1)

    print(str(stdout))
    print("Kafka is up!")


if __name__ == "__main__":
    main()
