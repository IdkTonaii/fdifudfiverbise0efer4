import requests
import threading
import time
import traceback


# ============================================================
# CONFIGURATION
# ============================================================

SERVER_URL = "https://YOUR-DOMAIN.COM/connectWorker67.php"

POLL_INTERVAL = 5
HEARTBEAT_INTERVAL = 30
REQUEST_TIMEOUT = 15


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
                        data.get("error", "Unknown error")
                    )

                    time.sleep(5)
                    continue


                self.worker_id = data["worker_id"]
                self.worker_token = data["worker_token"]


                print()
                print("Worker registered")
                print("Worker ID:", self.worker_id)


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


            time.sleep(5)


        return False


    # ========================================================
    # WORKER REQUEST
    # ========================================================

    def request(self, data):

        data["worker_id"] = self.worker_id
        data["worker_token"] = self.worker_token


        try:

            response = self.session.post(
                SERVER_URL,
                json=data,
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

            self.worker_id = None
            self.worker_token = None

            return None


        try:

            return response.json()

        except ValueError:

            print(
                "Server returned invalid JSON."
            )

            return None


    # ========================================================
    # HEARTBEAT
    # ========================================================

    def heartbeat(self):

        if not self.worker_id or not self.worker_token:
            return False


        data = self.request({
            "action": "heartbeat"
        })


        if data is None:
            return False


        if not data.get("ok"):

            print(
                "Heartbeat failed:",
                data.get("error", "Unknown error")
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


            self.heartbeat()


    # ========================================================
    # POLL FOR JOB
    # ========================================================

    def poll(self):

        data = self.request({
            "action": "poll"
        })


        if data is None:
            return None


        if not data.get("ok"):

            print(
                "Poll failed:",
                data.get("error", "Unknown error")
            )

            return None


        task = data.get("task")


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

            "action": "progress",

            "task_id": task_id,

            "progress": progress,

            "message": message
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

            "action": "result",

            "task_id": task_id,

            "result": result
        })


        if data is None:
            return False


        if not data.get("ok"):

            print(
                "Result submission failed:",
                data.get("error", "Unknown error")
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


        # ----------------------------------------------------
        # PUT YOUR JOB 1 CODE HERE
        # ----------------------------------------------------

        self.progress(
            task_id,
            10,
            "Starting job 1"
        )


        # Example work:

        for i in range(10):

            time.sleep(1)

            percent = (
                (i + 1) *
                10
            )


            self.progress(
                task_id,
                percent,
                f"Job 1: {percent}%"
            )


        # ----------------------------------------------------
        # Return whatever you want saved as the result.
        # ----------------------------------------------------

        return {

            "success": True,

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

                    "success": False,

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

            print(e)


            traceback.print_exc()


            # -----------------------------------------------
            # Tell the server that the job failed.
            # -----------------------------------------------

            self.send_result(

                task_id,

                {

                    "success": False,

                    "error":
                        str(e)
                }
            )


    # ========================================================
    # MAIN LOOP
    # ========================================================

    def run(self):

        print(
            "Starting worker..."
        )


        # ----------------------------------------------------
        # Register
        # ----------------------------------------------------

        if not self.register():

            return


        # ----------------------------------------------------
        # Start heartbeat thread
        # ----------------------------------------------------

        self.heartbeat_thread.start()


        print(
            "Worker is waiting for jobs..."
        )


        # ----------------------------------------------------
        # Poll forever
        # ----------------------------------------------------

        while self.running:

            try:

                task = self.poll()


                # ------------------------------------------------
                # Authentication failure.
                #
                # Re-register and continue.
                # ------------------------------------------------

                if (
                    self.worker_id is None or
                    self.worker_token is None
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

                time.sleep(5)


        print(
            "Worker stopped."
        )


# ============================================================
# START WORKER
# ============================================================

if __name__ == "__main__":

    worker = Worker()

    worker.run()
