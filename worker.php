<?php

/*
 * worker.php
 *
 * Runs on each GitHub Actions Linux runner.
 *
 * The server sends a JOB ID.
 *
 * The worker chooses which local function
 * corresponds to that job ID.
 *
 * The server NEVER sends PHP code to execute.
 */


// ============================================================
// CONFIGURATION
// ============================================================

const SERVER_URL =
    'http://34.63.222.47/connectWorker67.php';

const POLL_DELAY = 2;


// ============================================================
// SERVER REQUEST
// ============================================================

function requestServer(array $data): array
{
    $ch = curl_init(SERVER_URL);


    curl_setopt_array($ch, [

        CURLOPT_POST => true,

        CURLOPT_POSTFIELDS =>
            json_encode($data),

        CURLOPT_HTTPHEADER => [
            'Content-Type: application/json'
        ],

        CURLOPT_RETURNTRANSFER => true,

        CURLOPT_CONNECTTIMEOUT => 10,

        CURLOPT_TIMEOUT => 30
    ]);


    $response = curl_exec($ch);


    if ($response === false) {

        $error = curl_error($ch);

        curl_close($ch);

        throw new Exception(
            'Connection failed: ' . $error
        );
    }


    $httpCode =
        curl_getinfo(
            $ch,
            CURLINFO_HTTP_CODE
        );


    curl_close($ch);


    $decoded = json_decode(
        $response,
        true
    );


    if (!is_array($decoded)) {

        throw new Exception(
            'Server returned invalid JSON'
        );
    }


    if ($httpCode >= 400) {

        throw new Exception(
            'Server returned HTTP ' .
            $httpCode .
            ': ' .
            ($decoded['error'] ?? 'unknown error')
        );
    }


    return $decoded;
}


// ============================================================
// WORKER PROGRESS / HEARTBEAT
// ============================================================

function workerProgress(
    string $workerId,
    string $workerToken,
    string $taskId,
    string $status,
    ?float $progress = null,
    ?string $message = null
): void {

    $data = [

        'action' =>
            'progress',

        'worker_id' =>
            $workerId,

        'worker_token' =>
            $workerToken,

        'task_id' =>
            $taskId,

        'status' =>
            $status
    ];


    if ($progress !== null) {

        $data['progress'] =
            max(0, min(100, $progress));
    }


    if ($message !== null) {

        $data['message'] =
            $message;
    }


    requestServer($data);
}


// ============================================================
// JOB 1
// ============================================================

function job1(
    array $task,
    callable $report
): mixed {

    /*
     * ========================================================
     *
     * PUT YOUR ACTUAL CODE HERE.
     *
     * This function is what the worker executes when:
     *
     *     job_id = 1
     *
     * You can use:
     *
     *     $task
     *
     * to access information associated with the job.
     *
     *
     * To report progress:
     *
     *     $report(
     *         25,
     *         '25 percent complete'
     *     );
     *
     *     $report(
     *         50,
     *         'Halfway done'
     *     );
     *
     *     $report(
     *         100,
     *         'Finished'
     *     );
     *
     * ========================================================
     */


    // ========================================================
    // YOUR CODE STARTS HERE
    // ========================================================



    /*
     * Example only:
     *
     * $report(25, 'Started');
     *
     * // YOUR ACTUAL WORK
     *
     * $report(50, 'Halfway finished');
     *
     * // MORE WORK
     *
     * $report(100, 'Complete');
     *
     * return [
     *     'message' => 'Hello from job 1'
     * ];
     */


    // ========================================================
    // YOUR CODE ENDS HERE
    // ========================================================


    return [
        'message' =>
            'Job 1 has no implementation yet.'
    ];
}


// ============================================================
// JOB DISPATCHER
// ============================================================

function executeJob(
    int $jobId,
    array $task,
    string $workerId,
    string $workerToken
): mixed {

    /*
     * This callback lets job1() report progress.
     */

    $report = function (
        ?float $progress = null,
        ?string $message = null
    ) use (
        $workerId,
        $workerToken,
        $task
    ) {

        workerProgress(

            $workerId,

            $workerToken,

            $task['task_id'],

            'running',

            $progress,

            $message
        );
    };


    switch ($jobId) {

        case 1:

            return job1(
                $task,
                $report
            );


        default:

            throw new Exception(
                'Unknown job ID: ' . $jobId
            );
    }
}


// ============================================================
// REGISTER
// ============================================================

echo "Registering worker...\n";


$registration = requestServer([
    'action' => 'register'
]);


if (
    empty($registration['ok']) ||
    empty($registration['worker_id']) ||
    empty($registration['worker_token'])
) {

    throw new Exception(
        'Registration failed'
    );
}


$workerId =
    $registration['worker_id'];

$workerToken =
    $registration['worker_token'];


echo "Worker registered.\n";

echo "Worker ID: " .
    $workerId .
    "\n";


// ============================================================
// MAIN WORKER LOOP
// ============================================================

echo "Waiting for jobs...\n";


while (true) {

    try {

        /*
         * Ask the VPS for work.
         */

        $response = requestServer([

            'action' =>
                'poll',

            'worker_id' =>
                $workerId,

            'worker_token' =>
                $workerToken
        ]);


        $task =
            $response['task'] ?? null;


        /*
         * Nothing to do.
         */

        if ($task === null) {

            sleep(POLL_DELAY);

            continue;
        }


        $taskId =
            $task['task_id'] ?? null;


        $jobId =
            isset($task['job_id'])
                ? (int)$task['job_id']
                : 0;


        if ($taskId === null) {

            echo "Received invalid task.\n";

            continue;
        }


        echo
            "Received task " .
            $taskId .
            " (job " .
            $jobId .
            ")\n";


        // ====================================================
        // TELL SERVER WE STARTED
        // ====================================================

        workerProgress(

            $workerId,

            $workerToken,

            $taskId,

            'starting',

            0,

            'Starting job'
        );


        $startedAt = microtime(true);


        try {

            /*
             * Execute the predefined function.
             */

            $result =
                executeJob(

                    $jobId,

                    $task,

                    $workerId,

                    $workerToken
                );


            $duration =
                microtime(true) -
                $startedAt;


            /*
             * Submit final result.
             */

            requestServer([

                'action' =>
                    'result',

                'worker_id' =>
                    $workerId,

                'worker_token' =>
                    $workerToken,

                'task_id' =>
                    $taskId,

                'result' => [

                    'success' =>
                        true,

                    'duration_seconds' =>
                        $duration,

                    'data' =>
                        $result
                ]
            ]);


            echo
                "Task " .
                $taskId .
                " completed.\n";
        }


        catch (Throwable $e) {

            /*
             * Report failure to the server.
             */

            requestServer([

                'action' =>
                    'result',

                'worker_id' =>
                    $workerId,

                'worker_token' =>
                    $workerToken,

                'task_id' =>
                    $taskId,

                'result' => [

                    'success' =>
                        false,

                    'error' =>
                        $e->getMessage()
                ]
            ]);


            echo
                "Task " .
                $taskId .
                " failed: " .
                $e->getMessage() .
                "\n";
        }


    }


    catch (Throwable $e) {

        /*
         * Network failure, server failure, etc.
         *
         * Don't kill the worker.
         */

        echo
            "Worker communication error: " .
            $e->getMessage() .
            "\n";

        sleep(5);
    }
}
