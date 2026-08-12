<?php

/*
 * ============================================================
 * WORKER
 * ============================================================
 *
 * This program:
 *
 * 1. Registers with the server
 * 2. Receives a unique worker ID/token
 * 3. Continuously polls for tasks
 * 4. Executes locally-defined jobs
 * 5. Reports progress
 * 6. Reports completion/failure
 * 7. Returns to waiting for another task
 *
 * The server sends JOB IDs, NOT PHP CODE.
 *
 * ============================================================
 */


// ============================================================
// CONFIGURATION
// ============================================================

const SERVER_URL =
    'http://34.63.222.47/connectWorker67.php';

const POLL_DELAY =
    2;

const ERROR_RETRY_DELAY =
    5;


// ============================================================
// SERVER REQUEST
// ============================================================

function requestServer(
    array $data
): array {

    $ch =
        curl_init(
            SERVER_URL
        );


    curl_setopt_array(

        $ch,

        [

            CURLOPT_POST =>
                true,

            CURLOPT_POSTFIELDS =>
                json_encode(
                    $data
                ),

            CURLOPT_HTTPHEADER => [

                'Content-Type: application/json',

                'Accept: application/json'
            ],

            CURLOPT_RETURNTRANSFER =>
                true,

            CURLOPT_CONNECTTIMEOUT =>
                10,

            CURLOPT_TIMEOUT =>
                30
        ]
    );


    $response =
        curl_exec($ch);


    if ($response === false) {

        $error =
            curl_error($ch);

        curl_close($ch);

        throw new Exception(
            'Connection failed: ' .
            $error
        );
    }


    $httpCode =
        curl_getinfo(
            $ch,
            CURLINFO_HTTP_CODE
        );


    curl_close($ch);


    $decoded =
        json_decode(
            $response,
            true
        );


    if (
        !is_array($decoded)
    ) {

        throw new Exception(
            'Server returned invalid JSON'
        );
    }


    if (
        $httpCode >= 400
    ) {

        throw new Exception(

            'Server returned HTTP ' .
            $httpCode .
            ': ' .
            (
                $decoded['error']
                ?? 'unknown error'
            )
        );
    }


    if (
        isset($decoded['ok']) &&
        !$decoded['ok']
    ) {

        throw new Exception(

            $decoded['error']
            ?? 'Server rejected request'
        );
    }


    return $decoded;
}


// ============================================================
// PROGRESS REPORTER
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


    if (
        $progress !== null
    ) {

        $data['progress'] =
            max(
                0,
                min(
                    100,
                    $progress
                )
            );
    }


    if (
        $message !== null
    ) {

        $data['message'] =
            $message;
    }


    requestServer($data);
}


// ============================================================
// JOB 1
// ============================================================

function job1(): array
{
    $url = 'http://34.63.222.47/iptest.php';

    $ch = curl_init($url);

    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => 15,
        CURLOPT_CONNECTTIMEOUT => 10,
    ]);

    $body = curl_exec($ch);

    $error = curl_error($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);

    curl_close($ch);

    return [
        'success' => $body !== false && $httpCode >= 200 && $httpCode < 300,
        'http_code' => $httpCode,
        'error' => $error ?: null
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
     * Create a callback that job1()
     * can use to report progress.
     */

    $report = function (

        ?float $progress = null,

        ?string $message = null

    ) use (

        $workerId,

        $workerToken,

        $task

    ): void {

        workerProgress(

            $workerId,

            $workerToken,

            $task['task_id'],

            'running',

            $progress,

            $message
        );
    };


    /*
     * Map job IDs to local functions.
     */

    switch ($jobId) {

        case 1:

            return job1(

                $task,

                $report
            );


        default:

            throw new Exception(

                'Unknown job ID: ' .
                $jobId
            );
    }
}


// ============================================================
// REGISTER
// ============================================================

echo
    "Registering worker...\n";


$registration =
    requestServer([

        'action' =>
            'register'
    ]);


if (
    empty($registration['ok']) ||
    empty($registration['worker_id']) ||
    empty($registration['worker_token'])
) {

    throw new Exception(
        'Worker registration failed'
    );
}


$workerId =
    $registration['worker_id'];

$workerToken =
    $registration['worker_token'];


echo
    "Worker registered.\n";


echo
    "Worker ID: " .
    $workerId .
    "\n";


echo
    "Waiting for tasks...\n";


// ============================================================
// MAIN LOOP
// ============================================================

while (true) {

    try {

        /*
         * Ask the server for work.
         */

        $response =
            requestServer([

                'action' =>
                    'poll',

                'worker_id' =>
                    $workerId,

                'worker_token' =>
                    $workerToken
            ]);


        $task =
            $response['task']
            ?? null;


        /*
         * No task.
         */

        if (
            $task === null
        ) {

            sleep(
                POLL_DELAY
            );

            continue;
        }


        /*
         * Validate task.
         */

        if (
            empty($task['task_id']) ||
            !isset($task['job_id'])
        ) {

            echo
                "Received invalid task.\n";

            sleep(1);

            continue;
        }


        $taskId =
            (string)$task['task_id'];

        $jobId =
            (int)$task['job_id'];


        echo
            "Received task " .
            $taskId .
            " (job " .
            $jobId .
            ")\n";


        // ====================================================
        // STARTING STATUS
        // ====================================================

        workerProgress(

            $workerId,

            $workerToken,

            $taskId,

            'starting',

            0,

            'Starting job'
        );


        $startedAt =
            microtime(true);


        // ====================================================
        // EXECUTE
        // ====================================================

        try {

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


            // ================================================
            // SEND SUCCESS RESULT
            // ================================================

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
             * The job itself failed.
             *
             * Report that failure to the server,
             * but keep the worker alive.
             */

            echo
                "Task " .
                $taskId .
                " failed: " .
                $e->getMessage() .
                "\n";


            try {

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

            }

            catch (Throwable $reportError) {

                echo
                    "Could not report failure: " .
                    $reportError->getMessage() .
                    "\n";
            }
        }


    }


    catch (Throwable $e) {

        /*
         * Communication failure.
         *
         * Do NOT terminate the worker.
         */

        echo
            "Worker communication error: " .
            $e->getMessage() .
            "\n";


        sleep(
            ERROR_RETRY_DELAY
        );
    }
}
