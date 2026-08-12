<?php

/*
 * worker.php
 *
 * Runs inside each GitHub Actions Linux runner.
 */


// ============================================================
// CONFIGURATION
// ============================================================

const SERVER_URL =
    'https://34.63.222.47/connectWorker67.php';

const POLL_DELAY = 2;


// ============================================================
// HTTP REQUEST
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


    $data = json_decode(
        $response,
        true
    );


    if (!is_array($data)) {

        throw new Exception(
            'Server returned invalid JSON'
        );
    }


    if ($httpCode >= 400) {

        throw new Exception(
            'Server returned HTTP ' .
            $httpCode .
            ': ' .
            ($data['error'] ?? 'unknown error')
        );
    }


    return $data;
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


echo "Registered successfully.\n";

echo "Worker ID: " .
    $workerId .
    "\n";


// ============================================================
// WORK LOOP
// ============================================================

echo "Worker is now waiting for tasks...\n";


while (true) {

    try {

        /*
         * Ask the VPS for a task.
         */

        $response = requestServer([

            'action' => 'poll',

            'worker_id' => $workerId,

            'worker_token' => $workerToken
        ]);


        /*
         * Check whether we received a task.
         */

        $task = $response['task'] ?? null;


        if ($task === null) {

            sleep(POLL_DELAY);

            continue;
        }


        echo "Received task.\n";


        $taskId =
            $task['id'] ?? null;


        if ($taskId === null) {

            echo "Received malformed task.\n";

            continue;
        }


        // ====================================================
        // TASK PROCESSING
        // ====================================================

        /*
         * IMPORTANT:
         *
         * We deliberately don't execute arbitrary shell
         * commands received from the server here.
         *
         * Add your own permitted task types below.
         */

        $type =
            $task['type'] ?? 'unknown';


        $result = null;


        if ($type === 'example') {

            /*
             * Example task.
             */

            $value =
                $task['data']['value'] ?? null;


            $result = [
                'type' => 'example',

                'input' => $value,

                'output' => $value
            ];
        }


        else {

            $result = [
                'error' =>
                    'Unknown task type'
            ];
        }


        // ====================================================
        // SEND RESULT
        // ====================================================

        requestServer([

            'action' => 'result',

            'worker_id' => $workerId,

            'worker_token' => $workerToken,

            'task_id' => $taskId,

            'result' => $result
        ]);


        echo "Task completed: " .
            $taskId .
            "\n";


    } catch (Throwable $e) {

        /*
         * Don't immediately kill the worker if the VPS
         * temporarily becomes unavailable.
         */

        echo "Worker error: " .
            $e->getMessage() .
            "\n";

        echo "Retrying...\n";

        sleep(5);
    }
}
