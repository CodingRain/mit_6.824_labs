#!/bin/bash

# Initialize counters
success_count=0
fail_count=0

# Number of times to run the test
num_runs=200

# Array to keep track of background job PIDs
pids=()

for i in $(seq 1 $num_runs)
do
    # Run the test in a background process
    ( 
        output=$(go test -run TestRejoin2B)

        # Check if the test passed or failed
        if [[ $output == *"PASS"* ]]; then
            echo "Test $i: PASS"
        else
            echo "Test $i: FAIL"

            # Record the failed output to a file
            echo "$output" > "fail_log_$i.txt"
        fi
    ) &

    # Store the PID of the background process
    pids+=($!)
done

# Wait for all background jobs to complete
for pid in ${pids[@]}; do
    wait $pid
    # Increment success or fail count based on the exit status
    if [ $? -eq 0 ]; then
        ((success_count++))
    else
        ((fail_count++))
    fi
done

# Print the results
echo "Successes: $success_count"
echo "Failures: $fail_count"
