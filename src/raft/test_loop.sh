#!/bin/bash

# Initialize counters
success_count=0
fail_count=0

# Number of times to run the test
num_runs=100

for i in $(seq 1 $num_runs)
do
    # Run the test and capture the output
    output=$(go test -run=TestUnreliableAgree2C)

    # Check if the test passed or failed
    if [[ $output == *"PASS"* ]]; then
        ((success_count++))
        echo "Test $i: PASS"
    else
        ((fail_count++))
        echo "Test $i: FAIL"

        # Record the failed output to a file
        echo "$output" > "fail_log_$i.txt"
    fi
done

# Print the results
echo "Successes: $success_count"
echo "Failures: $fail_count"
