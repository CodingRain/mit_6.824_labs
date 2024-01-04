#!/bin/bash

# Loop until the test fails
while true; do
    # Execute the test and capture the output
    output=$(go test -run=TestFigure8Unreliable2C)

    # Check if the output contains the word "FAIL"
    if [[ $output == *"FAIL"* ]]; then
        # If test fails, print the output to result.txt and exit the loop
        echo "$output" > fail.txt
        break
    fi
done

