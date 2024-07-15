#!/bin/bash

# clear
clear

if [ $# -lt 1 ]; then
        echo "Wrong usage WorkersNum."
        exit
fi

# Set the number of iterations for the loop
num_iterations=$1

# print
echo "Launching $num_iterations workers."

# Define the command you want to run in the background
command_to_run="go run worker.go -MasterAddr 127.0.0.1"

# Loop through the specified number of iterations
for ((i=1; i<=$num_iterations; i++)); do
    # Run the command in the background using the '&' operator
    # $command_to_run &>"output/output_$i.out" &
    $command_to_run &>"/dev/null" &

    usleep 100000
done

echo "Done."

wait

