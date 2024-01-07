
num_runs=10

for i in $(seq 1 $num_runs)
do
    go test -run=2C
done
