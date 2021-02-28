import csv, sys, os

INITIAL_PAGE_SIZE = 512
FINAL_PAGE_SIZE = 8192
INITIAL_BUFFER_SIZE = 5
FINAL_BUFFER_SIZE = 20

print("=== Running Experiment 1===")
try: 
    if len(sys.argv) < 3:
        raise Exception
except Exception:
    print("Too few arguments! Please provide input and output file names: run_tests.py <input_file> <output_file>")
    sys.exit(2)

input_file = sys.argv[1]
output_file = sys.argv[2]

# echo 1 | java QueryMain query7.sql query7BNJ.result 5000 12 | grep "Execution time"

with open('experiment1.csv', mode='w') as expr1_results:
    fieldnames = ["Page size", "Num Buffers", "Time Taken"]
    writer = csv.DictWriter(expr1_results, fieldnames=fieldnames)
    writer.writeheader()

    for page_size in range(INITIAL_PAGE_SIZE, FINAL_PAGE_SIZE + 1, 512):
        for buffer_size in range(INITIAL_BUFFER_SIZE, FINAL_BUFFER_SIZE + 1, 5):
            command = f'echo 1 | java QueryMain {input_file} {output_file} {page_size} {buffer_size} | grep "Execution time"'
            execution_time = os.popen(command).read()
            if execution_time == "":
                execution_time = "-"
            else: 
                execution_time = float(execution_time.split(" ")[3])

            writer.writerow({"Page size": page_size, "Num Buffers": buffer_size, "Time Taken": execution_time})
        

print("=== Experiment 1 Completed===")
