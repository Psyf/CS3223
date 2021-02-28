import csv, sys, os

INITIAL_PAGE_SIZE = 512
FINAL_PAGE_SIZE = 8192
INITIAL_BUFFER_SIZE = 5
FINAL_BUFFER_SIZE = 20

print("=== Running Experiment 1===")
try: 
    if len(sys.argv) < 4:
        raise Exception
except Exception:
    print("Too few arguments! Please provide input and output file names: run_tests.py <input_file> <output_file> <csv_file_name>")
    sys.exit(2)

input_file = sys.argv[1]
output_file = sys.argv[2]
csv_file_name = sys.argv[3]

# echo 1 | java QueryMain query7.sql query7BNJ.result 5000 12 | grep "Execution time"

with open(f'{csv_file_name}.csv', mode='w') as expr1_results:
    fieldnames = ["Page size", "Num Buffers", "Time Taken", "Query Plan"]
    writer = csv.DictWriter(expr1_results, fieldnames=fieldnames)
    writer.writeheader()

    for page_size in range(INITIAL_PAGE_SIZE, FINAL_PAGE_SIZE + 1, 512):
        for buffer_size in range(INITIAL_BUFFER_SIZE, FINAL_BUFFER_SIZE + 1, 5):
            command = f'echo 1 | java QueryMain {input_file} {output_file} {page_size} {buffer_size} | grep -A 1 -e "Execution Plan" -e "Execution time"'
            output_logs = os.popen(command).read()
            print(output_logs)
            query_plan, execution_time = output_logs.split("\n")[1], output_logs.split("\n")[3]
            if execution_time == "":
                execution_time = "-"
            else: 
                execution_time = float(execution_time.split(" ")[3])

            writer.writerow({"Page size": page_size, "Num Buffers": buffer_size, "Time Taken": execution_time, "Query Plan": query_plan})

# command = f'echo 1 | java QueryMain {input_file} {output_file} 5000 12 | grep -A 1 -e "Execution Plan" -e "Execution time"'
# output_logs = os.popen(command).read()
# query_plan, execution_time = output_logs.split("\n")[1], output_logs.split("\n")[3]
# print(query_plan, execution_time)

print("=== Experiment 1 Completed===")
