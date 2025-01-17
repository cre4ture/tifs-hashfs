import re
import matplotlib.pyplot as plt
import sys

# Check if the input file path is provided
if len(sys.argv) != 2:
    print("Usage: python log_visualizer.py <log_file_path>")
    sys.exit(1)

# Define the log file path from the command line argument
log_file_path = sys.argv[1]

# Define the regex pattern to match the log entries
pattern = re.compile(r'AutoStopWatch:([^:]+\(ms\))\d+:\[(\d+\.\d+)->(\d+\.\d+)\]')

# Initialize lists to store the parsed data
names = []
start_times = []
stop_times = []

# Read and parse the log file
with open(log_file_path, 'r') as file:
    for line in file:
        match = pattern.search(line)
        if match:
            names.append(match.group(1))
            start_times.append(float(match.group(2)))
            stop_times.append(float(match.group(3)))

# Plot the data
plt.figure(figsize=(10, 6))
plt.scatter(start_times, names, color='blue', label='Start Time')
plt.scatter(stop_times, names, color='red', label='Stop Time')
plt.xlabel('Time (ms)')
plt.ylabel('Operation')
plt.title('Operation Start and Stop Times')
plt.legend()
plt.grid(True)
plt.show()
