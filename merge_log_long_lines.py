import re
import sys

# Check if the input and output file paths are provided
if len(sys.argv) != 2:
    print("Usage: python merge_log_long_lines.py <input_log_file_path>")
    sys.exit(1)

# Define the log file paths from the command line arguments
log_file_path = sys.argv[1]
output_file_path = sys.argv[1] + "_merged"

# Define the regex pattern to match the timestamp at the beginning of a line
timestamp_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z')

# Read and merge the log file
with open(log_file_path, 'r') as infile, open(output_file_path, 'w') as outfile:
    buffer = ""
    for line in infile:
        if timestamp_pattern.match(line):
            if buffer:
                outfile.write(buffer + '\n')
            buffer = line.strip()
        else:
            buffer += line.strip()
    if buffer:
        outfile.write(buffer + '\n')

print(f"Merged log file saved to {output_file_path}")
