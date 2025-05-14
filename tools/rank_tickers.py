# Define the input and output file paths
input_file_path = 'datasets/CRSP/sp500/sp_500_for_CRSP.txt'
output_file_path = 'datasets/CRSP/sp500/sp_500_for_CRSP.txt'

# Read the content of the input file
with open(input_file_path, 'r') as file:
    lines = file.readlines()

# Sort the lines alphabetically
sorted_lines = sorted(lines)

# Write the sorted lines to the output file
with open(output_file_path, 'w') as file:
    file.writelines(sorted_lines)

print(f"Sorted lines have been saved to {output_file_path}")
