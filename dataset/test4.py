import matplotlib.pyplot as plt
import numpy as np

# Read data from a .txt file
def read_data(file_path):
    with open(file_path, 'r') as file:
        data = file.readlines()
    return [line.strip() for line in data]

# Sample dataset file path
file_path = "kosarak.txt"

# Parse dataset and count frequency of each webpage
click_stream_data = read_data(file_path)
page_counts = {}
for transaction in click_stream_data:
    pages = transaction.split()
    for page in pages:
        if page in page_counts:
            page_counts[page] += 1
        else:
            page_counts[page] = 1

# Sort frequencies in decreasing order
sorted_frequencies = sorted(page_counts.values(), reverse=True)

# Plot the graph
plt.plot(sorted_frequencies, marker='o')
plt.xlabel('Rank')
plt.ylabel('Frequency')
plt.title('Frequency Distribution of Web-pages')
plt.grid(True)

plt.show()

