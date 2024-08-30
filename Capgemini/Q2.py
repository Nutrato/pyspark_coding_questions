# Initialize the list with duplicate elements
L = [1, 2, 3, 4, 5, 1, 2, 3, 4]

# Initialize an empty list to store unique elements
unique_list = []

# Iterate over each item in the list 'L'
for item in L:
    # Check if the item is not already in the 'unique_list'
    if item not in unique_list:
        # If the item is not in 'unique_list', append it to 'unique_list'
        unique_list.append(item)

# Print the list of unique elements
print(unique_list)
