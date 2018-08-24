import json

# This code takes in a JSON file which has an array of objects and convert to
# a flat file where each line is a JSON object
# That way it is far easier to do yield reading.

# Run dos2unix on the originally exported JSON file
with open("<absolute path of master json file here>") as content:
    arr_of_items = json.load(content)
print(len(arr_of_items))
with open("<absolute path of massaged dataset","w") as output_file:
    for item in arr_of_items:
        output_file.write(str(item) + "\n")
