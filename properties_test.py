# Use a json properties file and git ignore to allow sensitive information 
# (e.g. file paths which contain usernames) to be included


import json

json_data = open('properties.json')
print(json_data)
d = json.load(json_data)
# json_data.close()
print(d)
# print(d["name"])
json_data.close()