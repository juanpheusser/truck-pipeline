import os

for file in os.listdir():
    if file.endswith('.csv'):
        os.remove(file)