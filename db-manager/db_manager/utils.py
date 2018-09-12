import csv
def read_csv(csv_path):
    "assumes a csv with headers. returns an iterable"
    with open(csv_path, 'r') as csv_file:
        return csv.DictReader(csv_file) # headers are automatically handled

def insert_or_update():
    pass

def doseq(fn, lst):
    "executes fn(x) for each x in lst. results do not accumulate in memory. returns None"
    for x in lst:
        fn(x)
