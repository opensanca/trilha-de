import argparse
import json
import logging
import string
import petl

def read_file(filename):
    with open(filename) as fp:
        for line in fp.readlines():
            yield json.loads(line)

def tokenize_page(page):
    return page.translate(page.maketrans(string.whitespace,"      ",string.punctuation)).split()

def generate_row(row):
    for word in tokenize_page(row[1]):
        yield [row[2],row[0],word]

def tokenize(table):
    return petl.rowmapmany(table, generate_row, header=['created_at','word'])

def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument("--filename", type=str, help='Input filename', required=True)
    # parser.add_argument("--max_iterations", type=int, help='Max number of requests', default=1000)

    known_args, _ = parser.parse_known_args()

    file_content = [line for line in read_file(known_args.filename)]
    table = petl.fromdicts(file_content)
    tokenized_table = tokenize(table)
    petl.tocsv(tokenized_table, 'words.csv')

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
