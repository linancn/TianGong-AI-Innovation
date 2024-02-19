import csv
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def read_csv(filename, num_rows=None):
    with open(filename, "r") as file:
        reader = csv.reader(file)
        if num_rows is None:
            logging.info("Reading all rows from the file")
            return list(reader)
        else:
            logging.info(f"Reading the first {num_rows} rows from the file")
            return list(next(reader) for _ in range(num_rows))


def write_csv(filename, rows):
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(rows)
    logging.info(f"Wrote {len(rows)} rows to the file {filename}")


rows = read_csv("data/concatenated.csv")
# rows = read_csv("data/concatenated.csv", 20)

for i, row in enumerate(rows):
    for j, item in enumerate(row[1:], start=1):
        score = eval(item)[1]
        rows[i][j] = score

write_csv("output.csv", rows)
