import csv
import os

def parse_transaction(s: str):
    """Parse a transaction like '(A, C, 5)' -> ['A', 'C', '5']"""
    s = s.strip()
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]
    return [item.strip() for item in s.split(",")]

def parse_nodes(s: str):
    """Parse nodes like '[n1, n2, n3]' -> ['n1', 'n2', 'n3']"""
    s = s.strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    return [item.strip() for item in s.split(",")]

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "sample_input.csv")

    # Try utf-8-sig first, fallback to latin1 if needed
    try:
        file = open(file_path, mode="r", newline="", encoding="utf-8-sig")
        file.read(1)
        file.seek(0)
    except UnicodeDecodeError:
        file = open(file_path, mode="r", newline="", encoding="latin1")

    with file:
        # Auto-detect delimiter (comma vs tab)
        sample = file.read(2048)
        file.seek(0)
        delimiter = "," if sample.count(",") > sample.count("\t") else "\t"
        reader = csv.DictReader(file, delimiter=delimiter)

        for row in reader:
            set_number = row["Set Number"].strip()
            transactions = parse_transaction(row["Transactions"])
            live_nodes = parse_nodes(row["Live Nodes"])

            # Print nicely
            print(f"\nSet Number: {set_number}")
            print("Transactions:")
            for t in transactions:
                print("  -", t)
            print("Live Nodes:")
            for node in live_nodes:
                print("  -", node)

if __name__ == "__main__":
    main()
