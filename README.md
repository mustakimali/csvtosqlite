# CSV to SQLite Importer

This Rust project imports CSV data into an SQLite database. It reads a CSV file, creates a corresponding SQLite table, and inserts the data into the table. The table's schema is inferred from the CSV file's contents.

[![csvtosqlite Demo](https://asciinema.org/a/674935.svg)](https://asciinema.org/a/674935)

## Features

- **CSV to SQLite:** Automatically create an SQLite table based on the CSV headers and data types.
- **Customizable Database and Table Names:** Specify the database name and table name via command-line arguments.
- **Dry Run Mode:** Preview the table creation and SQL insert statements without executing them.
- **Batch Insert:** Efficiently inserts rows in batches to optimize performance.

## Installation

### Install from Crates.io

```bash
cargo install --locked csvtosqlite
```

### Build from sourcce

Make sure you have Rust and Cargo installed on your machine. Then, build the project using:

```bash
cargo build --release
```

## Usage

Run the program using the command line. The following options are available:

```bash
csvtosqlite [OPTIONS]
```

### Options:

- `-f, --file <FILE>`: The CSV file to import. Default is `sample.csv`.
- `-t, --table <TABLE>`: The name of the SQLite table. If not specified, the table name defaults to the CSV file name (without extension).
- `-d, --db <DB>`: The SQLite database file. Default is `data.db`.
- `--dry-run`: When set, it prints the SQL commands without executing them. Default is `false`.
- `--no-auto-detect-types`: When set, skip detecting types. All columns will be treated as strings. This is useful rows have mixed data types
- `-b`, --batch-size <BATCH_SIZE>`: Batch size (Default 10000). When debugging: Reduce to 1 to identify the row that is causing the error. Query will be written in a file when insert fails.

### Example Commands

1. **Basic Import:**

   Import a CSV file (`sample.csv`) into the default database (`data.db`) and create a table with the same name as the CSV file:

   ```bash
   csv-to-sqlite --file sample.csv
   ```

2. **Custom Table Name:**

   Import a CSV file and create a custom table name:

   ```bash
   csv-to-sqlite --file data.csv --table my_table
   ```

3. **Dry Run:**

   Preview the SQL commands without actually creating the table or inserting data:

   ```bash
   csvtosqlite --file data.csv --dry-run true
   ```

4. Disable auto infer data type and specify the data type for each column:
  For CSV files with invalid data types across rows.

  ```bash
   csvtosqlite -f sample_file.csv --no-auto-detect-types
   ```

## How It Works

1. **CSV Parsing:**
   - The program reads the CSV file and extracts the headers.
   - It determines the data type of each column based on the first row's values.

2. **SQL Table Creation:**
   - A SQL `CREATE TABLE` statement is generated based on the headers and their inferred types (e.g., `INTEGER`, `TEXT`, `REAL`, etc.).
   - If `dry-run` is enabled, it only prints the SQL statements without executing them.

3. **Data Insertion:**
   - The program inserts the CSV rows into the SQLite table in batches of 10,000 rows for efficiency.
   - It displays the number of rows inserted and the speed of insertion.

## Performance

The program uses batching and transactions to optimize the performance of inserting data into SQLite. You can monitor the progress and speed of the insertion process, which is displayed in rows per second (wps).

30s to import 6.4million rows from [Geographic units, by industry and statistical area: 2000–2023 descending order – CSV](https://www.stats.govt.nz/assets/Uploads/New-Zealand-business-demography-statistics/New-Zealand-business-demography-statistics-At-February-2023/Download-data/geographic-units-by-industry-and-statistical-area-20002023-descending-order-.zip) from [stats.govt.nz](https://www.stats.govt.nz/large-datasets/csv-files-for-download/) site.
```bash
❯ time ./csvtosqlite -f sample_nz_geo_units.csv
Opening CSV file: sample_nz_geo_units.csv
Parsing headers...
Determining data type based on the first row...
> anzsic06 (anzsic06): String
> area (Area): String
> year (year): Integer
> geo_count (geo_count): Integer
> ec_count (ec_count): Integer
Creating database: data.db
Creating table:
CREATE TABLE IF NOT EXISTS sample_nz_geo_units (
  anzsic06 TEXT,
  area TEXT,
  year INTEGER,
  geo_count INTEGER,
  ec_count INTEGER
);
Inserting rows...
Finalising...Done! Inserted 6,457,053 rows (217,304,041 bytes) at (214150.80 rps)
./csvtosqlite -f sample_nz_geo_units.csv   28.76s  user 0.81s system 98% cpu 30.166 total
avg shared (code):         0 KB
avg unshared (data/stack): 0 KB
total (sum):               0 KB
max memory:                23
page faults from disk:     0
other page faults:         114444
```

## Dependencies

This project relies on the following Rust crates:

- **clap:** For command-line argument parsing.
- **csv:** For reading and parsing CSV files.
- **sqlx:** For interacting with the SQLite database.
- **tokio:** For asynchronous programming.

You can find the dependencies in the `Cargo.toml` file.

## Contributing

If you would like to contribute to this project, please open a pull request or an issue with your suggestions or bug reports.
