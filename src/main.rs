use std::{io::Write, time::Instant};

use clap::Parser;
use csv::StringRecord;
use itertools::Itertools;
use num_format::{Locale, ToFormattedString};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

// SQL reserved words
const RESERVED: [&str; 116] = [
    "add",
    "all",
    "alter",
    "and",
    "as",
    "asc",
    "autoincrement",
    "between",
    "by",
    "cascade",
    "case",
    "cast",
    "check",
    "collate",
    "column",
    "commit",
    "conflict",
    "constraint",
    "create",
    "cross",
    "current_date",
    "current_time",
    "current_timestamp",
    "database",
    "default",
    "deferrable",
    "deferred",
    "delete",
    "desc",
    "distinct",
    "drop",
    "each",
    "else",
    "end",
    "escape",
    "except",
    "exclusive",
    "exists",
    "explain",
    "fail",
    "for",
    "foreign",
    "from",
    "full",
    "glob",
    "group",
    "having",
    "if",
    "ignore",
    "immediate",
    "in",
    "index",
    "indexed",
    "initially",
    "inner",
    "insert",
    "instead",
    "intersect",
    "into",
    "is",
    "isnull",
    "join",
    "key",
    "left",
    "like",
    "limit",
    "match",
    "natural",
    "no",
    "not",
    "notnull",
    "null",
    "of",
    "offset",
    "on",
    "or",
    "order",
    "outer",
    "plan",
    "pragma",
    "primary",
    "query",
    "raise",
    "recursive",
    "references",
    "regexp",
    "reindex",
    "release",
    "rename",
    "replace",
    "restrict",
    "right",
    "rollback",
    "row",
    "savepoint",
    "select",
    "set",
    "table",
    "temp",
    "temporary",
    "then",
    "to",
    "transaction",
    "trigger",
    "union",
    "unique",
    "update",
    "using",
    "vacuum",
    "values",
    "view",
    "virtual",
    "when",
    "where",
    "with",
    "without",
];

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// CSV File to read
    #[clap(short, long, default_value = "sample.csv")]
    file: String,

    /// Table name to create (default if file name of the CSV)
    #[clap(short, long)]
    table: Option<String>,

    /// Database name (default: data.db)
    #[clap(short, long, default_value = "data.db")]
    db: String,

    /// Batch size.
    ///
    /// When debugging: Reduce to 1 to identify the row that is causing the error.
    #[clap(short, long, default_value = "10000")]
    batch_size: usize,

    /// Dry run
    #[clap(long, default_value = "false")]
    dry_run: bool,

    /// Do not auto-detect types.
    /// All columns will be treated as strings.
    /// This is useful rows have mixed data types.
    #[clap(long, default_value = "false")]
    no_auto_detect_types: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    println!("Opening CSV file: {}", args.file);

    let table_name = args
        .table
        .clone()
        .unwrap_or_else(|| args.file.replace(".csv", ""));
    let f = std::fs::File::open(args.file).expect("read csv file");
    let mut csv_r = csv::Reader::from_reader(f);

    println!("Parsing headers...");
    let headers = csv_r
        .headers()
        .expect("headers")
        .iter()
        .map(|h| h.to_string())
        .collect::<Vec<_>>();

    println!("Determining data type based on the first row...");
    let first_row = csv_r
        .records()
        .next()
        .expect("first row")
        .expect("first row");
    let headers = first_row
        .iter()
        .zip(headers.iter())
        .map(|(val, header)| CsvHeader::new(header.to_string(), determine_sql_type(val)))
        .map(|header| match args.no_auto_detect_types {
            true => header.with_type(SqlType::String),
            false => header,
        })
        .collect::<Vec<_>>();

    headers.iter().for_each(|header| {
        println!(
            "> {} ({}): {:?}",
            header.normalised, header.title, header.ty
        );
    });

    let create_table_sql = create_table_sql(&table_name, &headers);

    if args.dry_run {
        println!("Creating database: {}", args.db);

        println!("Creating table:");
        println!("{}", create_table_sql);

        println!("Dry run enabled, exiting...");
        return;
    }

    println!("Creating database: {}", args.db);

    let opt = SqliteConnectOptions::new()
        .filename(args.db)
        .create_if_missing(true)
        .statement_cache_capacity(0); // nothing to cache - this reduces memory leaks
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opt)
        .await
        .expect("connect to db");

    println!("Creating table:");
    println!("{}", create_table_sql);

    sqlx::raw_sql(&create_table_sql)
        .execute(&pool)
        .await
        .expect("create table");

    println!("Inserting rows...");
    let mut inserted = 0;
    let mut inserted_bytes = 0;

    let remaining = csv_r.records().flat_map(|r| r.ok());
    let started = Instant::now();
    let mut sql_buffer = String::with_capacity(5 * args.batch_size);
    let mut tnx = pool.begin().await.expect("begin transaction");
    for batch in &vec![first_row]
        .into_iter()
        .chain(remaining)
        .chunks(args.batch_size)
    {
        let rows = batch.collect::<Vec<_>>();

        let added = insert_row_sql_batch(&mut sql_buffer, &table_name, &headers, rows);

        match sqlx::query(&sql_buffer).execute(&mut *tnx).await {
            Ok(_) => {}
            Err(e) => {
                // write to a file
                let path = format!("csvtosql_error_{}.sql", chrono::Utc::now().to_rfc3339());
                eprintln!("Error inserting Row (Query dumped: {})", path);
                let mut f = std::fs::File::create_new(path).unwrap();

                f.write_all(sql_buffer.as_bytes())
                    .expect("write query to dump file");

                _ = tnx.commit().await; // commit unsaved changes

                panic!("{:#?}\nExamine the file. Run with reduced batch size`-b 1` to identify the row. Or, Run with --no-auto-detect-types to disable auto-detect of columns.", e);
            }
        }

        inserted += added;
        inserted_bytes += sql_buffer.len();

        let rps = inserted as f32 / started.elapsed().as_secs_f32();
        print!(
            "\rInserted {} rows ({} bytes) at ({:.2} rps)",
            inserted.to_formatted_string(&Locale::en),
            inserted_bytes.to_formatted_string(&Locale::en),
            rps
        );
        std::io::stdout().flush().unwrap();
    }

    print!("\rFinalising...");
    tnx.commit().await.expect("commit transaction");

    let rps = inserted as f32 / started.elapsed().as_secs_f32();
    println!(
        "Done! Inserted {} rows ({} bytes) at ({:.2} rps)",
        inserted.to_formatted_string(&Locale::en),
        inserted_bytes.to_formatted_string(&Locale::en),
        rps
    );
}

fn insert_row_sql_batch(
    buffer: &mut String,
    table: &str,
    headers: &[CsvHeader],
    rows: Vec<StringRecord>,
) -> usize {
    let mut count = 0;
    buffer.clear();

    buffer.push_str("INSERT INTO ");
    buffer.push_str(table);
    buffer.push_str(" (");

    for h in headers {
        buffer.push_str(&format!("{}, ", h.normalised));
    }
    buffer.remove(buffer.len() - 2);
    buffer.push_str(") VALUES ");

    for row in rows {
        buffer.push('(');

        headers.iter().zip(row.iter()).for_each(|(h, v)| {
            if v.is_empty() {
                buffer.push_str("NULL, ");
            } else if h.need_quotes() {
                buffer.push_str(&format!(
                    "'{}', ",
                    v.replace(r#"""#, r#"\""#).replace("'", "''")
                ));
            } else {
                buffer.push_str(&format!("{}, ", v));
            }
        });

        buffer.remove(buffer.len() - 2);
        buffer.push_str("), ");
        count += 1;
    }

    buffer.remove(buffer.len() - 2);

    count
}

fn create_table_sql(name: &str, items: &[CsvHeader]) -> String {
    let mut sql = format!("CREATE TABLE IF NOT EXISTS {name} (");
    for h in items {
        sql.push_str(&format!("\n  {} ", h.normalised));
        sql.push_str(h.ty_str());
        sql.push(',');
    }
    sql.remove(sql.len() - 1);
    sql.push_str("\n);");
    sql
}

#[derive(Debug, Clone)]
struct CsvHeader {
    title: String,
    normalised: String,
    ty: SqlType,
}

impl CsvHeader {
    pub fn ty_str(&self) -> &str {
        match self.ty {
            SqlType::String => "TEXT",
            SqlType::Integer => "INTEGER",
            SqlType::Float => "REAL",
            SqlType::IsoDateTime => "TIMESTAMP",
            SqlType::NaiveDate => "DATE",
            SqlType::Boolean => "BOOLEAN",
        }
    }

    pub fn need_quotes(&self) -> bool {
        match self.ty {
            SqlType::String => true,
            SqlType::IsoDateTime => true,
            SqlType::NaiveDate => true,
            SqlType::Integer | SqlType::Boolean | SqlType::Float => false,
        }
    }

    pub fn with_type(&self, ty: SqlType) -> Self {
        Self {
            title: self.title.clone(),
            normalised: self.normalised.clone(),
            ty,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SqlType {
    String,
    Integer,
    Boolean,
    Float,
    IsoDateTime,
    NaiveDate,
}

impl CsvHeader {
    fn new(title: String, ty: SqlType) -> Self {
        // keep a-z, A-Z, 0-9, replace everything else with _, replace % with _pct
        let mut normalised = title
            .chars()
            .map(|x| match x {
                'a'..='z' | 'A'..='Z' | '0'..='9' => x,
                '%' => 'p',
                _ => '_',
            })
            .collect::<String>()
            .to_lowercase();

        if RESERVED.contains(&normalised.as_str()) {
            normalised.push('_');
        }

        Self {
            title,
            normalised,
            ty,
        }
    }
}

fn determine_sql_type(val: &str) -> SqlType {
    if val.parse::<i64>().is_ok() {
        SqlType::Integer
    } else if val.parse::<f64>().is_ok() {
        SqlType::Float
    } else if chrono::DateTime::parse_from_rfc3339(val).is_ok() {
        SqlType::IsoDateTime
    } else if chrono::NaiveDate::parse_from_str(val, "%Y-%m-%d").is_ok() {
        SqlType::NaiveDate
    } else if val.eq_ignore_ascii_case("true") || val.eq_ignore_ascii_case("false") {
        SqlType::Boolean
    } else {
        SqlType::String
    }
}
