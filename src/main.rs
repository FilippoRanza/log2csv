use csv::Writer;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use structopt::StructOpt;

#[derive(StructOpt)]
struct Arguments {
    input_file: PathBuf,
    output_dir: PathBuf,
}

#[derive(Deserialize, Debug)]
struct Update<'a> {
    key: &'a str,
    value: &'a str,
}

#[derive(Deserialize, Debug)]
struct Log<'a> {
    #[serde(rename = "update-name")]
    name: &'a str,
    old: Vec<Update<'a>>,
    new: Vec<Update<'a>>,
    info: Vec<Update<'a>>,
}

#[derive(Deserialize, Debug)]
struct FileLog<'a> {
    #[serde(rename = "file-name")]
    name: &'a str,
    #[serde(rename = "log")]
    logs: Vec<Log<'a>>,
}

#[derive(Deserialize, Debug)]
#[serde(bound(deserialize = "'de: 'a"))]
struct Logs<'a>(Vec<FileLog<'a>>);

type Headers<'a> = HashMap<&'a str, Vec<String>>;
type Rows<'a> = HashMap<&'a str, Vec<Vec<&'a str>>>;
type Tables<'a> = HashMap<&'a str, Table<'a>>;

#[derive(Debug)]
struct Table<'a> {
    names: Vec<String>,
    rows: Vec<Vec<&'a str>>,
}
impl<'a> Table<'a> {
    fn new(names: Vec<String>) -> Self {
        let rows = Vec::new();
        Self { names, rows }
    }

    fn add_rows(&mut self, mut rows: Vec<Vec<&'a str>>) {
        self.rows.append(&mut rows);
    }

    fn write_to_csv<W: Write>(self, mut writer: Writer<W>) -> std::io::Result<()> {
        writer.write_record(self.names)?;
        for row in self.rows.into_iter() {
            writer.write_record(row)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ExportTable<'a> {
    tables: Tables<'a>,
}

fn make_csv_writer(base_dir: &Path, name: &str) -> std::io::Result<Writer<File>> {
    let file_path = base_dir.join(format!("{}.csv", name));
    let writer = Writer::from_path(file_path)?;
    Ok(writer)
}

fn export_table(name: &str, table: Table<'_>, base_dir: &Path) -> std::io::Result<()> {
    let writer = make_csv_writer(base_dir, name)?;
    table.write_to_csv(writer)
}

impl<'a> ExportTable<'a> {
    fn export_csv(self, base_dir: &Path) -> std::io::Result<()> {
        for (k, v) in self.tables.into_iter() {
            export_table(k, v, base_dir)?;
        }

        Ok(())
    }
}

fn make_header_row<'a>(log: &Log<'a>) -> Vec<String> {
    let mut vect = vec!["file-name".into()];
    for o in &log.old {
        let key = o.key;
        vect.push(format! {"old_{}", key});
        vect.push(format! {"new_{}", key});
    }

    for i in &log.info {
        vect.push(i.key.into());
    }

    vect
}

fn get_log_values<'a>(file_name: &'a str, log: &Log<'a>) -> Vec<&'a str> {
    let mut vect = vec![file_name];
    for (o, n) in log.old.iter().zip(&log.new) {
        vect.push(o.value);
        vect.push(n.value);
    }
    for i in &log.info {
        vect.push(i.value);
    }

    vect
}

fn insert_header<'a>(log: &Log<'a>, headers: &mut Headers<'a>) {
    let name = log.name;
    if !headers.contains_key(name) {
        let header = make_header_row(log);
        headers.insert(name, header);
    }
}

fn insert_row<'a>(
    file_name: &'a str,
    log: &Log<'a>,
    rows: &mut HashMap<&'a str, Vec<Vec<&'a str>>>,
) {
    let name = log.name;
    let row = get_log_values(file_name, log);
    if let Some(rows) = rows.get_mut(name) {
        rows.push(row);
    } else {
        rows.insert(name, vec![row]);
    }
}

fn convert_logs<'a>(file_name: &'a str, logs: &[Log<'a>], headers: &mut Headers<'a>) -> Rows<'a> {
    let mut rows = HashMap::new();
    for log in logs {
        insert_header(log, headers);
        insert_row(file_name, log, &mut rows);
    }

    rows
}

fn make_headers_and_rows<'a>(file_logs: &[FileLog<'a>]) -> (Headers<'a>, Vec<Rows<'a>>) {
    let mut headers = HashMap::new();
    let rows_map = file_logs
        .iter()
        .map(|flog| convert_logs(flog.name, &flog.logs, &mut headers))
        .collect();
    (headers, rows_map)
}

fn make_tables<'a>(header: Headers<'a>) -> HashMap<&'a str, Table<'a>> {
    header
        .into_iter()
        .map(|(k, v)| (k, Table::new(v)))
        .collect()
}

fn append_rows<'a>(mut tables: Tables<'a>, rows: Rows<'a>) -> Tables<'a> {
    for (name, row) in rows.into_iter() {
        let table = tables.get_mut(name).unwrap();
        table.add_rows(row);
    }
    tables
}

fn fill_tables<'a>(tables: Tables<'a>, rows: Vec<Rows<'a>>) -> Tables<'a> {
    rows.into_iter().fold(tables, append_rows)
}

fn logs_to_tables<'a>(file_logs: Logs<'a>) -> ExportTable<'a> {
    let (headers, rows) = make_headers_and_rows(&file_logs.0);
    let tables = make_tables(headers);
    let tables = fill_tables(tables, rows);
    ExportTable { tables }
}

fn read_file(file_name: &Path) -> std::io::Result<String> {
    let mut file = File::open(file_name)?;
    let mut str = String::new();
    file.read_to_string(&mut str)?;
    Ok(str)
}

fn make_dir_if_required(path: &Path) -> std::io::Result<()> {
    if !path.is_dir() {
        create_dir_all(path)
    } else {
        Ok(())
    }
}

fn export_convertion(exports: ExportTable<'_>, dir_path: &Path) -> std::io::Result<()> {
    make_dir_if_required(dir_path)?;
    exports.export_csv(dir_path)?;
    Ok(())
}

fn main() -> std::io::Result<()> {
    let args = Arguments::from_args();
    let log_text = read_file(&args.input_file)?;
    let logs: Logs = serde_json::from_str(&log_text).unwrap();
    let export = logs_to_tables(logs);
    export_convertion(export, &args.output_dir)?;
    Ok(())
}
