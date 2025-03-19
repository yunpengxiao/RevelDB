use std::path::Path;

use clap::Parser;
use leveldb::database::Database as DatabaseC;
use leveldb::kv::KV;
use leveldb::options::{Options, ReadOptions, WriteOptions};

use crate::database::Database;

mod database;
mod log_writter;
mod mem_table;
mod write_batch;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Flags {
    #[arg(long, default_value = "")]
    benchmarks: String,
    #[arg(long, default_value_t = 0.0)]
    compression_ratio: f64,
    #[arg(long, default_value_t = false)]
    histogram: bool,
    #[arg(long, default_value_t = false)]
    comparisons: bool,
    #[arg(long, default_value_t = false)]
    use_existing_db: bool,
    #[arg(long, default_value_t = false)]
    reuse_logs: bool,
    #[arg(long, default_value_t = false)]
    compression: bool,
    #[arg(long, default_value_t = 0)]
    num: i32,
    #[arg(long, default_value_t = 0)]
    reads: i32,
    #[arg(long, default_value_t = 0)]
    threads: i32,
    #[arg(long, default_value_t = 0)]
    value_size: i32,
    #[arg(long, default_value_t = 0)]
    write_buffer_size: i32,
    #[arg(long, default_value_t = 0)]
    max_file_size: i32,
    #[arg(long, default_value_t = 0)]
    block_size: i32,
    #[arg(long, default_value_t = 0)]
    key_prefix: i32,
    #[arg(long, default_value_t = 0)]
    cache_size: i32,
    #[arg(long, default_value_t = 0)]
    bloom_bits: i32,
    #[arg(long, default_value_t = 0)]
    open_files: i32,
    #[arg(long, default_value = "")]
    db: String,
}

struct Benchmark {
    db: Database,
}

impl Benchmark {
    pub fn new(flags: Flags) -> Self {
        Benchmark {
            db: Database::new(),
        }
    }

    pub fn run(&mut self) {
        self.print_header();
        self.run_benchmark();
    }

    fn print_header(&self) {
        println!("------------------------------------------------");
    }

    fn run_benchmark(&mut self) {
        self.db
            .put(&String::from("123"), &String::from("456"))
            .unwrap();
        println!("{}", self.db.get(&String::from("123")).unwrap());
    }
}

fn main() {
    let flags = Flags::parse();
    //println!("Flags: {:?}", flags);
    let mut benchmark = Benchmark::new(flags);
    benchmark.run();

    let mut options = Options::new();
    options.create_if_missing = true;
    let path = Path::new("/tmp");
    let database: DatabaseC<i32> = match DatabaseC::open(path, options) {
        Ok(db) => db,
        Err(e) => {
            panic!("failed to open database: {:?}", e)
        }
    };
    let write_opts = WriteOptions::new();
    match database.put(write_opts, 1, &[1]) {
        Ok(_) => (),
        Err(e) => {
            panic!("failed to write to database: {:?}", e)
        }
    };

    let read_opts = ReadOptions::new();
    let res = database.get(read_opts, 1).unwrap();
    println!("{:?}", res);
}
