mod batch_size_limit;
mod binary;
mod boolean;
mod column_strategy;
mod conversion_strategy;
mod current_file;
mod date;
mod decimal;
mod fetch_batch;
mod identical;
mod parquet_writer;
mod text;
mod time;
mod timestamp;
mod timestamp_precision;
mod timestamp_tz;

use crate::connection::{open_connection,ConnectOpts};
use parquet::basic::{Compression,ZstdLevel};
use anyhow::Error;
use fetch_batch::{fetch_strategy, FetchBatch};
use io_arg::IoArg;
use log::info;
use odbc_api::{Cursor, IntoParameter};
use std::io::{stdin, Read};

use self::{
    batch_size_limit::{BatchSizeLimit, FileSizeLimit},
    column_strategy::{ColumnStrategy, MappingOptions},
    conversion_strategy::ConversionStrategy,
    parquet_writer::{parquet_output, ParquetWriterOptions},
};

use std::path::PathBuf;
use bytesize::ByteSize;
use crate::enum_args::{CompressionVariants,EncodingArgument};


use parquet::basic::Encoding;
#[derive(Debug)]
pub struct QueryOpt {
    pub connect_opts: ConnectOpts,
    pub query: String,
    pub output: IoArg,
    pub column_compression_default: CompressionVariants,
    pub encoding: EncodingArgument,
    pub batch_size_row: Option<usize>,
    pub batch_size_memory: Option<ByteSize>,
    pub row_groups_per_file: u32,
    pub sequential_fetching: bool,
    pub file_size_threshold: Option<ByteSize>,
    pub column_length_limit: usize,
    pub parquet_column_encoding: Vec<(String, Encoding)>,
    pub driver_does_not_support_64bit_integers: bool,
    pub avoid_decimal: bool,
    pub suffix_length: usize,
    pub no_empty_file: bool,
    pub parameters: Vec<String>,
}



fn query_statement_text(query: String) -> Result<String, Error> {
    Ok(if query == "-" {
        let mut buf = String::new();
        stdin().lock().read_to_string(&mut buf)?;
        buf
    } else {
        query
    })
}

/// Execute a query and writes the result to parquet.
pub fn query(opt: QueryOpt) -> Result<usize, Error> {
    let QueryOpt {
        connect_opts,
        output,
        parameters,
        query,
        batch_size_row,
        batch_size_memory,
        row_groups_per_file,
        sequential_fetching,
        file_size_threshold,
        encoding,
        parquet_column_encoding,
        avoid_decimal,
        driver_does_not_support_64bit_integers,
        suffix_length,
        no_empty_file,
        column_length_limit,
        column_compression_default,
    } = opt;

    let batch_size = BatchSizeLimit::new(batch_size_row, batch_size_memory);
    let file_size = FileSizeLimit::new(row_groups_per_file, file_size_threshold);
    let query = query_statement_text(query)?;
    let params: Vec<_> = parameters.iter()
        .map(|param| param.as_str().into_parameter())
        .collect();

    let odbc_conn = open_connection(&connect_opts)?;
    let db_name = odbc_conn.database_management_system_name()?;
    info!("Database Management System Name: {db_name}");

    let parquet_format_options = ParquetWriterOptions {
        column_compression_default: Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        column_encodings: parquet_column_encoding,
        file_size,
        suffix_length,
        no_empty_file,
    };
    let mapping_options = MappingOptions {
        prefer_varbinary: false,
        db_name: &db_name,
        use_utf16: encoding.use_utf16(),
        avoid_decimal,
        driver_does_support_i64: !driver_does_not_support_64bit_integers,
        column_length_limit,
    };

    if let Some(cursor) = odbc_conn.into_cursor(&query, params.as_slice(), None)
        .map_err(odbc_api::Error::from)?
    {
        let row_count = cursor_to_parquet(
            cursor,
            output,
            batch_size,
            !sequential_fetching,
            mapping_options,
            parquet_format_options,
        )?;
        Ok(row_count)
    } else {
        eprintln!(
            "Query came back empty (not even a schema has been returned). No file has been created"
        );
        Ok(0)
    }
}



fn cursor_to_parquet(
    mut cursor: impl Cursor + Send + 'static,
    path: IoArg,
    batch_size: BatchSizeLimit,
    concurrent_fetching: bool,
    mapping_options: MappingOptions,
    parquet_format_options: ParquetWriterOptions,
) -> Result<usize, Error> {
    let table_strategy = ConversionStrategy::new(&mut cursor, mapping_options)?;
    let parquet_schema = table_strategy.parquet_schema();
    let writer = parquet_output(path, parquet_schema.clone(), parquet_format_options)?;
    let fetch_strategy: Box<dyn FetchBatch> =
        fetch_strategy(concurrent_fetching, cursor, &table_strategy, batch_size)?;
    let row_count = table_strategy.block_cursor_to_parquet(fetch_strategy, writer)?;
    println!("{}", row_count);
    eprintln!("Returning {} rows", row_count);
    Ok(row_count)
}