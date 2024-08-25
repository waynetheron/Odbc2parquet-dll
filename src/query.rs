mod batch_size_limit;
mod binary;
mod boolean;
mod column_strategy;
mod current_file;
mod date;
mod decimal;
mod identical;
mod parquet_writer;
mod table_strategy;
mod text;
mod time;
mod timestamp;
mod timestamp_precision;
mod timestamp_tz;

use anyhow::Error;
use io_arg::IoArg;
use log::info;
use odbc_api::{Cursor, Environment, IntoParameter};
use std::io::{stdin, Read};

use self::{
    batch_size_limit::{BatchSizeLimit, FileSizeLimit},
    column_strategy::{ColumnStrategy, MappingOptions},
    parquet_writer::{parquet_output, ParquetWriterOptions},
    table_strategy::TableStrategy,
};

use crate::{open_connection, QueryOpt};

/// Execute a query and writes the result to parquet.
pub fn query(environment: &Environment, opt: QueryOpt) -> Result<(), Error> {
    let QueryOpt {
        connect_opts,
        output,
        parameters,
        query,
        batch_size_row,
        batch_size_memory,
        row_groups_per_file,
        file_size_threshold,
        encoding,
        prefer_varbinary,
        column_compression_default,
        column_compression_level_default,
        parquet_column_encoding,
        avoid_decimal,
        driver_does_not_support_64bit_integers,
        suffix_length,
        no_empty_file,
        column_length_limit,
    } = opt;

    let batch_size = BatchSizeLimit::new(batch_size_row, batch_size_memory);
    let file_size = FileSizeLimit::new(row_groups_per_file, file_size_threshold);
    let query = query_statement_text(query)?;

    // Convert the input strings into parameters suitable for use with ODBC.
    let params: Vec<_> = parameters
        .iter()
        .map(|param| param.as_str().into_parameter())
        .collect();

    let odbc_conn = open_connection(environment, &connect_opts)?;
    let db_name = odbc_conn.database_management_system_name()?;
    info!("Database Management System Name: {db_name}");

    let parquet_format_options = ParquetWriterOptions {
        column_compression_default: column_compression_default
            .to_compression(column_compression_level_default)?,
        column_encodings: parquet_column_encoding,
        file_size,
        suffix_length,
        no_empty_file,
    };

    let mapping_options = MappingOptions {
        db_name: &db_name,
        use_utf16: encoding.use_utf16(),
        prefer_varbinary,
        avoid_decimal,
        driver_does_support_i64: !driver_does_not_support_64bit_integers,
        column_length_limit,
    };

    if let Some(cursor) = odbc_conn.execute(&query, params.as_slice())? {
        cursor_to_parquet(
            cursor,
            output,
            batch_size,
            mapping_options,
            parquet_format_options,
        )?;
    } else {
        eprintln!(
            "Query came back empty (not even a schema has been returned). No file has been created"
        );
    }
    Ok(())
}

/// The query statement is either passed verbatim at the command line, or via stdin. The latter is
/// indicated by passing `-` at the command line instead of the string. This method reads stdin
/// until EOF if required and always returns the statement text.
fn query_statement_text(query: String) -> Result<String, Error> {
    Ok(if query == "-" {
        let mut buf = String::new();
        stdin().lock().read_to_string(&mut buf)?;
        buf
    } else {
        query
    })
}

fn cursor_to_parquet(
    mut cursor: impl Cursor,
    path: IoArg,
    batch_size: BatchSizeLimit,
    mapping_options: MappingOptions,
    parquet_format_options: ParquetWriterOptions,
) -> Result<(), Error> {
    let table_strategy = TableStrategy::new(&mut cursor, mapping_options)?;
    let mut odbc_buffer = table_strategy.allocate_fetch_buffer(batch_size)?;
    let block_cursor = cursor.bind_buffer(&mut odbc_buffer)?;
    let parquet_schema = table_strategy.parquet_schema();
    let writer = parquet_output(path, parquet_schema.clone(), parquet_format_options)?;
    table_strategy.block_cursor_to_parquet(block_cursor, writer)?;
    Ok(())
}
