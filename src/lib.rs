use std::fs::File;
use std::ffi::{CStr, c_char};
use std::os::raw::c_int;
mod connection;
use std::path::PathBuf;
mod enum_args;
mod insert;
mod parquet_buffer;
mod query;

use connection::ConnectOpts;
use enum_args::{CompressionVariants, EncodingArgument};
use io_arg::IoArg;
use crate::query::query;

#[no_mangle]
pub extern "C" fn export_odbc_to_parquet(
    connection_str: *const c_char,
    sql_query: *const c_char,
    output_path: *const c_char,
) -> usize {
    let conn_str = unsafe { CStr::from_ptr(connection_str).to_string_lossy().into_owned() };
    let query_str = unsafe { CStr::from_ptr(sql_query).to_string_lossy().into_owned() };
    let out_path = unsafe { CStr::from_ptr(output_path).to_string_lossy().into_owned() };

    let connect_opts = ConnectOpts {
        connection_string: Some(conn_str),
        ..Default::default()
    };

    let query_opt = crate::query::QueryOpt {
        connect_opts,
        query: query_str,
        output: IoArg::File(PathBuf::from(out_path)),
        column_compression_default: CompressionVariants::Zstd,
        encoding: EncodingArgument::Auto,
        batch_size_row: None,
        batch_size_memory: None,
        row_groups_per_file: 0,
        sequential_fetching: false,
        file_size_threshold: None,
        column_length_limit: 4096,
        parquet_column_encoding: vec![],
        driver_does_not_support_64bit_integers: false,
        avoid_decimal: false,
        suffix_length: 2,
        no_empty_file: false,
        parameters: vec![],
    };

    match query(query_opt) {
        Ok(row_count) => row_count,
        Err(e) => {
            eprintln!("Rust FFI export failed: {e:?}");
            1
        }
    }
}
