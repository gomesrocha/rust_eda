use polars::prelude::*;
mod io;
use crate::io::io::*;
mod expressions;
use crate::expressions::expressions::*;
mod transformations;
use crate::transformations::transformations::*;

fn main() -> PolarsResult<()> {
    // File paths
    let csv_path = "datasets/GlobalLandTemperaturesByState.csv";
    // let parquet_path_1 = "datasets/Task7_1.parquet";
    // let parquet_path_2 = "datasets/Task7_2.parquet";
    
    // Columns for various operations
    let exclude_cols = ["State", "AverageTemperatureUncertainty"];
    let count_col = "State";
    let filter_col = "State";
    let start_with = "Acre";
    let select_col = "State";
    let aggregate_column = "Country";
    

    // Scan CSV file input
    let _df = LazyCsvReader::new(csv_path).finish()?;

    // Read CSV file
    let mut df = read_csv(csv_path)?;

    // Write Parquet file
    // write_parquet(&mut df, parquet_path)?;
    
    // Query all
    query_all(&mut df)?;

    // Query multi-column with expression
    query_multi_col(&mut df, &exclude_cols)?;

    // Get count of unique values
    get_count(&mut df, count_col)?;

    // Lower down memory usage
    down_cast(&mut df)?;

    // Filter rows
    filter(&mut df, filter_col, start_with)?;

    // Processing string
    let mut _cnt = explore(&mut df, select_col)?;

    // Aggregate and sort by count
    aggregate(&mut df, aggregate_column)?;

    // Handle different types of missing data
    null_cnt(&mut df);

    let mut fill_interpolation_df = clean_data(&mut df)?;
    null_cnt(&mut fill_interpolation_df);
    // write_parquet(&mut fill_interpolation_df, parquet_path)?;

    let mut fill_forward_df = forward_fill(&mut df)?;
    null_cnt(&mut fill_forward_df);
    // write_parquet(&mut fill_forward_df, parquet_path)?;


    // Perform window function aggregations
    get_mean(&mut fill_interpolation_df)?;

    // Operating on list
    let mut agg = aggregate(&mut df, aggregate_column)?;
    top_bottom(&mut agg)?;

    // Handle the Struct data type.
    to_struct(&mut fill_interpolation_df)?;
    let mut out = to_dict(&mut fill_interpolation_df)?;
    dict_to_struct(&mut out)?;

    // Work on different kinds of join operations.
    let mut join_df = join(&mut df)?;
    null_cnt(&mut join_df); 

    cross_join(&mut df)?;

    Ok(())
}
