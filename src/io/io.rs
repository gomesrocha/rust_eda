use polars::prelude::*;

pub fn read_csv(input_file: &str) -> PolarsResult<DataFrame> {
    let df = CsvReader::from_path(input_file)?
        .with_try_parse_dates(true)
        .finish()?;
    println!("{}", &df);

    Ok(df)
}

pub fn write_parquet(df: &mut DataFrame, output_file: &str) -> PolarsResult<()> {
    if std::fs::metadata(output_file).is_ok() {
        println!("File {} already exists. Choose a different output file.", output_file);
        return Ok(());
    }

    let f = std::fs::File::create(output_file)?;
    ParquetWriter::new(f).with_statistics(true).finish(df)?;
    Ok(())
}
