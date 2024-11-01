use polars::prelude::*;

pub fn join(df: &mut DataFrame) -> PolarsResult<DataFrame> {
    let df2 = CsvReader::from_path("datasets/GlobalLandTemperaturesByCountry.csv")
        .unwrap()
        .with_try_parse_dates(true)
        .finish()
        .unwrap();
    let fetch_df = df.clone().lazy().fetch(10)?;

    let out = fetch_df
        .clone()
        .lazy()
        .join(
            df2.clone().lazy(),
            [col("dt")],
            [col("dt")],
            JoinArgs::new(JoinType::Inner),
        )
        .collect()?;
    println!("{}", &out);
    Ok(out)
}

pub fn cross_join(df: &mut DataFrame) -> PolarsResult<DataFrame> {
    let df2 = CsvReader::from_path("datasets/GlobalTemperatures.csv")
        .unwrap()
        .with_try_parse_dates(true)
        .finish()
        .unwrap();

    let fetch_df = df2.clone().lazy().fetch(10)?;
    let cat = fetch_df
        .clone()
        .lazy()
        .select([col("dt"), col("LandAndOceanAverageTemperature")])
        .collect()?;

    let out = df.clone().lazy().cross_join(cat.clone().lazy()).collect()?;
    println!("{}", &out);
    Ok(out)
}
