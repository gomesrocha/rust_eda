use polars::prelude::*;
use std::process::exit;

pub fn query_all(df: &mut DataFrame) -> PolarsResult<DataFrame> {
    let out = df.clone().lazy().select([col("*")]).collect()?;
    println!("{}", &out);
    Ok(out)
}

pub fn query_multi_col(df: &mut DataFrame, exclude_cols: &[&str]) -> PolarsResult<DataFrame> {
    if exclude_cols.len() < 2 {
        println!("Please provide at least 2 column names to exclude");
        exit(1);
    }

    let out = df
        .clone()
        .lazy()
        .select([col("*").exclude(exclude_cols)])  
        .collect()?;
    println!("{}", &out);
    Ok(out)
}

pub fn get_count(df: &mut DataFrame, count_col: &str) -> PolarsResult<DataFrame> {
    let df_alias = df
        .clone()
        .lazy()
        .select([col(count_col).n_unique().alias("unique").cast(DataType::Utf8)])
        .collect()?;
    println!("{}", &df_alias);
    Ok(df_alias)
}

pub fn down_cast(df: &mut DataFrame) -> PolarsResult<DataFrame> {
    let down_cast_df = df
        .clone()
        .lazy()
        .with_columns([col("AverageTemperature").cast(DataType::Float32), col("AverageTemperatureUncertainty").cast(DataType::Float32)])
        .collect()?;
    println!("{}", &down_cast_df);
    Ok(down_cast_df)
}

pub fn filter(df: &mut DataFrame, filter_col: &str, start_with: &str) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .filter(col(filter_col).str().starts_with(lit(start_with)))
        .collect()?;
    println!("{}", &out);
    Ok(out)
}

pub fn explore(df: &mut DataFrame, select_col: &str) -> PolarsResult<DataFrame> {
    let _df = df.clone().lazy().select([col(select_col)]).collect()?;
    println!("{}", &_df);
    Ok(_df)
}


pub fn aggregate(df: &mut DataFrame, aggregate_column: &str) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .group_by([col(aggregate_column)])
        .agg([count(), col("AverageTemperature")])
        .sort(
            "count",
            SortOptions {
                descending: true,
                nulls_last: true,
                ..Default::default()
            },
        )
        .collect()?;
    println!("{}", &out);

    Ok(out)
}


pub fn null_cnt(df: &mut DataFrame) -> () {
    let null_cnt = df.null_count();
    println!("{}", &null_cnt);
}

pub fn clean_data(df: &mut DataFrame) -> PolarsResult<DataFrame> {
    let fill_interpolation_df = df
        .clone()
        .lazy()
        .with_columns([col("*").interpolate(InterpolationMethod::Linear)])
        .collect()?;
    println!("{}", &fill_interpolation_df);
    Ok(fill_interpolation_df)
}

pub fn forward_fill(df: &mut DataFrame) -> PolarsResult<DataFrame> {
    let forward_fill_df = df
        .clone()
        .lazy()
        .with_columns([col("*").forward_fill(None)])
        .collect()?;
    println!("{}", &forward_fill_df);
    Ok(forward_fill_df)
}

pub fn get_mean(df: &mut DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .select([
            col("Country"),
            col("State"),
            col("AverageTemperature")
                .mean()
                .over(["State"])
                .alias("MeanTemperatureByState")
                .cast(DataType::Float32),
            col("AverageTemperatureUncertainty")
                .mean()
                .over(["State", "Country"])
                .alias("Mean_TemperatureUncertaintyByStateCountry")
                .cast(DataType::Float32),
        ])
        .unique(None, UniqueKeepStrategy::First)  // Ensure unique rows
        .collect()?;

    println!("{}", out);

    Ok(out)
}

pub fn top_bottom(df: &mut DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .with_columns([
            col("AverageTemperature").list().head(lit(3)).alias("top3"),
            col("AverageTemperature")
                .list()
                .slice(lit(-3), lit(3))
                .alias("bottom_3"),
        ])
        .collect()?;
    println!("{}", &out);

    Ok(out)
}

pub fn to_struct(df: &mut DataFrame) -> PolarsResult<DataFrame> {
    let column = "Country"; // Example column name for struct creation
    let out = df
        .clone()
        .lazy()
        .select([col(column).value_counts(true, true)])
        .collect()?;

    println!("{}", out);

    Ok(out)
}

pub fn to_dict(df: &mut DataFrame) -> PolarsResult<Series> {
    let out = df.clone().into_struct("climate").into_series();
    let column_name = "Country"; // Example column name for dictionary extraction
    let dict = out.struct_()?.field_by_name(column_name)?;

    println!("{}", &out);
    println!("{}", &dict);
    Ok(out)
}

pub fn dict_to_struct(out: &mut Series) -> PolarsResult<()> {
    let df = DataFrame::new([out.clone()].into())?
        .lazy()
        .select([col("climate")
            .struct_()
            .rename_fields(
                [
                    "dt".into(),
                    "AverageTemperature".into(),
                    "AverageTemperatureUncertainty".into(),
                    "State".into(),
                    "Country".into(),
                ]
                .to_vec(),
            )])
        .unnest(["climate"])
        .collect()?;

    println!("{}", &df);
    Ok(())
}
