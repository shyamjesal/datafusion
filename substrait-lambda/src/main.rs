//! Minimal Substrait runner:
//! - reads a Substrait JSON plan from disk
//! - registers S3-backed parquet tables
//! - executes the plan
//! - writes results back to S3 as parquet
//!
//! Required environment variables:
//! - `SUBSTRAIT_PLAN_PATH=/tmp/plan.substrait.json`
//! - `SUBSTRAIT_S3_REGION=us-east-1`
//! - `SUBSTRAIT_INPUTS=orders|s3://bucket/orders/;lineitem|s3://bucket/lineitem/`
//! - `SUBSTRAIT_OUTPUT_LOCATION=s3://bucket/output/run-001/`

use std::collections::HashSet;
use std::env;
use std::fs;
use std::sync::Arc;

use datafusion::common::TableReference;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use object_store::aws::AmazonS3Builder;
use url::Url;

type DynError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, DynError>;

struct InputBinding {
    table_ref: TableReference,
    location: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let plan_path = required_env("SUBSTRAIT_PLAN_PATH")?;
    let region = required_env("S3_REGION")?;
    let output_location = required_env("OUTPUT_LOCATION")?;
    let inputs = parse_inputs(&required_env("SUBSTRAIT_INPUTS")?)?;

    let ctx = SessionContext::new();

    let mut all_locations: Vec<&str> =
        inputs.iter().map(|i| i.location.as_str()).collect();
    all_locations.push(output_location.as_str());
    register_s3_stores(&ctx, &region, &all_locations)?;

    for input in &inputs {
        ctx.register_parquet(
            input.table_ref.clone(),
            &input.location,
            ParquetReadOptions::default(),
        )
        .await?;
    }

    let plan_json = fs::read(plan_path)?;
    let plan: Plan = serde_json::from_slice(&plan_json)?;
    let logical_plan = from_substrait_plan(&ctx.state(), &plan).await?;
    let df = ctx.execute_logical_plan(logical_plan).await?;

    df.write_parquet(&output_location, DataFrameWriteOptions::new(), None)
        .await?;

    println!("Wrote Substrait results to {output_location}");
    Ok(())
}

fn required_env(name: &str) -> Result<String> {
    env::var(name)
        .map_err(|_| format!("missing required environment variable: {name}").into())
}

fn parse_inputs(raw: &str) -> Result<Vec<InputBinding>> {
    raw.split(';')
        .filter(|item| !item.trim().is_empty())
        .map(parse_input)
        .collect()
}

fn parse_input(raw: &str) -> Result<InputBinding> {
    let mut parts = raw.splitn(2, '|');
    let table = parts
        .next()
        .ok_or_else(|| format!("invalid input binding: {raw}"))?;
    let location = parts
        .next()
        .ok_or_else(|| format!("invalid input binding: {raw}"))?;

    if !location.starts_with("s3://") {
        return Err(format!("input location must be s3://..., got: {location}").into());
    }

    Ok(InputBinding {
        table_ref: TableReference::parse_str(table),
        location: location.to_string(),
    })
}

fn register_s3_stores(
    ctx: &SessionContext,
    region: &str,
    locations: &[&str],
) -> Result<()> {
    let mut seen_buckets = HashSet::new();

    for location in locations {
        let bucket = s3_bucket(location)?;
        if seen_buckets.insert(bucket.clone()) {
            let store = AmazonS3Builder::from_env()
                .with_bucket_name(&bucket)
                .with_region(region)
                .build()?;
            let root = Url::parse(&format!("s3://{bucket}"))?;
            ctx.register_object_store(&root, Arc::new(store));
        }
    }

    Ok(())
}

fn s3_bucket(location: &str) -> Result<String> {
    let parsed = Url::parse(location)?;
    if parsed.scheme() != "s3" {
        return Err(format!("expected s3:// URL, got {location}").into());
    }

    parsed
        .host_str()
        .map(ToString::to_string)
        .ok_or_else(|| format!("missing bucket in {location}").into())
}
