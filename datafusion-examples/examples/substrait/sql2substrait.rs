use datafusion::prelude::*;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use url::Url;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // 1. Initialize the Session Context
    let ctx = SessionContext::new();

    // 2. Configure the S3 Object Store
    // In a real application, you might use environment variables (AWS_ACCESS_KEY_ID, etc.)
    // which AmazonS3Builder will pick up automatically if configured to do so.
    let s3 = AmazonS3Builder::new()
        .with_bucket_name("serverless-analytics-tpch-west2")
        .with_region("us-west-2")
        .build()
        .expect("Failed to build S3 object store");

    // 3. Register the Object Store with DataFusion for the "s3://" scheme
    let s3_url = Url::parse("s3://serverless-analytics-tpch-west2").unwrap();
    ctx.runtime_env()
        .register_object_store(&s3_url, Arc::new(s3));

    // 4. Register the Parquet file as a table
    // DataFusion will read the Parquet metadata from S3 to infer the schema
    ctx.register_parquet(
        "s3_lineitem",
        "s3://serverless-analytics-tpch-west2/dataset/SF1-combined-metadata/velox_compatible/lineitem.parquet",
        ParquetReadOptions::default(),
    ).await?;

    // 5. Parse the SQL into a DataFusion Logical Plan
    let sql = "SELECT sum(l_extendedprice * l_discount) AS revenue FROM s3_lineitem WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;";
    let plan = ctx.state().create_logical_plan(sql).await?;

    // 6. Convert the DataFusion plan to a Substrait Plan
    let substrait_plan = to_substrait_plan(&plan, &ctx.state())?;

    // Print the Substrait plan (can be serialized to Protobuf bytes later)
    println!("{:#?}", substrait_plan);

    Ok(())
}
