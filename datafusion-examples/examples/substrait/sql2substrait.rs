use datafusion::prelude::*;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use object_store::aws::AmazonS3Builder;
use prost::Message;
use std::env;
use std::fs;
use std::sync::Arc;
use url::Url;
use datafusion_common::DataFusionError;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // 1. Initialize the Session Context
    let ctx = SessionContext::new();

    // 2. Configure the S3 Object Store
    // In a real application, you might use environment variables (AWS_ACCESS_KEY_ID, etc.)
    // which AmazonS3Builder will pick up automatically if configured to do so.
    let s3 = AmazonS3Builder::from_env()
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
        "lineitem",
        "s3://serverless-analytics-tpch-west2/dataset/SF1-combined-metadata/velox_compatible/lineitem.parquet",
        ParquetReadOptions::default(),
    ).await?;

    // 5. Parse the SQL into a DataFusion Logical Plan
    let sql = "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;";
    let plan = ctx.state().create_logical_plan(sql).await?;

    // 6. Convert the DataFusion plan to a Substrait Plan
    let substrait_plan = to_substrait_plan(&plan, &ctx.state())?;

    let json_path = env::var("SUBSTRAIT_PLAN_JSON_PATH")
        .unwrap_or_else(|_| "/tmp/plan.substrait.json".to_string());
    let proto_path = env::var("SUBSTRAIT_PLAN_PROTO_PATH")
        .unwrap_or_else(|_| "/tmp/plan.substrait.bin".to_string());

    let json_bytes = serde_json::to_vec_pretty(&substrait_plan)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    fs::write(&json_path, json_bytes)?;
    fs::write(&proto_path, substrait_plan.encode_to_vec())?;

    println!("Wrote Substrait JSON plan to {json_path}");
    println!("Wrote Substrait Protobuf plan to {proto_path}");

    Ok(())
}
