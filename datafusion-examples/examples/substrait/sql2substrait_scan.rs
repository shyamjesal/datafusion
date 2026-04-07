use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::DataFusionError;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use object_store::aws::AmazonS3Builder;
use prost::Message;
use std::env;
use std::fs;
use std::sync::Arc;
use url::Url;

fn env_or_default(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn parse_usize_env(name: &str, default: usize) -> Result<usize> {
    match env::var(name) {
        Ok(value) => value.parse::<usize>().map_err(|e| {
            DataFusionError::Execution(format!(
                "Invalid value for {name}: '{value}' ({e})"
            ))
        }),
        Err(_) => Ok(default),
    }
}

fn write_substrait_outputs(
    substrait_plan: &Plan,
    json_path: &str,
    proto_path: &str,
) -> Result<()> {
    let json_bytes = serde_json::to_vec_pretty(substrait_plan)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    fs::write(json_path, json_bytes)?;
    fs::write(proto_path, substrait_plan.encode_to_vec())?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let s3 = AmazonS3Builder::from_env()
        .with_bucket_name("serverless-analytics-tpch-west2")
        .with_region("us-west-2")
        .build()
        .expect("Failed to build S3 object store");

    let s3_url = Url::parse("s3://serverless-analytics-tpch-west2")
        .map_err(|e| DataFusionError::Execution(format!("Invalid S3 URL: {e}")))?;
    ctx.runtime_env()
        .register_object_store(&s3_url, Arc::new(s3));

    let orders_source_path = env_or_default(
        "ORDERS_SOURCE_PATH",
        "s3://serverless-analytics-tpch-west2/dataset/SF1-combined-metadata/velox_compatible/orders.parquet",
    );
    let lineitem_source_path = env_or_default(
        "LINEITEM_SOURCE_PATH",
        "s3://serverless-analytics-tpch-west2/dataset/SF1-combined-metadata/velox_compatible/lineitem.parquet",
    );

    ctx.register_parquet("orders", &orders_source_path, ParquetReadOptions::default())
        .await?;
    ctx.register_parquet(
        "lineitem",
        &lineitem_source_path,
        ParquetReadOptions::default(),
    )
    .await?;

    let hash_partition_count = parse_usize_env("HASH_PARTITION_COUNT", 8)?;

    let partitioned_orders = ctx
        .table("orders")
        .await?
        .repartition(Partitioning::Hash(
            vec![col("o_orderkey")],
            hash_partition_count,
        ))?
        .select(vec![
            lit("orders").alias("source_table"),
            col("o_orderkey").alias("join_key"),
        ])?;

    let partitioned_lineitem = ctx
        .table("lineitem")
        .await?
        .repartition(Partitioning::Hash(
            vec![col("l_orderkey")],
            hash_partition_count,
        ))?
        .select(vec![
            lit("lineitem").alias("source_table"),
            col("l_orderkey").alias("join_key"),
        ])?;

    let prepare_partitions_df = partitioned_orders.union(partitioned_lineitem)?;
    let prepare_partitions_plan = prepare_partitions_df.logical_plan().clone();
    let prepare_partitions_substrait_plan =
        to_substrait_plan(&prepare_partitions_plan, &ctx.state())?;

    let prepare_json_path = env_or_default(
        "SUBSTRAIT_PREP_PLAN_JSON_PATH",
        "/tmp/hash_join_prepare.substrait.json",
    );
    let prepare_proto_path = env_or_default(
        "SUBSTRAIT_PREP_PLAN_PROTO_PATH",
        "/tmp/hash_join_prepare.substrait.bin",
    );
    write_substrait_outputs(
        &prepare_partitions_substrait_plan,
        &prepare_json_path,
        &prepare_proto_path,
    )?;

    let orders_partitioned_path =
        env_or_default("ORDERS_PARTITIONED_PATH", &orders_source_path);
    let lineitem_partitioned_path =
        env_or_default("LINEITEM_PARTITIONED_PATH", &lineitem_source_path);

    ctx.register_parquet(
        "orders_partitions",
        &orders_partitioned_path,
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "lineitem_partitions",
        &lineitem_partitioned_path,
        ParquetReadOptions::default(),
    )
    .await?;

    let join_sql = "
        SELECT
            o.o_orderkey,
            o.o_custkey,
            l.l_partkey,
            l.l_quantity
        FROM orders_partitions o
        JOIN lineitem_partitions l
            ON o.o_orderkey = l.l_orderkey
    ";

    let read_and_join_plan = ctx.state().create_logical_plan(join_sql).await?;
    let read_and_join_substrait_plan =
        to_substrait_plan(&read_and_join_plan, &ctx.state())?;

    let join_json_path = env_or_default(
        "SUBSTRAIT_JOIN_PLAN_JSON_PATH",
        "/tmp/hash_join_read_join.substrait.json",
    );
    let join_proto_path = env_or_default(
        "SUBSTRAIT_JOIN_PLAN_PROTO_PATH",
        "/tmp/hash_join_read_join.substrait.bin",
    );
    write_substrait_outputs(
        &read_and_join_substrait_plan,
        &join_json_path,
        &join_proto_path,
    )?;

    println!("Wrote partition-prep Substrait JSON plan to {prepare_json_path}");
    println!("Wrote partition-prep Substrait protobuf plan to {prepare_proto_path}");
    println!("Wrote read-and-join Substrait JSON plan to {join_json_path}");
    println!("Wrote read-and-join Substrait protobuf plan to {join_proto_path}");

    Ok(())
}
