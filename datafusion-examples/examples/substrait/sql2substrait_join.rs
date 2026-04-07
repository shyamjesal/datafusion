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

const ORDERS_SOURCE_FILE: &str = "orders.parquet";
const LINEITEM_SOURCE_FILE: &str = "lineitem.parquet";

const ORDERS_PARTITIONED_FILE: &str = "orders_partitions.parquet";
const LINEITEM_PARTITIONED_FILE: &str = "lineitem_partitions.parquet";

const ORDERS_PREP_PLAN_JSON_FILE: &str = "orders_hash_partition_prepare.substrait.json";
const ORDERS_PREP_PLAN_PROTO_FILE: &str = "orders_hash_partition_prepare.substrait.bin";
const LINEITEM_PREP_PLAN_JSON_FILE: &str =
    "lineitem_hash_partition_prepare.substrait.json";
const LINEITEM_PREP_PLAN_PROTO_FILE: &str =
    "lineitem_hash_partition_prepare.substrait.bin";
const JOIN_PLAN_JSON_FILE: &str = "hash_join_read_join.substrait.json";
const JOIN_PLAN_PROTO_FILE: &str = "hash_join_read_join.substrait.bin";

fn env_or_default(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn join_path(dir: &str, file_name: &str) -> String {
    format!("{}/{}", dir.trim_end_matches('/'), file_name)
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

    let source_data_dir = env_or_default(
        "SOURCE_DATA_DIR",
        "s3://serverless-analytics-tpch-west2/dataset/SF1-combined-metadata/velox_compatible",
    );
    let orders_source_path = join_path(&source_data_dir, ORDERS_SOURCE_FILE);
    let lineitem_source_path = join_path(&source_data_dir, LINEITEM_SOURCE_FILE);

    ctx.register_parquet("orders", &orders_source_path, ParquetReadOptions::default())
        .await?;
    ctx.register_parquet(
        "lineitem",
        &lineitem_source_path,
        ParquetReadOptions::default(),
    )
    .await?;

    let hash_partition_count = parse_usize_env("HASH_PARTITION_COUNT", 8)?;

    let partitioned_orders = ctx.table("orders").await?.repartition(
        Partitioning::Hash(vec![col("o_orderkey")], hash_partition_count),
    )?;
    let partitioned_orders_plan = partitioned_orders.logical_plan().clone();
    let partitioned_orders_substrait_plan =
        to_substrait_plan(&partitioned_orders_plan, &ctx.state())?;

    let substrait_plan_dir = env_or_default("SUBSTRAIT_PLAN_DIR", "/tmp");
    let orders_prepare_json_path =
        join_path(&substrait_plan_dir, ORDERS_PREP_PLAN_JSON_FILE);
    let orders_prepare_proto_path =
        join_path(&substrait_plan_dir, ORDERS_PREP_PLAN_PROTO_FILE);
    write_substrait_outputs(
        &partitioned_orders_substrait_plan,
        &orders_prepare_json_path,
        &orders_prepare_proto_path,
    )?;

    let partitioned_lineitem =
        ctx.table("lineitem")
            .await?
            .repartition(Partitioning::Hash(
                vec![col("l_orderkey")],
                hash_partition_count,
            ))?;
    let partitioned_lineitem_plan = partitioned_lineitem.logical_plan().clone();
    let partitioned_lineitem_substrait_plan =
        to_substrait_plan(&partitioned_lineitem_plan, &ctx.state())?;

    let lineitem_prepare_json_path =
        join_path(&substrait_plan_dir, LINEITEM_PREP_PLAN_JSON_FILE);
    let lineitem_prepare_proto_path =
        join_path(&substrait_plan_dir, LINEITEM_PREP_PLAN_PROTO_FILE);
    write_substrait_outputs(
        &partitioned_lineitem_substrait_plan,
        &lineitem_prepare_json_path,
        &lineitem_prepare_proto_path,
    )?;

    let partitioned_data_dir = env_or_default("PARTITIONED_DATA_DIR", "/tmp");
    let orders_partitioned_path =
        join_path(&partitioned_data_dir, ORDERS_PARTITIONED_FILE);
    let lineitem_partitioned_path =
        join_path(&partitioned_data_dir, LINEITEM_PARTITIONED_FILE);

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

    let join_json_path = join_path(&substrait_plan_dir, JOIN_PLAN_JSON_FILE);
    let join_proto_path = join_path(&substrait_plan_dir, JOIN_PLAN_PROTO_FILE);
    write_substrait_outputs(
        &read_and_join_substrait_plan,
        &join_json_path,
        &join_proto_path,
    )?;

    println!(
        "Wrote orders partition-prep Substrait JSON plan to {orders_prepare_json_path}"
    );
    println!(
        "Wrote orders partition-prep Substrait protobuf plan to {orders_prepare_proto_path}"
    );
    println!(
        "Wrote lineitem partition-prep Substrait JSON plan to {lineitem_prepare_json_path}"
    );
    println!(
        "Wrote lineitem partition-prep Substrait protobuf plan to {lineitem_prepare_proto_path}"
    );
    println!("Wrote read-and-join Substrait JSON plan to {join_json_path}");
    println!("Wrote read-and-join Substrait protobuf plan to {join_proto_path}");

    Ok(())
}
