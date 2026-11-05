import datetime
from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
    "email": "ynnhi1508@gmail.com",
    "email_on_failure": True
    }

with DAG(
    "Spark_Batch_Job",
    start_date=datetime.datetime(2021, 1, 1),
    default_args=default_args,
    schedule="0 15 * * *",
    catchup=False,
) as dag:

    bronze_layer_load = SparkSubmitOperator(
        task_id="bronze_layer_load",
        conn_id="spark",
        application="src/batch/bronze_dimension_fact_load.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.openlineage:openlineage-spark_2.12:1.25.0",
        jars="jars/mysql-connector-j-8.0.33.jar",
        # conf={
        #     "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        #     "spark.openlineage.transport.type": "http",
        #     "spark.openlineage.transport.url": "http://marquez:5000",
        #     "spark.openlineage.namespace": "coffee_shop_spark_instance",
        #     "spark.openlineage.appName": "bronze_load"
        # }
    )

    bronze_data_quality_check = SparkSubmitOperator(
        task_id="bronze_data_quality_check",
        conn_id="spark",
        application="src/batch/validation/bronze_validation.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.1",
    )

    silver_layer_dimension_transform = SparkSubmitOperator(
        task_id="silver_layer_dimension_transform",
        conn_id="spark",
        application="src/batch/silver_dimensions.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.1,io.openlineage:openlineage-spark_2.12:1.25.0",
        # conf={
        #     "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        #     "spark.openlineage.transport.type": "http",
        #     "spark.openlineage.transport.url": "http://marquez:5000",
        #     "spark.openlineage.namespace": "coffee_shop_spark_instance",
        #     "spark.openlineage.appName": "silver_dimension_load",
        #     "spark.openlineage.parentJobNamespace": "{{ macros.OpenLineageProviderPlugin.lineage_job_namespace() }}",
        #     "spark.openlineage.parentJobName": "{{ macros.OpenLineageProviderPlugin.lineage_job_name(task_instance) }}",
        #     "spark.openlineage.parentRunId": "{{ macros.OpenLineageProviderPlugin.lineage_run_id(task_instance) }}",
        # },
    )

    silver_layer_fact_transform = SparkSubmitOperator(
        task_id="silver_layer_fact_transform",
        conn_id="spark",
        application="src/batch/silver_facts.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.1,io.openlineage:openlineage-spark_2.12:1.25.0",
        # conf={
        #     "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        #     "spark.openlineage.transport.type": "http",
        #     "spark.openlineage.transport.url": "http://marquez:5000",
        #     "spark.openlineage.namespace": "coffee_shop_spark_instance",
        #     "spark.openlineage.appName": "silver_fact_load",
        #     "spark.openlineage.parentJobNamespace": "{{ macros.OpenLineageProviderPlugin.lineage_job_namespace() }}",
        #     "spark.openlineage.parentJobName": "{{ macros.OpenLineageProviderPlugin.lineage_job_name(task_instance) }}",
        #     "spark.openlineage.parentRunId": "{{ macros.OpenLineageProviderPlugin.lineage_run_id(task_instance) }}",
        # },
    )

    silver_data_quality_check = SparkSubmitOperator(
        task_id="silver_data_quality_check",
        conn_id="spark",
        application="src/batch/validation/silver_validation.py",
        env_vars={"SPARK_VERSION": "3.5.1"},
        packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazon.deequ:deequ:2.0.7-spark-3.5",
    )

    gold_layer_dim_payment_scd2 = SparkSubmitOperator(
        task_id="gold_layer_dim_payment_scd2",
        conn_id="spark",
        application="src/batch/gold_dim_payment.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.0.0,io.openlineage:openlineage-spark_2.12:1.25.0",
        repositories="https://repo1.maven.org/maven2/",
        # conf={
        #     "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        #     "spark.openlineage.transport.type": "http",
        #     "spark.openlineage.transport.url": "http://marquez:5000",
        #     "spark.openlineage.namespace": "coffee_shop_spark_instance",
        #     "spark.openlineage.appName": "gld_dim_payment_load",
        #     "spark.openlineage.parentJobNamespace": "{{ macros.OpenLineageProviderPlugin.lineage_job_namespace() }}",
        #     "spark.openlineage.parentJobName": "{{ macros.OpenLineageProviderPlugin.lineage_job_name(task_instance) }}",
        #     "spark.openlineage.parentRunId": "{{ macros.OpenLineageProviderPlugin.lineage_run_id(task_instance) }}",
        # },
    )

    gold_layer_dim_stores_scd2 = SparkSubmitOperator(
        task_id="gold_layer_dim_stores_scd2",
        conn_id="spark",
        application="src/batch/gold_dim_stores.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.0.0,io.openlineage:openlineage-spark_2.12:1.25.0",
        repositories="https://repo1.maven.org/maven2/",
        # conf={
        #     "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        #     "spark.openlineage.transport.type": "http",
        #     "spark.openlineage.transport.url": "http://marquez:5000",
        #     "spark.openlineage.namespace": "coffee_shop_spark_instance",
        #     "spark.openlineage.appName": "gld_dim_stores_load",
        #     "spark.openlineage.parentJobNamespace": "{{ macros.OpenLineageProviderPlugin.lineage_job_namespace() }}",
        #     "spark.openlineage.parentJobName": "{{ macros.OpenLineageProviderPlugin.lineage_job_name(task_instance) }}",
        #     "spark.openlineage.parentRunId": "{{ macros.OpenLineageProviderPlugin.lineage_run_id(task_instance) }}",
        # }
    )

    gold_layer_dim_products_scd2 = SparkSubmitOperator(
        task_id="gold_layer_dim_products_scd2",
        conn_id="spark",
        application="src/batch/gold_dim_products.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.0.0,io.openlineage:openlineage-spark_2.12:1.25.0",
        repositories="https://repo1.maven.org/maven2/",
        # conf={
        #     "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        #     "spark.openlineage.transport.type": "http",
        #     "spark.openlineage.transport.url": "http://marquez:5000",
        #     "spark.openlineage.namespace": "coffee_shop_spark_instance",
        #     "spark.openlineage.appName": "gld_dim_products_load",
        #     "spark.openlineage.parentJobNamespace": "{{ macros.OpenLineageProviderPlugin.lineage_job_namespace() }}",
        #     "spark.openlineage.parentJobName": "{{ macros.OpenLineageProviderPlugin.lineage_job_name(task_instance) }}",
        #     "spark.openlineage.parentRunId": "{{ macros.OpenLineageProviderPlugin.lineage_run_id(task_instance) }}",
        # },
    )

    gold_layer_fact_orders = SparkSubmitOperator(
        task_id="gold_layer_fact_orders",
        conn_id="spark",
        application="src/batch/gold_fact_orders.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.0.0,io.openlineage:openlineage-spark_2.12:1.25.0",
        repositories="https://repo1.maven.org/maven2/",
        # conf={
        #     "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        #     "spark.openlineage.transport.type": "http",
        #     "spark.openlineage.transport.url": "http://marquez:5000",
        #     "spark.openlineage.namespace": "coffee_shop_spark_instance",
        #     "spark.openlineage.appName": "gld_fact_order_load",
        #     "spark.openlineage.parentJobNamespace": "{{ macros.OpenLineageProviderPlugin.lineage_job_namespace() }}",
        #     "spark.openlineage.parentJobName": "{{ macros.OpenLineageProviderPlugin.lineage_job_name(task_instance) }}",
        #     "spark.openlineage.parentRunId": "{{ macros.OpenLineageProviderPlugin.lineage_run_id(task_instance) }}",
        # },
    )

    show_gold_layer_data = SparkSubmitOperator(
        task_id="show_gold_layer_data",
        conn_id="spark",
        application="src/batch/show_gold_tables.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.0.0",
    )

# --- DAG Dependencies ---
# Bronze → Bronze Quality Check
bronze_layer_load >> bronze_data_quality_check

# Bronze Quality Check → Silver Layer (Dimensions + Fact)
bronze_data_quality_check >> [
    silver_layer_dimension_transform,
    silver_layer_fact_transform,
]

# Silver Layers → Silver Quality Check
[
    silver_layer_dimension_transform,
    silver_layer_fact_transform,
] >> silver_data_quality_check

# Silver Quality Check → Gold Dimensions
silver_data_quality_check >> [
    gold_layer_dim_payment_scd2,
    gold_layer_dim_stores_scd2,
    gold_layer_dim_products_scd2,
]

# Gold Dimensions → Gold Fact
(
    [
        gold_layer_dim_payment_scd2,
        gold_layer_dim_stores_scd2,
        gold_layer_dim_products_scd2,
    ]
    >> gold_layer_fact_orders
    >> show_gold_layer_data
)
