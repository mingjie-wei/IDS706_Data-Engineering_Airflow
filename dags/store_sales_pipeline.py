import logging
import pendulum
import pandas as pd

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    DoubleType,
    StringType,
)

# ----------------------------- Global Config -----------------------------
POSTGRES_CONN_ID = "postgres_main_db"
DATA_DIR = "/opt/airflow/data"

TABLE_FILES = {
    "stores_staging": f"{DATA_DIR}/stores.csv",
    "oil_staging": f"{DATA_DIR}/oil.csv",
    "transactions_staging": f"{DATA_DIR}/transactions.csv",
    "holidays_events_staging": f"{DATA_DIR}/holidays_events.csv",
    "train_staging": f"{DATA_DIR}/train.csv",
}

FINAL_TABLE_NAME = "sales_features_mart"

# Spark tunables (for Docker with moderate RAM)
SPARK_DRIVER_MEM = "8g"
SPARK_SHUFFLE_PARTS = "200"
SPARK_MAX_RESULT = "2g"
SPARK_MAX_TO_STRING = "200"
SPARK_TIMEZONE = "UTC"

# Ingest / transform repartition knobs
TRAIN_INGEST_REPARTS = 12
TRAIN_XFORM_REPARTS = 96

# JDBC tuning
JDBC_FETCH_SIZE = "10000"
JDBC_BATCH_SIZE = "5000"


def _spark_builder(app_name: str) -> SparkSession:
    """Create a SparkSession with consistent tuning for this project."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", SPARK_DRIVER_MEM)
        .config("spark.driver.maxResultSize", SPARK_MAX_RESULT)
        .config("spark.sql.session.timeZone", SPARK_TIMEZONE)
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTS)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.debug.maxToStringFields", SPARK_MAX_TO_STRING)
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0")
        # Make CSV reading more parallel friendly
        .config("spark.sql.files.maxPartitionBytes", "64m")
        .config("spark.sql.files.openCostInBytes", "8m")
        .getOrCreate()
    )


@dag(
    dag_id="store_sales_pipeline",
    start_date=pendulum.datetime(2012, 3, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["sales", "pyspark", "etl"],
)
def store_sales_pipeline():
    """Store Sales: ingest (parallel) -> spark transform -> quick model -> cleanup."""
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ========================= Parallel Ingest =========================
    def _schemas():
        """Define explicit CSV schemas to avoid type drift."""
        train_schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("date", StringType(), True),
                StructField("store_nbr", IntegerType(), True),
                StructField("family", StringType(), True),
                StructField("sales", DoubleType(), True),
                # keep as integer (0/1) for modeling
                StructField("onpromotion", IntegerType(), True),
            ]
        )
        transactions_schema = StructType(
            [
                StructField("date", StringType(), True),
                StructField("store_nbr", IntegerType(), True),
                StructField("transactions", IntegerType(), True),
            ]
        )
        oil_schema = StructType(
            [
                StructField("date", StringType(), True),
                StructField("dcoilwtico", DoubleType(), True),
            ]
        )
        stores_schema = StructType(
            [
                StructField("store_nbr", IntegerType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("type", StringType(), True),
                StructField("cluster", IntegerType(), True),
            ]
        )
        holidays_schema = StructType(
            [
                StructField("date", StringType(), True),
                StructField("type", StringType(), True),
                StructField("locale", StringType(), True),
                StructField("locale_name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("transferred", StringType(), True),
            ]
        )
        return {
            "train_staging": train_schema,
            "transactions_staging": transactions_schema,
            "oil_staging": oil_schema,
            "stores_staging": stores_schema,
            "holidays_events_staging": holidays_schema,
        }

    def _read_csv_with_schema(spark: SparkSession, path: str, schema: StructType):
        """Helper: read a CSV with header and provided schema."""
        return (
            spark.read.option("header", True)
            .option("escape", '"')
            .option("multiLine", False)
            .schema(schema)
            .csv(path)
        )

    @task
    def spark_ingest_one(table_name: str, csv_path: str):
        """
        Read ONE CSV with Spark using the explicit schema for that table,
        normalize minimal types, then write to Postgres staging via JDBC.
        """
        logging.info("Ingesting %s from %s", table_name, csv_path)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_connection(POSTGRES_CONN_ID)
        jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"
        jdbc_props = {
            "user": conn.login,
            "password": conn.password,
            "driver": "org.postgresql.Driver",
            "fetchsize": JDBC_FETCH_SIZE,
        }

        spark = _spark_builder(f"Ingest::{table_name}")
        schema_map = _schemas()
        schema = schema_map[table_name]

        df = _read_csv_with_schema(spark, csv_path, schema)

        # Minimal normalization per table
        if table_name == "train_staging":
            df = (
                df.withColumn("date", F.to_date("date"))
                .withColumn("store_nbr", F.col("store_nbr").cast("int"))
                .withColumn("sales", F.col("sales").cast("double"))
                .withColumn(
                    "onpromotion",
                    F.coalesce(F.col("onpromotion").cast("int"), F.lit(0)),
                )
                .select("id", "date", "store_nbr", "family", "sales", "onpromotion")
                .repartition(TRAIN_INGEST_REPARTS, "date")
            )
        elif table_name == "transactions_staging":
            df = (
                df.withColumn("date", F.to_date("date"))
                .withColumn("store_nbr", F.col("store_nbr").cast("int"))
                .withColumn("transactions", F.col("transactions").cast("int"))
            )
        elif table_name == "oil_staging":
            df = df.withColumn("date", F.to_date("date")).withColumn(
                "dcoilwtico", F.col("dcoilwtico").cast("double")
            )
        elif table_name == "stores_staging":
            df = df.withColumn("store_nbr", F.col("store_nbr").cast("int")).withColumn(
                "cluster", F.col("cluster").cast("int")
            )
        elif table_name == "holidays_events_staging":
            df = df.withColumn("date", F.to_date("date"))

        # JDBC write
        (
            df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", jdbc_props["user"])
            .option("password", jdbc_props["password"])
            .option("driver", jdbc_props["driver"])
            .option("batchsize", JDBC_BATCH_SIZE)
            .mode("overwrite")
            .save()
        )
        logging.info("Wrote %s", table_name)
        spark.stop()
        return table_name  # small XCom is fine

    @task_group(group_id="ingest_data")
    def ingest_data():
        """Create 5 parallel ingest tasks (one per table)."""
        t_stores = spark_ingest_one.override(task_id="ingest_stores")(
            "stores_staging", TABLE_FILES["stores_staging"]
        )
        t_oil = spark_ingest_one.override(task_id="ingest_oil")(
            "oil_staging", TABLE_FILES["oil_staging"]
        )
        t_txn = spark_ingest_one.override(task_id="ingest_transactions")(
            "transactions_staging", TABLE_FILES["transactions_staging"]
        )
        t_hol = spark_ingest_one.override(task_id="ingest_holidays")(
            "holidays_events_staging", TABLE_FILES["holidays_events_staging"]
        )
        t_train = spark_ingest_one.override(task_id="ingest_train")(
            "train_staging", TABLE_FILES["train_staging"]
        )
        # tasks return implicitly; parallelism controlled by executor/queues

    # ========================= Transform =========================
    @task
    def transform_with_spark():
        """Build a feature mart with memory-safe joins, broadcast dimensions, and partitioned JDBC write."""
        logging.info("Spark transformation started")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_connection(POSTGRES_CONN_ID)
        jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"
        jdbc_props = {
            "user": conn.login,
            "password": conn.password,
            "driver": "org.postgresql.Driver",
            "fetchsize": JDBC_FETCH_SIZE,
        }

        spark = _spark_builder("StoreSalesTransformation")

        # Read staging via JDBC
        stores_df = spark.read.jdbc(jdbc_url, "stores_staging", properties=jdbc_props)
        oil_df = spark.read.jdbc(jdbc_url, "oil_staging", properties=jdbc_props)
        transactions_df = spark.read.jdbc(
            jdbc_url, "transactions_staging", properties=jdbc_props
        )
        train_df = spark.read.jdbc(jdbc_url, "train_staging", properties=jdbc_props)
        holidays_df = spark.read.jdbc(
            jdbc_url, "holidays_events_staging", properties=jdbc_props
        )

        # Ensure types and prune columns early
        train_df = train_df.withColumn("date", F.to_date("date")).select(
            "id", "date", "store_nbr", "family", "sales", "onpromotion"
        )
        transactions_df = transactions_df.withColumn("date", F.to_date("date")).select(
            "date", "store_nbr", "transactions"
        )
        oil_df = oil_df.withColumn("date", F.to_date("date")).select(
            "date", "dcoilwtico"
        )
        stores_df = stores_df.select("store_nbr", "city", "state", "type", "cluster")
        holidays_df = holidays_df.withColumn("date", F.to_date("date")).select(
            "date", "type", "locale", "locale_name"
        )

        # Repartition the big fact table to avoid single-partition pressure
        train_df = train_df.repartition(TRAIN_XFORM_REPARTS, "store_nbr", "date")

        # Oil forward/backward fill
        w_ffill = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
        w_bfill = Window.orderBy("date").rowsBetween(0, Window.unboundedFollowing)
        oil_df = oil_df.withColumn(
            "dcoilwtico", F.last("dcoilwtico", True).over(w_ffill)
        ).withColumn("dcoilwtico", F.first("dcoilwtico", True).over(w_bfill))

        # Holiday flags (broadcastable)
        national_holidays = (
            holidays_df.filter(F.col("locale") == "National")
            .select(
                "date",
                F.lit(True).alias("is_national_holiday"),
                F.col("type").alias("holiday_type"),
            )
            .distinct()
        )
        regional_holidays = (
            holidays_df.filter(F.col("locale") == "Regional")
            .select(
                "date",
                F.col("locale_name").alias("state"),
                F.lit(True).alias("is_regional_holiday"),
            )
            .distinct()
        )
        local_holidays = (
            holidays_df.filter(F.col("locale") == "Local")
            .select(
                "date",
                F.col("locale_name").alias("city"),
                F.lit(True).alias("is_local_holiday"),
            )
            .distinct()
        )

        # Broadcast small tables
        stores_b = F.broadcast(stores_df)
        nat_b = F.broadcast(national_holidays)
        reg_b = F.broadcast(regional_holidays)
        loc_b = F.broadcast(local_holidays)
        oil_b = F.broadcast(oil_df)

        # Join
        df = (
            train_df.join(stores_b, "store_nbr", "left")
            .join(transactions_df, ["date", "store_nbr"], "left")
            .join(oil_b, "date", "left")
            .join(nat_b, "date", "left")
            .join(reg_b, ["date", "state"], "left")
            .join(loc_b, ["date", "city"], "left")
        )

        # Calendar features
        df = (
            df.withColumn("day_of_week", F.dayofweek("date"))
            .withColumn("month", F.month("date"))
            .withColumn("year", F.year("date"))
        )

        # Final feature mart
        final_df = (
            df.select(
                "id",
                "date",
                "store_nbr",
                "family",
                "onpromotion",
                "sales",
                F.col("type").alias("store_type"),
                "cluster",
                F.col("transactions").alias("store_transactions"),
                F.col("dcoilwtico").alias("oil_price"),
                "day_of_week",
                "month",
                "year",
                "is_national_holiday",
                "holiday_type",
                "is_regional_holiday",
                "is_local_holiday",
            )
            .na.fill(
                {
                    "store_transactions": 0,
                    "is_national_holiday": False,
                    "is_regional_holiday": False,
                    "is_local_holiday": False,
                    "holiday_type": "None",
                }
            )
            .repartition(16, "store_nbr")  # parallelize JDBC flush
        )

        logging.info(f"Final schema for {FINAL_TABLE_NAME}:")
        final_df.printSchema()

        # JDBC write (partitioned, batched)
        (
            final_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", FINAL_TABLE_NAME)
            .option("user", jdbc_props["user"])
            .option("password", jdbc_props["password"])
            .option("driver", jdbc_props["driver"])
            .option("batchsize", JDBC_BATCH_SIZE)
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )

        spark.stop()
        logging.info("Spark transformation done")

    @task_group(group_id="process_with_spark")
    def process_with_spark():
        transform_with_spark()

    # ========================= Quick Model =========================
    @task
    def train_simple_model():
        """
        Train a quick RandomForest model, log metrics, persist run info,
        and save visualizations to /opt/airflow/artifacts.
        """
        import os, json, joblib
        from datetime import datetime
        import numpy as np
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        from sklearn.model_selection import train_test_split
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.metrics import mean_squared_error, r2_score

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        sql = f"""
            SELECT sales, onpromotion, "cluster", oil_price, day_of_week, month,
                   store_transactions, is_national_holiday, is_regional_holiday, is_local_holiday,
                   family
            FROM {FINAL_TABLE_NAME}
            ORDER BY random()
            LIMIT 80000;
        """
        df = pd.read_sql(sql, engine)
        if df.empty:
            logging.warning("No data for training. Skip.")
            return None

        df = pd.get_dummies(
            df, columns=["cluster", "day_of_week", "month", "family"], drop_first=True
        )

        X = df.drop("sales", axis=1)
        y = np.log1p(df["sales"])

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        params = dict(n_estimators=200, max_depth=16, n_jobs=-1, random_state=42)
        model = RandomForestRegressor(**params).fit(X_train, y_train)
        y_pred = model.predict(X_test)

        rmse_exp = float(
            np.sqrt(mean_squared_error(np.expm1(y_test), np.expm1(y_pred)))
        )
        r2 = float(r2_score(y_test, y_pred))

        logging.info(
            "Samples: train=%d, test=%d, features=%d",
            len(X_train),
            len(X_test),
            X.shape[1],
        )
        logging.info("Metrics: RMSE(exp m1)=%.4f, R2=%.4f", rmse_exp, r2)

        # Persist metadata
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS model_run_history(
                        run_id TEXT PRIMARY KEY,
                        ts TIMESTAMP,
                        model_name TEXT,
                        params_json JSONB,
                        n_train INT,
                        n_test INT,
                        rmse DOUBLE PRECISION,
                        r2 DOUBLE PRECISION
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS model_feature_importance(
                        run_id TEXT,
                        feature TEXT,
                        importance DOUBLE PRECISION
                    );
                    """
                )
                run_id = f"rf_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
                cur.execute(
                    """INSERT INTO model_run_history(run_id, ts, model_name, params_json,
                                                     n_train, n_test, rmse, r2)
                       VALUES (%s, NOW(), %s, %s::jsonb, %s, %s, %s, %s)""",
                    (
                        run_id,
                        "RandomForestRegressor",
                        json.dumps(params),
                        len(X_train),
                        len(X_test),
                        rmse_exp,
                        r2,
                    ),
                )

                importances = model.feature_importances_
                cols = list(X.columns)
                rows = [
                    (run_id, cols[i], float(importances[i])) for i in range(len(cols))
                ]
                if rows:
                    args = ",".join(cur.mogrify("(%s,%s,%s)", r).decode() for r in rows)
                    cur.execute(
                        "INSERT INTO model_feature_importance(run_id, feature, importance) VALUES "
                        + args
                    )

        # Save artifacts (model + plots)
        art_dir = "/opt/airflow/artifacts"
        os.makedirs(art_dir, exist_ok=True)

        model_path = f"{art_dir}/{run_id}.joblib"
        joblib.dump(model, model_path)

        # Feature importance (top 30)
        feat_imp = pd.Series(model.feature_importances_, index=X.columns).sort_values(
            ascending=False
        )
        topk = min(30, len(feat_imp))
        plt.figure(figsize=(10, 8))
        feat_imp.head(topk)[::-1].plot(kind="barh")
        plt.title(f"Top {topk} Feature Importances — {run_id}")
        plt.xlabel("Importance")
        plt.tight_layout()
        feat_png = f"{art_dir}/{run_id}_featimp_top{topk}.png"
        plt.savefig(feat_png, dpi=160)
        plt.close()

        # Residuals vs Fitted (original sales scale)
        fitted = np.expm1(y_pred)
        actual = np.expm1(y_test)
        residuals = actual - fitted
        plt.figure(figsize=(8, 6))
        plt.scatter(fitted, residuals, s=8, alpha=0.5)
        plt.axhline(0.0, linestyle="--")
        plt.title(f"Residuals vs Fitted — {run_id}")
        plt.xlabel("Fitted (sales)")
        plt.ylabel("Residuals (actual - fitted)")
        plt.tight_layout()
        resid_png = f"{art_dir}/{run_id}_residuals.png"
        plt.savefig(resid_png, dpi=160)
        plt.close()

        logging.info(
            "Saved artifacts:\n  %s\n  %s\n  %s", model_path, feat_png, resid_png
        )
        logging.info("Training done (run_id=%s).", run_id)
        return run_id

    @task_group(group_id="train_model")
    def train_model():
        train_simple_model()

    # ========================= Cleanup =========================
    @task
    def cleanup_staging_tables():
        """Drop staging tables after a successful run."""
        logging.info("Dropping staging tables...")
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for table_name in TABLE_FILES.keys():
                    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
        logging.info("Cleanup done")

    @task_group(group_id="cleanup_staging")
    def cleanup_staging():
        cleanup_staging_tables()

    # ========================= Orchestration =========================
    ingestion_group = ingest_data()  # 5 parallel ingest tasks
    spark_group = process_with_spark()  # single transform after all ingests
    analysis_group = train_model()  # simple model after transform
    cleanup_group = cleanup_staging()  # drop staging at the end

    start >> ingestion_group >> spark_group >> analysis_group >> cleanup_group >> end


store_sales_pipeline()
