import yaml
from dataclasses import dataclass, field

class ConfigError(Exception):
    pass

@dataclass
class Config:
    spark: any
    env_name: str = "dev"
    target_catalog: str = "hive_metastore"
    target_database: str = "databricks_dw"
    target_tables: list = field(default_factory=list)
    source_filters: dict = field(default_factory=dict)
    yaml_uri: str = None
    shuffle_partitions: int = 800
    broadcast_threshold: int = 104857600
    salt_buckets: int = 8
    data_model_csv_path: str = None
    data_model_df: any = None

    def load(self):
        self.yaml_uri = self._get_conf("app.config.yaml_uri")
        if not self.yaml_uri:
            raise ConfigError("Spark conf 'app.config.yaml_uri' must be set to abfss://.../etl_config.yml")

        content = self._read_yaml(self.yaml_uri)
        self.env_name = content.get("env_name", self.env_name)
        self.target_catalog = content.get("target_catalog", self.target_catalog)
        self.target_database = content.get("target_database", self.target_database)
        self.target_tables = content.get("target_tables", [])
        self.source_filters = content.get("source_filters", {})
        self.data_model_csv_path = content.get("data_model_csv_path")
        settings = content.get("settings", {})
        self.shuffle_partitions = int(settings.get("shuffle_partitions", self.shuffle_partitions))
        self.broadcast_threshold = int(settings.get("broadcast_threshold", self.broadcast_threshold))
        self.salt_buckets = int(settings.get("salt_buckets", self.salt_buckets))

        # Load data model CSV for reference (optional usage in advanced mapping)
        if self.data_model_csv_path:
            self.data_model_df = self.spark.read.option("header", True).csv(self.data_model_csv_path)

    def apply_spark_confs(self):
        confs = {
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.autoBroadcastJoinThreshold": str(self.broadcast_threshold),
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
        }
        for k, v in confs.items():
            self.spark.conf.set(k, v)

    def _get_conf(self, key: str):
        try:
            return self.spark.conf.get(key)
        except Exception:
            return None

    def _read_yaml(self, path: str) -> dict:
        df = self.spark.read.text(path)
        content = "\n".join(r.value for r in df.collect())
        return yaml.safe_load(content) or {}