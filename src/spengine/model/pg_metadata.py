from dataclasses import dataclass


@dataclass
class PgUpdateMetadata:
    updated_values: list[str]  # list of key on the data that want to be updated {column: updated_value}
    condition: str = ""  # must be a valid sql condition
    condition_values: list[str] | None = None


@dataclass
class PgMetadata:
    table_name: str
    schema_name: str
    source: str
    exclude: list[str]
    update_metadata: PgUpdateMetadata | None = None
    mode: str = "insert"
