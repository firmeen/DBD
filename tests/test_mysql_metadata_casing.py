import importlib

import pytest

from dbd.storage.mysql.repositories.fabric_worker_repository import _canonical_metadata


migration = importlib.import_module(
    "dbd.storage.mysql.alembic.versions.0012_run9_data_repair"
)


@pytest.mark.parametrize(
    "source",
    [
        {
            "DATA_TYPE": "varchar",
            "COLUMN_TYPE": "varchar(500)",
            "CHARACTER_MAXIMUM_LENGTH": 500,
            "CHARACTER_OCTET_LENGTH": 2000,
            "IS_NULLABLE": "YES",
            "CHARACTER_SET_NAME": "utf8mb4",
            "COLLATION_NAME": "utf8mb4_0900_ai_ci",
            "COLUMN_DEFAULT": None,
            "EXTRA": "",
            "COLUMN_COMMENT": "",
        },
        {
            "data_type": "varchar",
            "column_type": "varchar(500)",
            "character_maximum_length": 500,
            "character_octet_length": 2000,
            "is_nullable": "YES",
            "character_set_name": "utf8mb4",
            "collation_name": "utf8mb4_0900_ai_ci",
            "column_default": None,
            "extra": "",
            "column_comment": "",
        },
        {
            "Data_Type": "varchar",
            "Column_Type": "varchar(500)",
            "Character_Maximum_Length": 500,
            "Character_Octet_Length": 2000,
            "Is_Nullable": "YES",
            "Character_Set_Name": "utf8mb4",
            "Collation_Name": "utf8mb4_0900_ai_ci",
            "Column_Default": None,
            "Extra": "",
            "Column_Comment": "",
        },
    ],
)
def test_migration_metadata_is_case_insensitive(source):
    metadata = migration._canonical_metadata(source)

    assert metadata["data_type"] == "varchar"
    assert metadata["column_type"] == "varchar(500)"
    assert metadata["character_maximum_length"] == 500
    assert metadata["character_octet_length"] == 2000
    assert metadata["is_nullable"] == "YES"
    assert metadata["character_set_name"] == "utf8mb4"
    assert metadata["collation_name"] == "utf8mb4_0900_ai_ci"


@pytest.mark.parametrize(
    "source",
    [
        {
            "DATA_TYPE": "text",
            "CHARACTER_MAXIMUM_LENGTH": 65535,
            "CHARACTER_OCTET_LENGTH": 65535,
        },
        {
            "data_type": "text",
            "character_maximum_length": 65535,
            "character_octet_length": 65535,
        },
        {
            "Data_Type": "text",
            "Character_Maximum_Length": 65535,
            "Character_Octet_Length": 65535,
        },
    ],
)
def test_runtime_metadata_is_case_insensitive(source):
    metadata = _canonical_metadata(source)

    assert metadata == {
        "data_type": "text",
        "character_maximum_length": 65535,
        "character_octet_length": 65535,
    }


@pytest.mark.parametrize(
    "normalizer,error",
    [
        (migration._canonical_metadata, "refusing automatic migration"),
        (_canonical_metadata, "refusing automatic repair"),
    ],
)
def test_metadata_normalization_fails_closed_on_ambiguous_labels(normalizer, error):
    with pytest.raises(RuntimeError, match=error):
        normalizer({"DATA_TYPE": "varchar", "data_type": "text"})


def test_metadata_normalization_preserves_missing_required_key_behavior():
    metadata = _canonical_metadata({"CHARACTER_MAXIMUM_LENGTH": 500})

    assert "data_type" not in metadata
