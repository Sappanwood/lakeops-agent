import copy
import json
from pathlib import Path
import unittest

from data.catalog.validator import (
    CatalogValidationError,
    canonical_metadata_json,
    validate_catalog,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = REPO_ROOT / "data" / "catalog" / "catalog.json"


class CatalogValidatorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.complete_catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))

    def test_complete_contract_produces_stable_consumer_metadata(self) -> None:
        metadata = validate_catalog(self.complete_catalog)
        reordered = copy.deepcopy(self.complete_catalog)
        reordered["sources"].reverse()
        reordered["datasets"].reverse()
        reordered["volume_profiles"].reverse()
        reordered["joins"].reverse()
        reordered["kpis"].reverse()
        reordered["business_terms"].reverse()
        reordered["query_surface"]["views"].reverse()

        self.assertEqual(canonical_metadata_json(metadata), canonical_metadata_json(validate_catalog(reordered)))
        self.assertEqual(metadata["schema"], "lakeops/catalog@1")
        self.assertEqual(metadata["datasets"][0]["id"], "ingestion_freshness")
        self.assertEqual(metadata["query_surface"]["views"][0], "v_ingestion_freshness")

    def test_consumer_metadata_contains_every_validated_contract_surface(self) -> None:
        metadata = validate_catalog(self.complete_catalog)

        self.assertEqual(metadata["time_domain"], self.complete_catalog["time_domain"])
        self.assertIn("daily_batch", metadata["sources"][0])
        self.assertIn("endpoint", metadata["sources"][1])
        self.assertEqual(metadata["datasets"][0]["sensitivity"], "public")
        self.assertIn("freshness", metadata["datasets"][0])
        self.assertEqual(len(metadata["joins"]), 3)
        self.assertEqual(len(metadata["kpis"]), 4)
        self.assertEqual(len(metadata["business_terms"]), 5)
        self.assertTrue(metadata["publication"]["immutable_objects"])
        self.assertIn("forbidden_persisted_stream_fields", metadata["privacy"])
        self.assertEqual(
            metadata["query_surface"]["view_contracts"]["v_pipeline_runs"]["inputs"],
            ["pipeline_runs"],
        )
        self.assertEqual(metadata["primary_scenario"]["remediation"]["operation"], "backfill_hour")

    def test_rejects_invalid_contract_shapes_with_explicit_codes(self) -> None:
        cases = (
            ("missing field", self._without_schema_type, "missing_field"),
            ("incompatible type", self._unsupported_field_type, "incompatible_type"),
            ("non-string type", self._non_string_field_type, "incompatible_type"),
            ("invalid join", self._unknown_join_field, "invalid_join_reference"),
            ("incompatible join types", self._incompatible_join_types, "invalid_join"),
            ("invalid join cardinality type", self._non_string_join_cardinality, "invalid_join"),
            ("duplicate identifier", self._duplicate_dataset_id, "duplicate_identifier"),
            ("cross-collection duplicate", self._source_dataset_id_collision, "duplicate_identifier"),
            ("undocumented view", self._missing_view_contract, "undocumented_logical_view"),
            ("invalid partition", self._unknown_partition_key, "invalid_partition_metadata"),
            ("nullable partition", self._nullable_partition_key, "invalid_partition_metadata"),
            ("missing security section", self._missing_privacy_rule, "missing_field"),
            ("missing top-level section", self._missing_storage, "missing_field"),
            ("missing source acquisition", self._missing_source_acquisition, "missing_field"),
            ("missing source file schema", self._missing_source_file_schema, "missing_field"),
            ("empty source identifier", self._empty_source_id, "invalid_identifier"),
            ("duplicate KPI", self._duplicate_kpi_id, "duplicate_identifier"),
            ("duplicate volume profile", self._duplicate_volume_profile_id, "duplicate_identifier"),
            ("duplicate join", self._duplicate_join_id, "duplicate_identifier"),
            ("empty primary key", self._empty_primary_key, "invalid_key"),
            ("view field outside inputs", self._view_field_outside_inputs, "undocumented_logical_view"),
            ("unproven join cardinality", self._unproven_join_cardinality, "invalid_join"),
            ("referenced self join", self._referenced_self_join, "invalid_join"),
            ("non-string view contract key", self._non_string_view_contract_key, "undocumented_logical_view"),
        )

        for name, mutator, code in cases:
            with self.subTest(name=name):
                catalog = copy.deepcopy(self.complete_catalog)
                mutator(catalog)
                with self.assertRaisesRegex(CatalogValidationError, rf"^\[{code}\]"):
                    validate_catalog(catalog)

    def test_rejects_obsolete_or_unknown_contract_versions_without_coercion(self) -> None:
        for schema, catalog_version, code in (
            ("lakeops/catalog@0", "2.0.0", "unsupported_contract_schema"),
            ("lakeops/catalog@1", "0.9.0", "unsupported_catalog_version"),
            ("lakeops/catalog@1", 2, "invalid_catalog_version"),
        ):
            with self.subTest(schema=schema, catalog_version=catalog_version):
                catalog = copy.deepcopy(self.complete_catalog)
                catalog["schema"] = schema
                catalog["catalog_version"] = catalog_version
                with self.assertRaisesRegex(CatalogValidationError, rf"^\[{code}\]"):
                    validate_catalog(catalog)

    @staticmethod
    def _without_schema_type(catalog: dict) -> None:
        del catalog["datasets"][0]["schema"][0]["type"]

    @staticmethod
    def _unsupported_field_type(catalog: dict) -> None:
        catalog["datasets"][0]["schema"][0]["type"] = "decimal"

    @staticmethod
    def _non_string_field_type(catalog: dict) -> None:
        catalog["datasets"][0]["schema"][0]["type"] = ["string"]

    @staticmethod
    def _unknown_join_field(catalog: dict) -> None:
        catalog["joins"][0]["left"] = (
            "pageviews_hourly.unknown_field,"
            "pageviews_hourly.page_title,"
            "pageviews_hourly.window_end"
        )

    @staticmethod
    def _incompatible_join_types(catalog: dict) -> None:
        catalog["joins"][0]["right"] = (
            "page_activity_hourly.project_code,"
            "page_activity_hourly.page_title,"
            "page_activity_hourly.change_count"
        )

    @staticmethod
    def _non_string_join_cardinality(catalog: dict) -> None:
        catalog["joins"][0]["cardinality"] = ["one_to_one"]

    @staticmethod
    def _duplicate_dataset_id(catalog: dict) -> None:
        catalog["datasets"][1]["id"] = catalog["datasets"][0]["id"]

    @staticmethod
    def _source_dataset_id_collision(catalog: dict) -> None:
        catalog["datasets"][0]["id"] = catalog["sources"][0]["id"]

    @staticmethod
    def _missing_view_contract(catalog: dict) -> None:
        catalog["query_surface"]["views"].append("v_missing_contract")

    @staticmethod
    def _unknown_partition_key(catalog: dict) -> None:
        catalog["datasets"][0]["partition_keys"] = ["missing_partition"]

    @staticmethod
    def _nullable_partition_key(catalog: dict) -> None:
        field = next(
            definition
            for definition in catalog["datasets"][0]["schema"]
            if definition["name"] == "partition_date"
        )
        field["nullable"] = True

    @staticmethod
    def _missing_privacy_rule(catalog: dict) -> None:
        del catalog["privacy"]["rule"]

    @staticmethod
    def _missing_storage(catalog: dict) -> None:
        del catalog["storage"]

    @staticmethod
    def _missing_source_acquisition(catalog: dict) -> None:
        del catalog["sources"][0]["daily_batch"]

    @staticmethod
    def _missing_source_file_schema(catalog: dict) -> None:
        del catalog["sources"][0]["file_schema"]

    @staticmethod
    def _empty_source_id(catalog: dict) -> None:
        catalog["sources"][0]["id"] = ""

    @staticmethod
    def _duplicate_kpi_id(catalog: dict) -> None:
        catalog["kpis"][1]["id"] = catalog["kpis"][0]["id"]

    @staticmethod
    def _duplicate_volume_profile_id(catalog: dict) -> None:
        catalog["volume_profiles"][1]["id"] = catalog["volume_profiles"][0]["id"]

    @staticmethod
    def _duplicate_join_id(catalog: dict) -> None:
        catalog["joins"][1]["id"] = catalog["joins"][0]["id"]

    @staticmethod
    def _empty_primary_key(catalog: dict) -> None:
        catalog["datasets"][0]["primary_key"] = []

    @staticmethod
    def _view_field_outside_inputs(catalog: dict) -> None:
        fields = catalog["query_surface"]["view_contracts"]["v_pipeline_runs"]["fields"]
        fields[0] = "project_code"

    @staticmethod
    def _unproven_join_cardinality(catalog: dict) -> None:
        catalog["joins"][1]["right"] = "source_partitions.logical_partition"
        catalog["joins"][1]["left"] = "pipeline_runs.logical_partition"

    @staticmethod
    def _referenced_self_join(catalog: dict) -> None:
        catalog["joins"][0]["right"] = catalog["joins"][0]["left"]
        catalog["query_surface"]["view_contracts"]["v_page_traffic_activity"]["joins"] = [
            catalog["joins"][0]["id"]
        ]

    @staticmethod
    def _non_string_view_contract_key(catalog: dict) -> None:
        catalog["query_surface"]["view_contracts"][7] = {
            "inputs": ["pipeline_runs"],
            "joins": [],
            "fields": ["run_id"],
        }


if __name__ == "__main__":
    unittest.main()
