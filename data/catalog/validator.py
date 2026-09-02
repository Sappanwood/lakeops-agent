"""Strict validation and deterministic consumer metadata for LakeOps catalogs."""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Mapping, Sequence
from typing import Any


SUPPORTED_SCHEMA = "lakeops/catalog@1"
SUPPORTED_CATALOG_MAJOR = 2
ALLOWED_TYPES = frozenset({"string", "integer", "boolean", "timestamp", "date", "array_string"})
ALLOWED_STAGES = frozenset({"silver", "gold"})
PARTITION_TYPES = frozenset({"date", "integer"})
JOIN_CARDINALITIES = frozenset({"one_to_zero_or_one", "many_to_one", "one_to_one"})
REQUIRED_PRIVATE_FIELDS = frozenset({"user", "comment", "parsedcomment", "log_params", "ip"})
REQUIRED_PROVENANCE_FIELDS = frozenset(
    {"source_url", "source_last_modified", "source_etag", "source_content_length", "source_sha256", "retrieved_at"}
)
SEMVER_PATTERN = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")


class CatalogValidationError(ValueError):
    """A catalog validation failure with a stable machine-readable code."""

    def __init__(self, code: str, path: str, detail: str) -> None:
        self.code = code
        self.path = path
        self.detail = detail
        super().__init__(f"[{code}] {path}: {detail}")


def validate_catalog(catalog: Mapping[str, Any]) -> dict[str, Any]:
    """Validate one complete current catalog and return deterministic metadata."""

    _require_mapping(catalog, "$", "invalid_catalog")
    _validate_version(catalog)
    _required_strings(catalog, ("title", "updated", "domain", "authority", "volume_note"), "$")
    _validate_time_domain(catalog)
    sources = _validate_sources(catalog)
    _validate_volume_profiles(catalog)
    _validate_storage(catalog)
    _validate_publication(catalog)
    _validate_stage_responsibilities(catalog)
    _validate_identity_normalization(catalog)
    private_fields = _validate_privacy(catalog)
    datasets = _validate_datasets(catalog, sources)
    _validate_stream_projection(datasets, private_fields)
    joins = _validate_joins(catalog, datasets)
    kpis = _validate_kpis(catalog)
    _validate_business_terms(catalog, datasets, kpis)
    _validate_query_surface(catalog, datasets, joins)
    _validate_primary_scenario(catalog)
    return _metadata(catalog)


def canonical_metadata_json(metadata: Mapping[str, Any]) -> str:
    """Serialize validated metadata in one deterministic, compact representation."""

    return json.dumps(metadata, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _validate_version(catalog: Mapping[str, Any]) -> None:
    schema = _required(catalog, "schema", "$")
    if schema != SUPPORTED_SCHEMA:
        raise CatalogValidationError("unsupported_contract_schema", "$.schema", f"expected {SUPPORTED_SCHEMA!r}")

    version = _required(catalog, "catalog_version", "$")
    if not isinstance(version, str):
        raise CatalogValidationError("invalid_catalog_version", "$.catalog_version", "must be a semantic-version string")
    match = SEMVER_PATTERN.fullmatch(version)
    if not match:
        raise CatalogValidationError("invalid_catalog_version", "$.catalog_version", "must be MAJOR.MINOR.PATCH")
    if int(match.group(1)) != SUPPORTED_CATALOG_MAJOR:
        raise CatalogValidationError(
            "unsupported_catalog_version",
            "$.catalog_version",
            f"expected major version {SUPPORTED_CATALOG_MAJOR}",
        )


def _validate_time_domain(catalog: Mapping[str, Any]) -> None:
    value = _required_mapping(catalog, "time_domain", "$")
    _required_strings(
        value,
        (
            "timezone",
            "batch_partition",
            "stream_window",
            "pageview_filename_semantics",
            "logical_hour_semantics",
        ),
        "$.time_domain",
    )


def _validate_sources(catalog: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    raw_sources = _required_sequence(catalog, "sources", "$")
    sources: dict[str, Mapping[str, Any]] = {}
    for index, source in enumerate(raw_sources):
        path = f"$.sources[{index}]"
        _require_mapping(source, path, "invalid_source")
        source_id = _identifier(source, path)
        _add_unique(sources, source_id, source, path)
        _required_strings(source, ("publisher", "title", "delivery", "format", "documentation"), path)
        delivery = source["delivery"]
        if delivery == "hourly_files":
            _validate_hourly_source(source, path)
        elif delivery == "server_sent_events":
            _validate_stream_source(source, path)
        else:
            raise CatalogValidationError("invalid_source", f"{path}.delivery", f"unsupported delivery {delivery!r}")
    if not sources:
        raise CatalogValidationError("invalid_sources", "$.sources", "must not be empty")
    return sources


def _validate_hourly_source(source: Mapping[str, Any], path: str) -> None:
    _required_strings(source, ("base_url", "url_template", "url_template_clock"), path)
    file_schema = _string_set(_required_sequence(source, "file_schema", path), f"{path}.file_schema")
    if not file_schema:
        raise CatalogValidationError("invalid_source", f"{path}.file_schema", "must not be empty")
    license_contract = _required_mapping(source, "license", path)
    _required_strings(license_contract, ("id", "url"), f"{path}.license")
    daily = _required_mapping(source, "daily_batch", path)
    _required_strings(
        daily,
        ("timezone", "source_file_set", "completeness_rule", "late_source_policy"),
        f"{path}.daily_batch",
    )
    _positive_integer(
        _required(daily, "expected_hourly_files", f"{path}.daily_batch"),
        f"{path}.daily_batch.expected_hourly_files",
        "invalid_source",
    )
    provenance = _required_mapping(source, "provenance", path)
    required = _required_sequence(provenance, "required", f"{path}.provenance")
    provenance_fields = _string_set(required, f"{path}.provenance.required")
    if not provenance_fields:
        raise CatalogValidationError("invalid_source", f"{path}.provenance.required", "must not be empty")
    missing_provenance = sorted(REQUIRED_PROVENANCE_FIELDS - provenance_fields)
    if missing_provenance:
        raise CatalogValidationError(
            "invalid_source",
            f"{path}.provenance.required",
            f"missing required provenance fields {missing_provenance}",
        )
    _required_strings(provenance, ("repository_policy",), f"{path}.provenance")


def _validate_stream_source(source: Mapping[str, Any], path: str) -> None:
    _required_strings(source, ("endpoint", "schema_url", "ingress_projection"), path)
    recovery = _required_mapping(source, "recovery", path)
    _required_strings(recovery, ("cursor", "checkpoint_rule", "retention_note"), f"{path}.recovery")


def _validate_volume_profiles(catalog: Mapping[str, Any]) -> None:
    profiles = _required_sequence(catalog, "volume_profiles", "$")
    identifiers: dict[str, Mapping[str, Any]] = {}
    for index, profile in enumerate(profiles):
        path = f"$.volume_profiles[{index}]"
        _require_mapping(profile, path, "invalid_volume_profile")
        profile_id = _identifier(profile, path)
        _add_unique(identifiers, profile_id, profile, path)
        _required_strings(profile, ("purpose",), path)
        _positive_integer(_required(profile, "pageview_hours", path), f"{path}.pageview_hours", "invalid_volume_profile")
        expected = _required_sequence(profile, "expected_compressed_mb", path)
        if len(expected) != 2 or any(not _is_positive_number(item) for item in expected) or expected[0] > expected[1]:
            raise CatalogValidationError(
                "invalid_volume_profile", f"{path}.expected_compressed_mb", "must be an ascending positive [min, max] pair"
            )
    if not identifiers:
        raise CatalogValidationError("invalid_volume_profile", "$.volume_profiles", "must not be empty")


def _validate_storage(catalog: Mapping[str, Any]) -> None:
    storage = _required_mapping(catalog, "storage", "$")
    _required_strings(storage, ("bronze_batch", "bronze_stream", "silver", "gold", "manifests"), "$.storage")


def _validate_publication(catalog: Mapping[str, Any]) -> None:
    publication = _required_mapping(catalog, "publication", "$")
    immutable = _required(publication, "immutable_objects", "$.publication")
    if immutable is not True:
        raise CatalogValidationError("invalid_security_contract", "$.publication.immutable_objects", "must be true")
    _required_strings(publication, ("no_clobber", "accepted_manifest", "replacement"), "$.publication")


def _validate_stage_responsibilities(catalog: Mapping[str, Any]) -> None:
    stages = _required_mapping(catalog, "stage_responsibilities", "$")
    _required_strings(stages, ("bronze", "silver", "gold"), "$.stage_responsibilities")


def _validate_identity_normalization(catalog: Mapping[str, Any]) -> None:
    normalization = _required_mapping(catalog, "identity_normalization", "$")
    allowlist = _required_mapping(normalization, "stream_project_allowlist", "$.identity_normalization")
    if not allowlist:
        raise CatalogValidationError(
            "invalid_identity_normalization", "$.identity_normalization.stream_project_allowlist", "must not be empty"
        )
    for server_name, project_code in allowlist.items():
        _non_empty_string(server_name, "$.identity_normalization.stream_project_allowlist", "invalid_identity_normalization")
        _non_empty_string(project_code, "$.identity_normalization.stream_project_allowlist", "invalid_identity_normalization")
    _required_strings(
        normalization,
        ("project_code_rule", "page_title_rule", "join_rule"),
        "$.identity_normalization",
    )


def _validate_privacy(catalog: Mapping[str, Any]) -> set[str]:
    privacy = _required_mapping(catalog, "privacy", "$")
    _required_strings(privacy, ("classification", "rule", "pageview_boundary"), "$.privacy")
    raw_fields = _required_sequence(privacy, "forbidden_persisted_stream_fields", "$.privacy")
    fields = _string_set(raw_fields, "$.privacy.forbidden_persisted_stream_fields")
    missing = sorted(REQUIRED_PRIVATE_FIELDS - fields)
    if missing:
        raise CatalogValidationError(
            "invalid_security_contract",
            "$.privacy.forbidden_persisted_stream_fields",
            f"missing required private fields {missing}",
        )
    return fields


def _validate_datasets(
    catalog: Mapping[str, Any], sources: Mapping[str, Mapping[str, Any]]
) -> dict[str, dict[str, Any]]:
    raw_datasets = _required_sequence(catalog, "datasets", "$")
    datasets_by_id: dict[str, Mapping[str, Any]] = {}
    paths_by_id: dict[str, str] = {}
    for index, dataset in enumerate(raw_datasets):
        path = f"$.datasets[{index}]"
        _require_mapping(dataset, path, "invalid_dataset")
        dataset_id = _identifier(dataset, path)
        if dataset_id in datasets_by_id or dataset_id in sources:
            raise CatalogValidationError("duplicate_identifier", f"{path}.id", f"duplicate id {dataset_id!r}")
        datasets_by_id[dataset_id] = dataset
        paths_by_id[dataset_id] = path
    if not datasets_by_id:
        raise CatalogValidationError("invalid_datasets", "$.datasets", "must not be empty")

    datasets: dict[str, dict[str, Any]] = {}
    for dataset_id, dataset in datasets_by_id.items():
        path = paths_by_id[dataset_id]
        _required_strings(dataset, ("domain", "source", "stage", "sensitivity", "freshness"), path)
        if dataset["stage"] not in ALLOWED_STAGES:
            raise CatalogValidationError("invalid_dataset", f"{path}.stage", f"unsupported stage {dataset['stage']!r}")
        fields = _required_sequence(dataset, "fields", path)
        if not fields:
            raise CatalogValidationError("invalid_fields", f"{path}.fields", "must not be empty")
        field_names = _validate_schema(dataset, fields, path)
        _validate_dataset_source(dataset, sources, datasets_by_id, path)
        primary_key = _validate_key_fields(dataset, "primary_key", field_names, path)
        _validate_partition_fields(dataset, field_names, path)
        datasets[dataset_id] = {"dataset": dataset, "fields": field_names, "primary_key": primary_key}
    return datasets


def _validate_schema(
    dataset: Mapping[str, Any], fields: Sequence[Any], path: str
) -> dict[str, dict[str, Any]]:
    schema = _required_sequence(dataset, "schema", path)
    if not schema:
        raise CatalogValidationError("invalid_schema", f"{path}.schema", "must not be empty")
    field_names: dict[str, dict[str, Any]] = {}
    for index, definition in enumerate(schema):
        field_path = f"{path}.schema[{index}]"
        _require_mapping(definition, field_path, "invalid_field")
        name = _non_empty_string(_required(definition, "name", field_path), f"{field_path}.name", "invalid_field")
        field_type = _required(definition, "type", field_path)
        nullable = _required(definition, "nullable", field_path)
        if name in field_names:
            raise CatalogValidationError("duplicate_identifier", f"{field_path}.name", f"duplicate field {name!r}")
        if not isinstance(field_type, str) or field_type not in ALLOWED_TYPES:
            raise CatalogValidationError("incompatible_type", f"{field_path}.type", f"unsupported type {field_type!r}")
        if not isinstance(nullable, bool):
            raise CatalogValidationError("invalid_field", f"{field_path}.nullable", "must be boolean")
        field_names[name] = {"type": field_type, "nullable": nullable}

    declared = _string_set(fields, f"{path}.fields")
    if set(field_names) != declared:
        missing = sorted(declared - set(field_names))
        undocumented = sorted(set(field_names) - declared)
        raise CatalogValidationError(
            "missing_field",
            f"{path}.schema",
            f"schema and fields differ; missing={missing}, undocumented={undocumented}",
        )
    return field_names


def _validate_dataset_source(
    dataset: Mapping[str, Any],
    sources: Mapping[str, Mapping[str, Any]],
    datasets: Mapping[str, Mapping[str, Any]],
    path: str,
) -> None:
    source = dataset["source"]
    for source_id in source.split(","):
        if not source_id or source_id.isspace():
            raise CatalogValidationError("invalid_source_reference", f"{path}.source", "contains an empty source")
        if source_id != "internal" and source_id not in sources and source_id not in datasets:
            raise CatalogValidationError("invalid_source_reference", f"{path}.source", f"unknown source {source_id!r}")


def _validate_key_fields(
    dataset: Mapping[str, Any], key: str, field_names: Mapping[str, Mapping[str, Any]], path: str
) -> set[str]:
    values = _required_sequence(dataset, key, path)
    names = _string_set(values, f"{path}.{key}")
    if not names:
        raise CatalogValidationError("invalid_key", f"{path}.{key}", "must not be empty")
    missing = sorted(names - set(field_names))
    if missing:
        raise CatalogValidationError("missing_field", f"{path}.{key}", f"unknown fields {missing}")
    nullable = sorted(name for name in names if field_names[name]["nullable"])
    if nullable:
        raise CatalogValidationError("invalid_key", f"{path}.{key}", f"nullable key fields {nullable}")
    return names


def _validate_partition_fields(
    dataset: Mapping[str, Any], field_names: Mapping[str, Mapping[str, Any]], path: str
) -> None:
    values = _required_sequence(dataset, "partition_keys", path)
    names = _string_set(values, f"{path}.partition_keys")
    if not names:
        raise CatalogValidationError("invalid_partition_metadata", f"{path}.partition_keys", "must not be empty")
    for name in names:
        field = field_names.get(name)
        if field is None or field["type"] not in PARTITION_TYPES:
            detail = "is absent from schema" if field is None else f"has unsupported partition type {field['type']!r}"
            raise CatalogValidationError("invalid_partition_metadata", f"{path}.partition_keys", f"{name!r} {detail}")
        if field["nullable"]:
            raise CatalogValidationError(
                "invalid_partition_metadata", f"{path}.partition_keys", f"{name!r} must not be nullable"
            )


def _validate_stream_projection(datasets: Mapping[str, dict[str, Any]], private_fields: set[str]) -> None:
    stream = datasets.get("recentchange_events")
    if stream is None:
        raise CatalogValidationError("missing_field", "$.datasets", "missing required dataset 'recentchange_events'")
    leaked = sorted(private_fields & set(stream["fields"]))
    if leaked:
        raise CatalogValidationError("invalid_security_contract", "$.datasets", f"private stream fields persisted {leaked}")


def _validate_joins(
    catalog: Mapping[str, Any], datasets: Mapping[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    raw_joins = _required_sequence(catalog, "joins", "$")
    joins: dict[str, dict[str, Any]] = {}
    for index, join in enumerate(raw_joins):
        path = f"$.joins[{index}]"
        _require_mapping(join, path, "invalid_join")
        join_id = _identifier(join, path)
        if join_id in joins:
            raise CatalogValidationError("duplicate_identifier", f"{path}.id", f"duplicate join id {join_id!r}")
        _required_strings(join, ("left", "right", "purpose"), path)
        cardinality = _non_empty_string(
            _required(join, "cardinality", path), f"{path}.cardinality", "invalid_join"
        )
        if cardinality not in JOIN_CARDINALITIES:
            raise CatalogValidationError("invalid_join", f"{path}.cardinality", f"unsupported cardinality {cardinality!r}")
        left = _join_columns(join["left"], f"{path}.left")
        right = _join_columns(join["right"], f"{path}.right")
        if len(left) != len(right):
            raise CatalogValidationError("invalid_join", path, "left and right key counts differ")
        left_dataset = _single_join_dataset(left, f"{path}.left")
        right_dataset = _single_join_dataset(right, f"{path}.right")
        if left_dataset == right_dataset:
            raise CatalogValidationError("invalid_join", path, "self-joins are not supported")
        for (left_id, left_field), (right_id, right_field) in zip(left, right, strict=True):
            for dataset_id, field in ((left_id, left_field), (right_id, right_field)):
                if dataset_id not in datasets or field not in datasets[dataset_id]["fields"]:
                    raise CatalogValidationError("invalid_join_reference", path, f"unknown field {dataset_id}.{field}")
            left_type = datasets[left_id]["fields"][left_field]["type"]
            right_type = datasets[right_id]["fields"][right_field]["type"]
            if left_type != right_type:
                raise CatalogValidationError(
                    "invalid_join",
                    path,
                    f"incompatible field types {left_id}.{left_field} ({left_type}) and "
                    f"{right_id}.{right_field} ({right_type})",
                )
        left_unique = datasets[left_dataset]["primary_key"].issubset({field for _, field in left})
        right_unique = datasets[right_dataset]["primary_key"].issubset({field for _, field in right})
        if cardinality == "many_to_one" and not right_unique:
            raise CatalogValidationError("invalid_join", path, "right join keys do not cover a declared primary key")
        if cardinality in {"one_to_one", "one_to_zero_or_one"} and not (left_unique and right_unique):
            raise CatalogValidationError("invalid_join", path, "both join keys must cover declared primary keys")
        joins[join_id] = {"join": join, "datasets": frozenset({left_dataset, right_dataset})}
    if not joins:
        raise CatalogValidationError("invalid_joins", "$.joins", "must not be empty")
    return joins


def _validate_kpis(catalog: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    raw_kpis = _required_sequence(catalog, "kpis", "$")
    kpis: dict[str, Mapping[str, Any]] = {}
    for index, kpi in enumerate(raw_kpis):
        path = f"$.kpis[{index}]"
        _require_mapping(kpi, path, "invalid_kpi")
        kpi_id = _identifier(kpi, path)
        _add_unique(kpis, kpi_id, kpi, path)
        _required_strings(kpi, ("unit", "formula"), path)
        if "completeness" in kpi:
            _non_empty_string(kpi["completeness"], f"{path}.completeness", "invalid_kpi")
    if not kpis:
        raise CatalogValidationError("invalid_kpi", "$.kpis", "must not be empty")
    return kpis


def _validate_business_terms(
    catalog: Mapping[str, Any], datasets: Mapping[str, dict[str, Any]], kpis: Mapping[str, Mapping[str, Any]]
) -> None:
    raw_terms = _required_sequence(catalog, "business_terms", "$")
    terms: set[str] = set()
    for index, term_contract in enumerate(raw_terms):
        path = f"$.business_terms[{index}]"
        _require_mapping(term_contract, path, "invalid_business_term")
        term = _non_empty_string(_required(term_contract, "term", path), f"{path}.term", "invalid_business_term")
        if term in terms:
            raise CatalogValidationError("duplicate_identifier", f"{path}.term", f"duplicate term {term!r}")
        terms.add(term)
        _required_strings(term_contract, ("definition",), path)
        mappings = _required_sequence(term_contract, "maps_to", path)
        references = _string_set(mappings, f"{path}.maps_to")
        if not references:
            raise CatalogValidationError("invalid_business_term", f"{path}.maps_to", "must not be empty")
        for reference in references:
            if reference in datasets or reference in kpis:
                continue
            dataset_id, separator, field = reference.partition(".")
            if not separator or dataset_id not in datasets or field not in datasets[dataset_id]["fields"]:
                raise CatalogValidationError("invalid_business_term", f"{path}.maps_to", f"unknown reference {reference!r}")
    if not terms:
        raise CatalogValidationError("invalid_business_term", "$.business_terms", "must not be empty")


def _validate_query_surface(
    catalog: Mapping[str, Any], datasets: Mapping[str, dict[str, Any]], joins: Mapping[str, dict[str, Any]]
) -> None:
    surface = _required_mapping(catalog, "query_surface", "$")
    _required_strings(surface, ("policy",), "$.query_surface")
    raw_views = _required_sequence(surface, "views", "$.query_surface")
    contracts = _required_mapping(surface, "view_contracts", "$.query_surface")
    view_names = _string_set(raw_views, "$.query_surface.views")
    if not view_names:
        raise CatalogValidationError("undocumented_logical_view", "$.query_surface.views", "must not be empty")
    contract_names = {
        _non_empty_string(name, "$.query_surface.view_contracts", "undocumented_logical_view")
        for name in contracts
    }
    if view_names != contract_names:
        missing = sorted(view_names - contract_names)
        extra = sorted(contract_names - view_names)
        raise CatalogValidationError(
            "undocumented_logical_view",
            "$.query_surface.view_contracts",
            f"missing={missing}, unregistered={extra}",
        )
    for name in sorted(view_names):
        path = f"$.query_surface.view_contracts.{name}"
        contract = contracts[name]
        _require_mapping(contract, path, "undocumented_logical_view")
        inputs = _string_set(_required_sequence(contract, "inputs", path), f"{path}.inputs")
        join_ids = _string_set(_required_sequence(contract, "joins", path), f"{path}.joins")
        fields = _string_set(_required_sequence(contract, "fields", path), f"{path}.fields")
        if not inputs or not fields:
            raise CatalogValidationError("undocumented_logical_view", path, "inputs and fields must not be empty")
        unknown_inputs = sorted(inputs - set(datasets))
        unknown_joins = sorted(join_ids - set(joins))
        if unknown_inputs or unknown_joins:
            raise CatalogValidationError(
                "undocumented_logical_view", path, f"unknown inputs={unknown_inputs}, joins={unknown_joins}"
            )
        if len(inputs) == 1 and join_ids:
            raise CatalogValidationError("undocumented_logical_view", path, "single-input views must not declare joins")
        if len(inputs) > 1:
            _validate_view_join_graph(inputs, join_ids, joins, path)
        input_fields = {field for dataset_id in inputs for field in datasets[dataset_id]["fields"]}
        unknown_fields = sorted(fields - input_fields)
        if unknown_fields:
            raise CatalogValidationError(
                "undocumented_logical_view", f"{path}.fields", f"fields outside declared inputs {unknown_fields}"
            )


def _validate_view_join_graph(
    inputs: set[str], join_ids: set[str], joins: Mapping[str, dict[str, Any]], path: str
) -> None:
    if not join_ids:
        raise CatalogValidationError("undocumented_logical_view", path, "multi-input views require join provenance")
    adjacency = {dataset_id: set() for dataset_id in inputs}
    for join_id in join_ids:
        participants = joins[join_id]["datasets"]
        if not participants.issubset(inputs):
            raise CatalogValidationError(
                "undocumented_logical_view", f"{path}.joins", f"join {join_id!r} references undeclared inputs"
            )
        left, right = tuple(participants)
        adjacency[left].add(right)
        adjacency[right].add(left)
    visited: set[str] = set()
    pending = [next(iter(inputs))]
    while pending:
        current = pending.pop()
        if current in visited:
            continue
        visited.add(current)
        pending.extend(adjacency[current] - visited)
    if visited != inputs:
        raise CatalogValidationError("undocumented_logical_view", f"{path}.joins", "joins do not connect all inputs")


def _validate_primary_scenario(catalog: Mapping[str, Any]) -> None:
    scenario = _required_mapping(catalog, "primary_scenario", "$")
    _required_strings(
        scenario,
        (
            "id",
            "question",
            "project_code",
            "partition_date",
            "logical_window",
            "withheld_source_label",
            "fault",
            "fault_injection",
            "expected_diagnosis",
            "negative_control",
        ),
        "$.primary_scenario",
    )
    remediation = _required_mapping(scenario, "remediation", "$.primary_scenario")
    _required_strings(remediation, ("operation", "verification"), "$.primary_scenario.remediation")
    required_booleans = {
        "requires_approval": True,
        "publishes_new_manifest": True,
        "overwrites_existing_objects": False,
    }
    for key, expected in required_booleans.items():
        value = _required(remediation, key, "$.primary_scenario.remediation")
        if value is not expected:
            raise CatalogValidationError(
                "invalid_security_contract", f"$.primary_scenario.remediation.{key}", f"must be {expected!r}"
            )


def _metadata(catalog: Mapping[str, Any]) -> dict[str, Any]:
    metadata = copy.deepcopy(dict(catalog))
    for key in ("sources", "volume_profiles", "datasets", "joins", "kpis"):
        metadata[key] = sorted(metadata[key], key=lambda item: item["id"])
    metadata["business_terms"] = sorted(metadata["business_terms"], key=lambda item: item["term"])
    metadata["privacy"]["forbidden_persisted_stream_fields"] = sorted(
        metadata["privacy"]["forbidden_persisted_stream_fields"]
    )
    metadata["query_surface"]["views"] = sorted(metadata["query_surface"]["views"])
    for contract in metadata["query_surface"]["view_contracts"].values():
        contract["inputs"] = sorted(contract["inputs"])
        contract["joins"] = sorted(contract["joins"])
    for term in metadata["business_terms"]:
        term["maps_to"] = sorted(term["maps_to"])
    return metadata


def _join_columns(value: Any, path: str) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, str) or not value:
        raise CatalogValidationError("invalid_join", path, "must be a comma-separated dataset.field list")
    columns: list[tuple[str, str]] = []
    for reference in value.split(","):
        dataset_id, separator, field = reference.partition(".")
        if not separator or not dataset_id or not field or "." in field:
            raise CatalogValidationError("invalid_join", path, f"invalid reference {reference!r}")
        columns.append((dataset_id, field))
    if len(set(columns)) != len(columns):
        raise CatalogValidationError("invalid_join", path, "contains duplicate join fields")
    return tuple(columns)


def _single_join_dataset(columns: Sequence[tuple[str, str]], path: str) -> str:
    datasets = {dataset_id for dataset_id, _ in columns}
    if len(datasets) != 1:
        raise CatalogValidationError("invalid_join", path, "all join fields on one side must belong to one dataset")
    return next(iter(datasets))


def _identifier(value: Mapping[str, Any], path: str) -> str:
    return _non_empty_string(_required(value, "id", path), f"{path}.id", "invalid_identifier")


def _add_unique(
    target: dict[str, Mapping[str, Any]], key: str, value: Mapping[str, Any], path: str
) -> None:
    if key in target:
        raise CatalogValidationError("duplicate_identifier", f"{path}.id", f"duplicate id {key!r}")
    target[key] = value


def _required(value: Mapping[str, Any], key: str, path: str) -> Any:
    if key not in value:
        raise CatalogValidationError("missing_field", path, f"missing required field {key!r}")
    return value[key]


def _required_mapping(value: Mapping[str, Any], key: str, path: str) -> Mapping[str, Any]:
    result = _required(value, key, path)
    _require_mapping(result, f"{path}.{key}", "missing_field")
    return result


def _required_sequence(value: Mapping[str, Any], key: str, path: str) -> Sequence[Any]:
    result = _required(value, key, path)
    _require_sequence(result, f"{path}.{key}", "missing_field")
    return result


def _required_strings(value: Mapping[str, Any], keys: Sequence[str], path: str) -> None:
    for key in keys:
        _non_empty_string(_required(value, key, path), f"{path}.{key}", "missing_field")


def _non_empty_string(value: Any, path: str, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CatalogValidationError(code, path, "must be a non-empty string")
    return value


def _positive_integer(value: Any, path: str, code: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CatalogValidationError(code, path, "must be a positive integer")
    return value


def _is_positive_number(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float)) and value > 0


def _require_mapping(value: Any, path: str, code: str) -> None:
    if not isinstance(value, Mapping):
        raise CatalogValidationError(code, path, "must be an object")


def _require_sequence(value: Any, path: str, code: str) -> None:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise CatalogValidationError(code, path, "must be an array")


def _string_set(values: Sequence[Any], path: str) -> set[str]:
    result: set[str] = set()
    for index, value in enumerate(values):
        item = _non_empty_string(value, f"{path}[{index}]", "invalid_identifier")
        if item in result:
            raise CatalogValidationError("duplicate_identifier", f"{path}[{index}]", f"duplicate identifier {item!r}")
        result.add(item)
    return result
