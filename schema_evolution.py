import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

# Configure basic logging (you can configure more advanced logging in production)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SchemaRegistry:
    """
    A registry for schema versions and their evolution over time.
    Tracks each schema by topic and version, computes a hash of the schema,
    and detects breaking changes between consecutive versions.
    """

    def __init__(self) -> None:
        """
        Initialize the SchemaRegistry with empty dictionaries
        for storing schemas and their versions.
        """
        self.schemas: Dict[str, Dict[int, Dict[str, Any]]] = {}
        self.schema_versions: Dict[str, int] = {}

    def register_schema(self, topic: str, schema: Dict[str, Any], version: Optional[int] = None) -> None:
        """
        Register a new schema for a given topic. If no version is specified, 
        the next version number is automatically assigned.

        :param topic: The schema topic/category (e.g., 'user_events').
        :param schema: The actual schema definition as a dictionary.
        :param version: Optional version number for the new schema. If not provided, 
                        it increments from the current highest version.
        :raises ValueError: If an attempt is made to overwrite an existing version.
        """
        logger.info("Registering schema for topic: %s", topic)

        if topic not in self.schemas:
            self.schemas[topic] = {}
            self.schema_versions[topic] = 0

        if version is None:
            version = self.schema_versions[topic] + 1

        if version in self.schemas[topic]:
            raise ValueError(f"Version {version} already exists for topic '{topic}'.")

        old_schema = self.schemas[topic].get(version - 1, {}).get('schema')
        schema_hash = self._compute_schema_hash(schema)
        breaking_changes = self._detect_breaking_changes(topic, old_schema, schema)

        self.schemas[topic][version] = {
            'schema': schema,
            'hash': schema_hash,
            'created_at': datetime.now(),
            'breaking_changes': breaking_changes,
        }
        self.schema_versions[topic] = version

        logger.info("Registered schema version %d for topic '%s'", version, topic)

    def _compute_schema_hash(self, schema: Dict[str, Any]) -> str:
        """
        Compute a SHA-256 hash of the schema for quick comparison.

        :param schema: The schema as a dictionary.
        :return: Hexadecimal hash string.
        """
        return hashlib.sha256(
            json.dumps(schema, sort_keys=True).encode('utf-8')
        ).hexdigest()

    def _detect_breaking_changes(
        self,
        topic: str,
        old_schema: Optional[Dict[str, Any]],
        new_schema: Dict[str, Any]
    ) -> List[str]:
        """
        Detect breaking changes between two schema versions. 
        Currently checks for removed fields and type changes.

        :param topic: The schema topic (not strictly used here, but kept for potential future logic).
        :param old_schema: The previous version of the schema (if any).
        :param new_schema: The new schema.
        :return: List of breaking change descriptions.
        """
        if not old_schema:
            return []

        breaking_changes: List[str] = []

        # Identify removed fields
        old_fields = set(self._flatten_schema(old_schema))
        new_fields = set(self._flatten_schema(new_schema))
        removed_fields = old_fields - new_fields
        if removed_fields:
            breaking_changes.append(f"Removed fields: {removed_fields}")

        # Identify type changes for fields present in both old and new schemas
        for field in old_fields & new_fields:
            old_type = self._get_field_type(old_schema, field)
            new_type = self._get_field_type(new_schema, field)
            if old_type != new_type:
                breaking_changes.append(
                    f"Type change for '{field}': {old_type} -> {new_type}"
                )

        return breaking_changes

    def _flatten_schema(self, schema: Dict[str, Any], prefix: str = "") -> List[str]:
        """
        Flatten a nested schema into a list of dotted field paths.

        :param schema: The schema dictionary.
        :param prefix: A prefix for nested fields (used internally).
        :return: List of dotted field paths (e.g., 'metadata.source').
        """
        fields: List[str] = []
        for key, value in schema.items():
            full_key = f"{prefix}.{key}" if prefix else key
            # If 'type' is not in a nested dict, assume it's another level of nesting
            if isinstance(value, dict) and 'type' not in value:
                fields.extend(self._flatten_schema(value, full_key))
            else:
                fields.append(full_key)
        return fields

    def _get_field_type(self, schema: Dict[str, Any], field_path: str) -> Optional[str]:
        """
        Get the 'type' of a field given its dotted path.

        :param schema: The schema dictionary.
        :param field_path: The dotted path to the field.
        :return: The field type if found, otherwise None.
        """
        keys = field_path.split(".")
        current_level = schema
        for key in keys:
            if key in current_level:
                current_level = current_level[key]
            else:
                return None
        return current_level.get('type')

class DataLineageTracker:
    """
    A tracker that maintains relationships (lineage) between different data nodes (e.g., tables, files, datasets).
    Each transformation from a source to a target is recorded with details.
    """

    def __init__(self) -> None:
        """
        Initialize the DataLineageTracker with empty structures
        for the lineage graph and transformations.
        """
        self.lineage_graph: Dict[str, Dict[str, Set[str]]] = {}
        self.transformations: Dict[str, Dict[str, Any]] = {}

    def track_transformation(
        self,
        source_id: str,
        target_id: str,
        transformation_type: str,
        transformation_details: Dict[str, Any]
    ) -> None:
        """
        Track a data transformation from source_id to target_id.

        :param source_id: The unique identifier of the data source.
        :param target_id: The unique identifier of the data target.
        :param transformation_type: The type of transformation (e.g., 'cleanup', 'aggregation').
        :param transformation_details: Dictionary describing how the transformation was performed.
        """
        logger.info("Tracking transformation from '%s' to '%s'", source_id, target_id)

        # Ensure both source and target exist in the lineage graph
        if source_id not in self.lineage_graph:
            self.lineage_graph[source_id] = {'downstream': set(), 'upstream': set()}
        if target_id not in self.lineage_graph:
            self.lineage_graph[target_id] = {'downstream': set(), 'upstream': set()}

        # Record the relationships
        self.lineage_graph[source_id]['downstream'].add(target_id)
        self.lineage_graph[target_id]['upstream'].add(source_id)

        # Create a unique ID for this transformation
        transformation_id = f"{source_id}->{target_id}"
        self.transformations[transformation_id] = {
            'type': transformation_type,
            'details': transformation_details,
            'timestamp': datetime.now()
        }

        logger.debug("Transformation details: %s", self.transformations[transformation_id])

    def get_upstream_lineage(self, target_id: str) -> Dict[str, Any]:
        """
        Recursively gather all upstream dependencies (and the transformations 
        that led to them) for a given target_id.

        :param target_id: The unique identifier of the data target.
        :return: A nested dictionary representing the upstream lineage structure.
        """
        visited: Set[str] = set()

        def traverse_upstream(node_id: str) -> Dict[str, Any]:
            if node_id in visited:
                return {}
            visited.add(node_id)

            result: Dict[str, Any] = {}
            upstream_nodes = self.lineage_graph.get(node_id, {}).get('upstream', set())

            for up_id in upstream_nodes:
                transformation_id = f"{up_id}->{node_id}"
                result[up_id] = {
                    'transformation': self.transformations.get(transformation_id),
                    'upstream': traverse_upstream(up_id)
                }
            return result

        return traverse_upstream(target_id)

def main() -> None:
    """
    Demonstrates usage of the SchemaRegistry and DataLineageTracker with example data.
    """
    # ---------------------------------------
    # Schema evolution example
    # ---------------------------------------
    registry = SchemaRegistry()
    logger.info("=== Starting Schema Evolution Example ===")

    # Register initial schema
    initial_schema = {
        "user_id": {"type": "string"},
        "timestamp": {"type": "timestamp"},
        "event_type": {"type": "string"},
        "metadata": {
            "source": {"type": "string"},
            "version": {"type": "integer"}
        }
    }
    registry.register_schema("user_events", initial_schema, version=1)

    # Register updated schema with new field
    updated_schema = {
        "user_id": {"type": "string"},
        "timestamp": {"type": "timestamp"},
        "event_type": {"type": "string"},
        "metadata": {
            "source": {"type": "string"},
            "version": {"type": "integer"},
            "environment": {"type": "string"}  # New field
        }
    }
    registry.register_schema("user_events", updated_schema, version=2)

    # Display the registered schemas
    logger.info("Schemas: %s", json.dumps(registry.schemas, default=str, indent=2))

    # ---------------------------------------
    # Data lineage example
    # ---------------------------------------
    lineage_tracker = DataLineageTracker()
    logger.info("=== Starting Data Lineage Example ===")

    # Track data flow from raw_events to cleaned_events
    lineage_tracker.track_transformation(
        "raw_events",
        "cleaned_events",
        "cleanup",
        {
            "operations": ["deduplication", "timestamp_standardization"],
            "filters": ["removed_null_user_ids"]
        }
    )

    # Track data flow from cleaned_events to aggregated_metrics
    lineage_tracker.track_transformation(
        "cleaned_events",
        "aggregated_metrics",
        "aggregation",
        {
            "operations": ["hourly_rollup", "user_count"],
            "grouping_keys": ["event_type", "source"]
        }
    )

    # Get upstream lineage for aggregated_metrics
    upstream_lineage = lineage_tracker.get_upstream_lineage("aggregated_metrics")
    logger.info("Upstream lineage for 'aggregated_metrics':\n%s",
                json.dumps(upstream_lineage, default=str, indent=2))

if __name__ == "__main__":
    main()
