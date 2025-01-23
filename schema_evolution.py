import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

# Configure basic logging (you can configure more advanced logging in production)
logging.basicConfig(level=logging.INFO) # Sets up basic logging to display informational messages.
logger = logging.getLogger(__name__) # Gets a logger instance named after the current module.

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
        self.schemas: Dict[str, Dict[int, Dict[str, Any]]] = {} # Initializes an empty dictionary to store schemas, organized by topic and version.
        self.schema_versions: Dict[str, int] = {} # Initializes an empty dictionary to track the latest version for each topic.

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
        logger.info("Registering schema for topic: %s", topic) # Logs an informational message indicating schema registration is starting.

        if topic not in self.schemas:
            self.schemas[topic] = {} # If the topic is new, initialize an empty dictionary for its versions.
            self.schema_versions[topic] = 0 # Initialize the version counter for the new topic to 0.

        if version is None:
            version = self.schema_versions[topic] + 1 # If no version is provided, automatically increment from the current latest version.

        if version in self.schemas[topic]:
            raise ValueError(f"Version {version} already exists for topic '{topic}'.") # Raises an error if the given version already exists for the topic.

        old_schema = self.schemas[topic].get(version - 1, {}).get('schema') # Retrieves the schema of the previous version for comparison, if it exists.
        schema_hash = self._compute_schema_hash(schema) # Computes a hash of the new schema for version identification.
        breaking_changes = self._detect_breaking_changes(topic, old_schema, schema) # Detects breaking changes by comparing the new schema with the previous one.

        self.schemas[topic][version] = {
            'schema': schema,
            'hash': schema_hash,
            'created_at': datetime.now(),
            'breaking_changes': breaking_changes,
        } # Stores the new schema, its hash, creation timestamp, and detected breaking changes in the schemas dictionary.
        self.schema_versions[topic] = version # Updates the latest version number for the topic.

        logger.info("Registered schema version %d for topic '%s'", version, topic) # Logs an informational message confirming successful schema registration.

    def _compute_schema_hash(self, schema: Dict[str, Any]) -> str:
        """
        Compute a SHA-256 hash of the schema for quick comparison.

        :param schema: The schema as a dictionary.
        :return: Hexadecimal hash string.
        """
        return hashlib.sha256(
            json.dumps(schema, sort_keys=True).encode('utf-8')
        ).hexdigest() # Converts the schema to a JSON string, sorts keys for consistent hashing, encodes to UTF-8, computes SHA-256 hash, and returns the hex digest.

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
            return [] # If there is no old schema, there are no breaking changes, so return an empty list.

        breaking_changes: List[str] = [] # Initialize an empty list to store descriptions of breaking changes.

        # Identify removed fields
        old_fields = set(self._flatten_schema(old_schema)) # Get a set of all fields from the old schema.
        new_fields = set(self._flatten_schema(new_schema)) # Get a set of all fields from the new schema.
        removed_fields = old_fields - new_fields # Find fields that are in the old schema but not in the new schema (removed fields).
        if removed_fields:
            breaking_changes.append(f"Removed fields: {removed_fields}") # If there are removed fields, add a description of them to the breaking changes list.

        # Identify type changes for fields present in both old and new schemas
        for field in old_fields & new_fields:
            old_type = self._get_field_type(old_schema, field) # Get the type of the field from the old schema.
            new_type = self._get_field_type(new_schema, field) # Get the type of the field from the new schema.
            if old_type != new_type:
                breaking_changes.append(
                    f"Type change for '{field}': {old_type} -> {new_type}"
                ) # If the types are different, add a description of the type change to the breaking changes list.

        return breaking_changes # Return the list of breaking change descriptions.

    def _flatten_schema(self, schema: Dict[str, Any], prefix: str = "") -> List[str]:
        """
        Flatten a nested schema into a list of dotted field paths.

        :param schema: The schema dictionary.
        :param prefix: A prefix for nested fields (used internally).
        :return: List of dotted field paths (e.g., 'metadata.source').
        """
        fields: List[str] = [] # Initialize an empty list to store flattened field paths.
        for key, value in schema.items():
            full_key = f"{prefix}.{key}" if prefix else key # Construct the full field path, including the prefix for nested fields.
            # If 'type' is not in a nested dict, assume it's another level of nesting
            if isinstance(value, dict) and 'type' not in value:
                fields.extend(self._flatten_schema(value, full_key)) # If the value is a dictionary and does not contain 'type', recursively flatten it with the updated prefix.
            else:
                fields.append(full_key) # If it's not a nested schema or contains 'type', add the full key to the list of fields.
        return fields # Return the list of flattened field paths.

    def _get_field_type(self, schema: Dict[str, Any], field_path: str) -> Optional[str]:
        """
        Get the 'type' of a field given its dotted path.

        :param schema: The schema dictionary.
        :param field_path: The dotted path to the field.
        :return: The field type if found, otherwise None.
        """
        keys = field_path.split(".") # Split the dotted field path into individual keys.
        current_level = schema # Start at the root of the schema.
        for key in keys:
            if key in current_level:
                current_level = current_level[key] # Move down to the next level in the schema if the key is found.
            else:
                return None # If any key in the path is not found, the field does not exist, return None.
        return current_level.get('type') # After traversing the path, return the 'type' of the field at the final level, or None if 'type' key is not present.

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
        self.lineage_graph: Dict[str, Dict[str, Set[str]]] = {} # Initializes an empty dictionary to represent the lineage graph, storing upstream and downstream nodes for each node.
        self.transformations: Dict[str, Dict[str, Any]] = {} # Initializes an empty dictionary to store details of each transformation.

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
        logger.info("Tracking transformation from '%s' to '%s'", source_id, target_id) # Logs an informational message indicating transformation tracking is starting.

        # Ensure both source and target exist in the lineage graph
        if source_id not in self.lineage_graph:
            self.lineage_graph[source_id] = {'downstream': set(), 'upstream': set()} # If the source node is new, add it to the lineage graph with empty upstream and downstream sets.
        if target_id not in self.lineage_graph:
            self.lineage_graph[target_id] = {'downstream': set(), 'upstream': set()} # If the target node is new, add it to the lineage graph with empty upstream and downstream sets.

        # Record the relationships
        self.lineage_graph[source_id]['downstream'].add(target_id) # Add the target node as a downstream dependency of the source node.
        self.lineage_graph[target_id]['upstream'].add(source_id) # Add the source node as an upstream dependency of the target node.

        # Create a unique ID for this transformation
        transformation_id = f"{source_id}->{target_id}" # Create a unique ID for the transformation based on source and target IDs.
        self.transformations[transformation_id] = {
            'type': transformation_type,
            'details': transformation_details,
            'timestamp': datetime.now()
        } # Store the transformation type, details, and timestamp in the transformations dictionary, using the transformation ID as the key.

        logger.debug("Transformation details: %s", self.transformations[transformation_id]) # Logs debug information about the transformation details.

    def get_upstream_lineage(self, target_id: str) -> Dict[str, Any]:
        """
        Recursively gather all upstream dependencies (and the transformations
        that led to them) for a given target_id.

        :param target_id: The unique identifier of the data target.
        :return: A nested dictionary representing the upstream lineage structure.
        """
        visited: Set[str] = set() # Initialize a set to keep track of visited nodes during traversal to prevent cycles.

        def traverse_upstream(node_id: str) -> Dict[str, Any]:
            if node_id in visited:
                return {} # If the node has already been visited, return an empty dictionary to prevent infinite recursion.
            visited.add(node_id) # Mark the current node as visited.

            result: Dict[str, Any] = {} # Initialize an empty dictionary to store the upstream lineage for the current node.
            upstream_nodes = self.lineage_graph.get(node_id, {}).get('upstream', set()) # Get the set of upstream nodes for the current node.

            for up_id in upstream_nodes:
                transformation_id = f"{up_id}->{node_id}" # Construct the transformation ID for the edge from the upstream node to the current node.
                result[up_id] = {
                    'transformation': self.transformations.get(transformation_id), # Get the transformation details for this edge.
                    'upstream': traverse_upstream(up_id) # Recursively traverse upstream from the upstream node.
                }
            return result # Return the upstream lineage structure for the current node.

        return traverse_upstream(target_id) # Start the upstream traversal from the target node and return the resulting lineage structure.

def main() -> None:
    """
    Demonstrates usage of the SchemaRegistry and DataLineageTracker with example data.
    """
    # ---------------------------------------
    # Schema evolution example
    # ---------------------------------------
    registry = SchemaRegistry() # Create an instance of the SchemaRegistry.
    logger.info("=== Starting Schema Evolution Example ===") # Log the start of the schema evolution example.

    # Register initial schema
    initial_schema = {
        "user_id": {"type": "string"},
        "timestamp": {"type": "timestamp"},
        "event_type": {"type": "string"},
        "metadata": {
            "source": {"type": "string"},
            "version": {"type": "integer"}
        }
    } # Define an initial schema as a dictionary.
    registry.register_schema("user_events", initial_schema, version=1) # Register the initial schema for the "user_events" topic with version 1.

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
    } # Define an updated schema with a new field.
    registry.register_schema("user_events", updated_schema, version=2) # Register the updated schema for the "user_events" topic with version 2.

    # Display the registered schemas
    logger.info("Schemas: %s", json.dumps(registry.schemas, default=str, indent=2)) # Log and display the registered schemas in JSON format for inspection.

    # ---------------------------------------
    # Data lineage example
    # ---------------------------------------
    lineage_tracker = DataLineageTracker() # Create an instance of the DataLineageTracker.
    logger.info("=== Starting Data Lineage Example ===") # Log the start of the data lineage example.

    # Track data flow from raw_events to cleaned_events
    lineage_tracker.track_transformation(
        "raw_events",
        "cleaned_events",
        "cleanup",
        {
            "operations": ["deduplication", "timestamp_standardization"],
            "filters": ["removed_null_user_ids"]
        }
    ) # Track a transformation from "raw_events" to "cleaned_events" of type "cleanup" with specific details.

    # Track data flow from cleaned_events to aggregated_metrics
    lineage_tracker.track_transformation(
        "cleaned_events",
        "aggregated_metrics",
        "aggregation",
        {
            "operations": ["hourly_rollup", "user_count"],
            "grouping_keys": ["event_type", "source"]
        }
    ) # Track a transformation from "cleaned_events" to "aggregated_metrics" of type "aggregation" with specific details.

    # Get upstream lineage for aggregated_metrics
    upstream_lineage = lineage_tracker.get_upstream_lineage("aggregated_metrics") # Retrieve the upstream lineage for "aggregated_metrics".
    logger.info("Upstream lineage for 'aggregated_metrics':\n%s",
                json.dumps(upstream_lineage, default=str, indent=2)) # Log and display the upstream lineage in JSON format.

if __name__ == "__main__":
    main() # Execute the main function when the script is run directly.