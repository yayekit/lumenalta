from datetime import datetime
from typing import Dict, List, Optional
import hashlib
import json

class SchemaRegistry:
    def __init__(self):
        self.schemas = {}
        self.schema_versions = {}
        
    def register_schema(self, topic: str, schema: Dict, version: Optional[int] = None):
        """Register a new schema version"""
        if topic not in self.schemas:
            self.schemas[topic] = {}
            self.schema_versions[topic] = 0
            
        if version is None:
            version = self.schema_versions[topic] + 1
            
        schema_hash = self._compute_schema_hash(schema)
        
        self.schemas[topic][version] = {
            'schema': schema,
            'hash': schema_hash,
            'created_at': datetime.now(),
            'breaking_changes': self._detect_breaking_changes(
                topic, 
                self.schemas[topic].get(version - 1, {}).get('schema'),
                schema
            )
        }
        
        self.schema_versions[topic] = version
        
    def _compute_schema_hash(self, schema: Dict) -> str:
        """Compute a hash of the schema for quick comparison"""
        return hashlib.sha256(
            json.dumps(schema, sort_keys=True).encode()
        ).hexdigest()
        
    def _detect_breaking_changes(self, 
                               topic: str, 
                               old_schema: Optional[Dict], 
                               new_schema: Dict) -> List[str]:
        """Detect breaking changes between schema versions"""
        if not old_schema:
            return []
            
        breaking_changes = []
        
        # Check for removed fields
        old_fields = set(self._flatten_schema(old_schema))
        new_fields = set(self._flatten_schema(new_schema))
        
        removed_fields = old_fields - new_fields
        if removed_fields:
            breaking_changes.append(f"Removed fields: {removed_fields}")
            
        # Check for type changes
        for field in old_fields & new_fields:
            old_type = self._get_field_type(old_schema, field)
            new_type = self._get_field_type(new_schema, field)
            if old_type != new_type:
                breaking_changes.append(
                    f"Type change for {field}: {old_type} -> {new_type}"
                )
                
        return breaking_changes
        
    def _flatten_schema(self, schema: Dict, prefix: str = "") -> List[str]:
        """Flatten nested schema into list of field paths"""
        fields = []
        for key, value in schema.items():
            full_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict) and 'type' not in value:
                fields.extend(self._flatten_schema(value, full_key))
            else:
                fields.append(full_key)
        return fields

class DataLineageTracker:
    def __init__(self):
        self.lineage_graph = {}
        self.transformations = {}
        
    def track_transformation(self, 
                           source_id: str,
                           target_id: str,
                           transformation_type: str,
                           transformation_details: Dict):
        """Track data transformation for lineage"""
        if source_id not in self.lineage_graph:
            self.lineage_graph[source_id] = {'downstream': set(), 'upstream': set()}
            
        if target_id not in self.lineage_graph:
            self.lineage_graph[target_id] = {'downstream': set(), 'upstream': set()}
            
        self.lineage_graph[source_id]['downstream'].add(target_id)
        self.lineage_graph[target_id]['upstream'].add(source_id)
        
        transformation_id = f"{source_id}->{target_id}"
        self.transformations[transformation_id] = {
            'type': transformation_type,
            'details': transformation_details,
            'timestamp': datetime.now()
        }
        
    def get_upstream_lineage(self, target_id: str) -> Dict:
        """Get all upstream dependencies for a target"""
        visited = set()
        
        def traverse_upstream(node_id):
            if node_id in visited:
                return {}
                
            visited.add(node_id)
            upstream = {}
            
            for up_id in self.lineage_graph.get(node_id, {}).get('upstream', set()):
                transformation_id = f"{up_id}->{node_id}"
                upstream[up_id] = {
                    'transformation': self.transformations.get(transformation_id),
                    'upstream': traverse_upstream(up_id)
                }
                
            return upstream
            
        return traverse_upstream(target_id)

# Example usage
if __name__ == "__main__":
    # Schema evolution example
    registry = SchemaRegistry()
    
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
    
    # Lineage tracking example
    lineage = DataLineageTracker()
    
    # Track data flow
    lineage.track_transformation(
        "raw_events",
        "cleaned_events",
        "cleanup",
        {
            "operations": ["deduplication", "timestamp_standardization"],
            "filters": ["removed_null_user_ids"]
        }
    )
    
    lineage.track_transformation(
        "cleaned_events",
        "aggregated_metrics",
        "aggregation",
        {
            "operations": ["hourly_rollup", "user_count"],
            "grouping_keys": ["event_type", "source"]
        }
    )
    
    # Get upstream lineage for metrics
    upstream = lineage.get_upstream_lineage("aggregated_metrics")
    print(json.dumps(upstream, indent=2))