# utils/column_lineage_extractor.py
import json
import re
import yaml
import os
from collections import defaultdict

class ColumnLineageExtractor:
    def __init__(self, manifest_path, catalog_path):
        self.manifest = self._load_json(manifest_path)
        self.catalog = self._load_json(catalog_path)
        self.lineage_map = defaultdict(dict)

    def _load_json(self, path):
        with open(path, 'r') as f:
            return json.load(f)

    def _extract_model_dependencies(self, model):
        return self.manifest['nodes'][model].get('depends_on', {}).get('nodes', [])

    def _extract_columns(self, model):
        print(f"Checking columns for model: {model}")
        for unique_id, source_data in self.catalog.items():
            if unique_id == model:
                if 'columns' in source_data:
                    return list(source_data['columns'].keys())
        if 'nodes' in self.catalog and model in self.catalog['nodes']:
            return list(self.catalog['nodes'][model]['columns'].keys())
        print(f"WARNING: Model {model} not found in catalog.")
        return []

    def _parse_select_columns(self, sql_code, model):
        select_pattern = r"SELECT\s+\*\s+FROM"
        match = re.search(select_pattern, sql_code, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return {col: col for col in self._extract_columns(model)}

        select_pattern = r"SELECT\s+(.*?)\s+FROM"
        match = re.search(select_pattern, sql_code, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            return {}

        select_part = match.group(1)
        column_mappings = {}

        for column in select_part.split(','):
            column = column.strip()
            match = re.match(r"(\w+)\s+AS\s+(\w+)", column, flags=re.IGNORECASE)
            if match:
                original, alias = match.groups()
                column_mappings[alias] = original
            else:
                column_mappings[column] = column

        return column_mappings

    def _extract_column_lineage(self, model):
        if model not in self.manifest['nodes']:
            return {}

        model_data = self.manifest['nodes'][model]
        
        if model_data['resource_type'] == 'source':
            column_mappings = {col: col for col in self._extract_columns(model)}
            for column, original in column_mappings.items():
                self.lineage_map[model][column] = {
                    "source_model": model,
                    "source_column": original,
                }
            return {}

        sql_code = model_data.get('compiled_code') or model_data.get('raw_code')

        if not sql_code:
            return {}

        column_mappings = self._parse_select_columns(sql_code, model)
        dependencies = self._extract_model_dependencies(model)

        for upstream_model in dependencies:
            upstream_columns = self._extract_columns(upstream_model)
            for alias, original in column_mappings.items():
                if original in upstream_columns:
                    self.lineage_map[model][alias] = {
                        "source_model": upstream_model,
                        "source_column": original,
                    }

    def build_lineage_map(self):
        for model in self.catalog.get('nodes', {}):
            if model.endswith('_r') or model.startswith("source."):
                column_mappings = {col: col for col in self._extract_columns(model)}
                for column, original in column_mappings.items():
                    self.lineage_map[model][column] = {
                        "source_model": model,
                        "source_column": original,
                    }

        for model in self.manifest['nodes']:
            if self.manifest['nodes'][model]['resource_type'] == 'model':
                self._extract_column_lineage(model)

        for model, source_data in self.catalog.items():
            if model.startswith("source.") and model not in self.manifest['nodes']:
                column_mappings = {col: col for col in self._extract_columns(model)}
                for column, original in column_mappings.items():
                    self.lineage_map[model][column] = {
                        "source_model": model,
                        "source_column": original,
                    }

        return self.lineage_map

    def write_lineage_to_yaml(self, output_file):
        clean_lineage_map = self._convert_defaultdict_to_dict(self.lineage_map)
        with open(output_file, 'w') as yaml_file:
            yaml.dump(clean_lineage_map, yaml_file, default_flow_style=False)

    def _convert_defaultdict_to_dict(self, obj):
        if isinstance(obj, defaultdict):
            obj = {k: self._convert_defaultdict_to_dict(v) for k, v in obj.items()}
        elif isinstance(obj, dict):
            obj = {k: self._convert_defaultdict_to_dict(v) for k, v in obj.items()}
        return obj
