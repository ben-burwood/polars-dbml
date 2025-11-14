import argparse
import ast


def polars_to_dbml_type(pl_type: str) -> str:
    """Convert Polars data type (e.g. pl.String) to DBML type"""
    if "List" in pl_type:
        return "array"
    if "Int" in pl_type:
        return "int"
    if "Float" in pl_type:
        return "float"
    if "String" in pl_type or "Utf8" in pl_type:
        return "varchar"
    if "Boolean" in pl_type:
        return "bool"
    if "Datetime" in pl_type:
        return "timestamp"
    return "varchar"


def schema_to_dbml(table_name: str, schema: dict[str, str]):
    dbml = f"Table {table_name} {{\n"
    for col, dtype in schema.items():
        dbml_type = polars_to_dbml_type(dtype)
        dbml += f"  {col} {dbml_type}\n"
    dbml += "}"
    return dbml


def extract_schemas_from_python(tree: ast.Module) -> dict[str, dict[str, str]]:
    """Extracts Polars Schema(s) from Python AST Interpreted Code"""
    schemas = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and isinstance(node.value, ast.Dict):
                    # Extract Keys and Values as Strings
                    keys = []
                    values = []
                    for k, v in zip(node.value.keys, node.value.values):
                        if k is None and isinstance(v, ast.Name):
                            # This is a dict unpack (**other_schema)
                            keys.append(None)
                            values.append(v.id)
                        else:
                            if isinstance(k, ast.Constant):
                                keys.append(k.value)
                            else:
                                keys.append(str(k))

                            if isinstance(v, ast.Constant):
                                values.append(str(v.value))
                            elif isinstance(v, ast.Attribute):
                                # Handles pl.Int64, pl.String, etc.
                                values.append(f"{v.value.id}.{v.attr}")
                            else:
                                values.append(str(v))
                    schemas[target.id] = (keys, values)

    resolved_schemas = {}
    for name, (keys, values) in schemas.items():
        schema = {}
        for k, v in zip(keys, values):
            if k is None:
                # Unpack referenced schema
                ref_schema = resolved_schemas.get(v)
                if ref_schema:
                    schema.update(ref_schema)
            else:
                schema[k] = v
        resolved_schemas[name] = schema
    return resolved_schemas


def main():
    parser = argparse.ArgumentParser(description="Convert Polars Schema to DBML")
    parser.add_argument("--input", "-i", type=str, help="Path to the Polars Schema (.py)")
    parser.add_argument("--output", "-o", type=str, help="Path to the output DBML file (.dbml)")
    args = parser.parse_args()

    with open(args.input, "r") as f:
        source = f.read()
    tree = ast.parse(source, filename=args.input)

    schemas = extract_schemas_from_python(tree)
    dbml = []
    for table_name, schema in schemas.items():
        table_dbml = f"Table {table_name} {{\n"
        for col, dtype in schema.items():
            dbml_type = polars_to_dbml_type(dtype)
            table_dbml += f"  {col} {dbml_type}\n"
        table_dbml += "}"
        dbml.append(table_dbml)

    print("\n\n".join(dbml))
    if args.output:
        with open(args.output, "w") as f:
            f.write("\n\n".join(dbml))


if __name__ == "__main__":
    main()
