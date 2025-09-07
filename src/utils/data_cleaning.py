import hashlib

def generate_unique_id(row, cols):
    combined = "_".join(str(row[c]) for c in cols)
    return hashlib.md5(combined.encode()).hexdigest()

def rename_colnames_using_mapping_object(df_name, df, mapping_object):
    mapping = {}
    for df_mapping in mapping_object.get(df_name, []):
        prefix = df_mapping.get("prefix", "")
        columns = df_mapping.get("columns", [])
        mapping.update({col: f"{prefix}{col}" for col in columns})
    return df.rename(columns=mapping)