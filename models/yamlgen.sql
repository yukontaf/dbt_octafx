{{
    codegen.generate_source(
        schema_name="amplitude",
        table_names=["events_octa_raw_filled_personal_info"],
        generate_columns=True,
        include_descriptions=True,
    )
}}
