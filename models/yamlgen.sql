{{
    codegen.generate_source(
        schema_name="wh_raw",
        table_names=["trading_otr_deals_real"],
        generate_columns=True,
        include_descriptions=True,
    )
}}
