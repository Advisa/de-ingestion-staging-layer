# Define a local variable
locals {
  schema_table_queries =  tomap({
    for line in split("\n", trimspace(file("/Users/duygugenc/Documents/de-ingestion-staging-layer/prod/modules/auth_views/authorized_view_scripts/auth_view_mapping.txt"))): 
      "${split("|", line)[0]}.${split("|",line)[1]}" => {
        schema = split("|", line)[0]
        table  = split("|", line)[1]
        query = split("|",line)[2]

      }    
  }) 
  unique_schemas = distinct([for values in local.schema_table_queries : values.schema])
}