locals {
  # Determine the correct file path based on the view_type variable
  queries_file = var.view_type == "encrypted" ? "../prod/authorized_view_service/templates/auth_view_mapping.txt" : "../prod/authorized_view_service/templates/auth_view_mapping_non_encrypted.txt"

  # Load schema-table queries from the appropriate file
  schema_table_queries = tomap({
    for line in split("\n", trimspace(file(local.queries_file))) : 
    "${split("|", line)[0]}.${split("|", line)[1]}" => {
      schema = split("|", line)[0]
      table  = split("|", line)[1]
      query  = split("|", line)[2]
    }
  })

  # Extract unique schemas from the queries
  unique_schemas = distinct([for values in local.schema_table_queries : values.schema])
}
