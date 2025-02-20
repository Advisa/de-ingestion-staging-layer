# Define a local variable
locals {
 
  lvs_schema_table_queries = tomap({
    for line in split("\n", trimspace(file("../prod/authorized_view_service/templates/auth_view_mapping_lvs.txt"))) :
    "${split("|", line)[0]}.${split("|", line)[1]}" => {
      schema = split("|", line)[0]
      table  = split("|", line)[1]
      query  = split("|", line)[2]

    }
  })
  salus_schema_table_queries = tomap({
    for line in split("\n", trimspace(file("../prod/authorized_view_service/templates/auth_view_mapping_salus.txt"))) :
    "${split("|", line)[0]}.${split("|", line)[1]}" => {
      schema = split("|", line)[0]
      table  = split("|", line)[1]
      query  = split("|", line)[2]

    }
  })
  sambla_legacy_schema_table_queries = tomap({
    for line in split("\n", trimspace(file("../prod/authorized_view_service/templates/auth_view_mapping_sambla_legacy.txt"))) :
    "${split("|", line)[0]}.${split("|", line)[1]}" => {
      schema = split("|", line)[0]
      table  = split("|", line)[1]
      query  = split("|", line)[2]

    }
  })
  advisa_history_schema_table_queries = tomap({
    for line in split("\n", trimspace(file("../prod/authorized_view_service/templates/auth_view_mapping_advisa_history.txt"))) :
    "${split("|", line)[0]}.${split("|", line)[1]}" => {
      schema = split("|", line)[0]
      table  = split("|", line)[1]
      query  = split("|", line)[2]

    }
  })
  unencrypted_schema_table_queries = tomap({
    for line in split("\n", trimspace(file("../prod/authorized_view_service/templates/auth_view_mapping_non_encrypted.txt"))) :
    "${split("|", line)[0]}.${split("|", line)[1]}" => {
      schema = split("|", line)[0]
      table  = split("|", line)[1]
      query  = split("|", line)[2]

    }
  })
  unique_schemas = distinct([for values in local.unencrypted_schema_table_queries : values.schema])
}
