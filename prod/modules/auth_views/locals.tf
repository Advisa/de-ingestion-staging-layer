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
  maxwell_schema_table_queries = tomap({
    for line in split("\n", trimspace(file("../prod/authorized_view_service/templates/auth_view_mapping_maxwell.txt"))) :
    "${split("|", line)[0]}.${split("|", line)[1]}" => {
      schema = split("|", line)[0]
      table  = split("|", line)[1]
      query  = split("|", line)[2]

    }
  })

  rahalaitos_schema_table_queries = tomap({
    for line in split("\n", trimspace(file("../prod/authorized_view_service/templates/auth_view_mapping_rahalaitos.txt"))) :
    "${split("|", line)[0]}.${split("|", line)[1]}" => {
      schema = split("|", line)[0]
      table  = split("|", line)[1]
      query  = join("|", slice(split("|", line), 2, length(split("|", line))))

    }
  })

  cdc_schema_table_queries = tomap({
    for line in split("\n", trimspace(file("../prod/authorized_view_service/templates/auth_view_mapping_cdc.txt"))) :
    "${split("|", line)[0]}.${split("|", line)[1]}" => {
      schema = split("|", line)[0]
      table  = split("|", line)[1]
      query  = join("|", slice(split("|", line), 2, length(split("|", line))))
      table_id = "${split("|", line)[1]}${(endswith(split("|", line)[0], "_fi") ? "_fi" : (endswith(split("|", line)[0], "_no") ? "_no" : ""))}"

    }
  })

  cdc_schema_table_queries_prod = tomap({
    for line in split("\n", trimspace(file("../prod/authorized_view_service/templates/auth_view_mapping_cdc_prod.txt"))) :
    "${split("|", line)[0]}.${split("|", line)[1]}" => {
      schema = split("|", line)[0]
      table  = split("|", line)[1]
      query  = join("|", slice(split("|", line), 2, length(split("|", line))))
      table_id = "${split("|", line)[1]}${(endswith(split("|", line)[0], "_fi") ? "_fi" : (endswith(split("|", line)[0], "_no") ? "_no" : ""))}"

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

  prod_schema_table_queries = tomap({
    for line in split("\n", trimspace(file("../prod/authorized_view_service/templates/auth_view_mapping.txt"))) :
    "${split("|", line)[0]}.${split("|", line)[1]}" => {
      schema = split("|", line)[0]
      table  = split("|", line)[1]
      query  = join("|", slice(split("|", line), 2, length(split("|", line))))

    }
  })
  unique_schemas = distinct([for values in local.unencrypted_schema_table_queries : values.schema])
  unique_schemas_cdc = distinct([for values in local.cdc_schema_table_queries : values.schema])
  unique_schemas_prod = distinct([for values in local.prod_schema_table_queries : values.schema])
  unique_schemas_cdc_prod = distinct([for values in local.cdc_schema_table_queries_prod : values.schema])

}
