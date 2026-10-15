resource "confluent_environment" "lab" {
  display_name = "payments-lab"
  stream_governance {
    package = "ESSENTIALS"
  }
}

resource "confluent_kafka_cluster" "lab" {
  display_name = "payments"
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = var.region
  basic {}
  environment {
    id = confluent_environment.lab.id
  }
}

data "confluent_organization" "current" {}
data "confluent_schema_registry_cluster" "lab" {
  environment {
    id = confluent_environment.lab.id
  }
  depends_on = [confluent_environment.lab]
}
data "confluent_flink_region" "lab" {
  cloud  = "AWS"
  region = var.region
}

resource "confluent_service_account" "identity" {
  for_each = {
    operator = "Provisionamento, SQL e diagnostico do ambiente isolado"
    writer   = "CDC: escreve somente nos topicos de origem"
    reader   = "Aplicacao: le somente os resultados"
  }
  display_name = "payments-${each.key}"
  description  = each.value
}

# A identidade de operacao e separada das duas identidades de aplicacao.
resource "confluent_role_binding" "operator" {
  principal   = "User:${confluent_service_account.identity["operator"].id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.lab.resource_name
}
resource "confluent_role_binding" "flink" {
  principal   = "User:${confluent_service_account.identity["operator"].id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = confluent_environment.lab.resource_name
}
resource "confluent_role_binding" "assigner" {
  principal   = "User:${confluent_service_account.identity["operator"].id}"
  role_name   = "Assigner"
  crn_pattern = "${data.confluent_organization.current.resource_name}/service-account=${confluent_service_account.identity["operator"].id}"
}

resource "confluent_api_key" "kafka" {
  for_each     = toset(["operator", "reader"])
  display_name = "payments-${each.key}-kafka"
  owner {
    id          = confluent_service_account.identity[each.key].id
    api_version = confluent_service_account.identity[each.key].api_version
    kind        = confluent_service_account.identity[each.key].kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.lab.id
    api_version = confluent_kafka_cluster.lab.api_version
    kind        = confluent_kafka_cluster.lab.kind
    environment {
      id = confluent_environment.lab.id
    }
  }
  depends_on = [confluent_role_binding.operator]
}

resource "confluent_role_binding" "reader_schema" {
  principal   = "User:${confluent_service_account.identity["reader"].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.lab.resource_name}/subject=payments.*"
}
resource "confluent_role_binding" "writer_schema" {
  principal   = "User:${confluent_service_account.identity["writer"].id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_schema_registry_cluster.lab.resource_name}/subject=payments.public.*"
}

resource "confluent_api_key" "registry" {
  for_each     = toset(["operator", "reader"])
  display_name = "payments-${each.key}-registry"
  owner {
    id          = confluent_service_account.identity[each.key].id
    api_version = confluent_service_account.identity[each.key].api_version
    kind        = confluent_service_account.identity[each.key].kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.lab.id
    api_version = data.confluent_schema_registry_cluster.lab.api_version
    kind        = data.confluent_schema_registry_cluster.lab.kind
    environment {
      id = confluent_environment.lab.id
    }
  }
  depends_on = [confluent_role_binding.operator, confluent_role_binding.reader_schema]
}

resource "confluent_api_key" "flink" {
  display_name = "payments-operator-flink"
  owner {
    id          = confluent_service_account.identity["operator"].id
    api_version = confluent_service_account.identity["operator"].api_version
    kind        = confluent_service_account.identity["operator"].kind
  }
  managed_resource {
    id          = data.confluent_flink_region.lab.id
    api_version = data.confluent_flink_region.lab.api_version
    kind        = data.confluent_flink_region.lab.kind
    environment {
      id = confluent_environment.lab.id
    }
  }
  depends_on = [confluent_role_binding.flink, confluent_role_binding.assigner]
}

resource "confluent_flink_compute_pool" "lab" {
  display_name = "payments-pool"
  cloud        = "AWS"
  region       = var.region
  max_cfu      = var.max_cfu
  environment {
    id = confluent_environment.lab.id
  }
}

resource "confluent_kafka_topic" "source" {
  for_each = toset(["accounts", "transactions"])
  kafka_cluster {
    id = confluent_kafka_cluster.lab.id
  }
  topic_name       = "payments.public.${each.key}"
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.lab.rest_endpoint
  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "604800000"
  }
  credentials {
    key    = confluent_api_key.kafka["operator"].id
    secret = confluent_api_key.kafka["operator"].secret
  }
}

locals {
  acls = {
    writer_write          = ["writer", "TOPIC", "payments.public.", "PREFIXED", "WRITE"]
    writer_describe       = ["writer", "TOPIC", "payments.public.", "PREFIXED", "DESCRIBE"]
    writer_create         = ["writer", "TOPIC", "payments.public.", "PREFIXED", "CREATE"]
    writer_cluster        = ["writer", "CLUSTER", "kafka-cluster", "LITERAL", "DESCRIBE"]
    writer_idempotent     = ["writer", "CLUSTER", "kafka-cluster", "LITERAL", "IDEMPOTENT_WRITE"]
    writer_preview_create = ["writer", "TOPIC", "data-preview", "PREFIXED", "CREATE"]
    writer_preview_write  = ["writer", "TOPIC", "data-preview", "PREFIXED", "WRITE"]
    reader_alerts         = ["reader", "TOPIC", "payments.fraud_alerts", "LITERAL", "READ"]
    reader_enriched       = ["reader", "TOPIC", "payments.transactions_enriched", "LITERAL", "READ"]
    reader_group          = ["reader", "GROUP", "payments-reader-", "PREFIXED", "READ"]
  }
}
resource "confluent_kafka_acl" "app" {
  for_each = local.acls
  kafka_cluster {
    id = confluent_kafka_cluster.lab.id
  }
  principal     = "User:${confluent_service_account.identity[each.value[0]].id}"
  resource_type = each.value[1]
  resource_name = each.value[2]
  pattern_type  = each.value[3]
  operation     = each.value[4]
  permission    = "ALLOW"
  host          = "*"
  rest_endpoint = confluent_kafka_cluster.lab.rest_endpoint
  credentials {
    key    = confluent_api_key.kafka["operator"].id
    secret = confluent_api_key.kafka["operator"].secret
  }
}

resource "confluent_connector" "postgres" {
  count = var.enable_connector ? 1 : 0
  environment {
    id = confluent_environment.lab.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.lab.id
  }
  config_sensitive = {
    "database.password" = var.cdc_password
  }
  config_nonsensitive = merge(jsondecode(file("${path.module}/../connectors/postgres.json")), {
    "kafka.service.account.id" = confluent_service_account.identity["writer"].id
    "database.hostname"        = var.pg_host
    "database.port"            = tostring(var.pg_port)
    "database.dbname"          = var.pg_database
    "database.user"            = var.cdc_user
  })
  lifecycle {
    precondition {
      condition     = var.pg_host != "" && var.cdc_password != ""
      error_message = "Configure PGHOST e CDC_PASSWORD antes de habilitar o conector."
    }
  }
  depends_on = [confluent_kafka_acl.app, confluent_kafka_topic.source, confluent_role_binding.writer_schema]
}
