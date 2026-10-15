output "public" {
  value = {
    environment_id  = confluent_environment.lab.id
    catalog         = confluent_environment.lab.display_name
    cluster_id      = confluent_kafka_cluster.lab.id
    database        = confluent_kafka_cluster.lab.display_name
    organization_id = data.confluent_organization.current.id
    bootstrap       = confluent_kafka_cluster.lab.bootstrap_endpoint
    kafka_rest      = confluent_kafka_cluster.lab.rest_endpoint
    registry_url    = data.confluent_schema_registry_cluster.lab.rest_endpoint
    flink_url       = data.confluent_flink_region.lab.rest_endpoint
    pool_id         = confluent_flink_compute_pool.lab.id
    operator_id     = confluent_service_account.identity["operator"].id
    writer_id       = confluent_service_account.identity["writer"].id
    reader_id       = confluent_service_account.identity["reader"].id
    connector_id    = try(confluent_connector.postgres[0].id, null)
  }
}
output "credentials" {
  sensitive = true
  value = {
    operator_kafka    = { key = confluent_api_key.kafka["operator"].id, secret = confluent_api_key.kafka["operator"].secret }
    reader_kafka      = { key = confluent_api_key.kafka["reader"].id, secret = confluent_api_key.kafka["reader"].secret }
    operator_registry = { key = confluent_api_key.registry["operator"].id, secret = confluent_api_key.registry["operator"].secret }
    reader_registry   = { key = confluent_api_key.registry["reader"].id, secret = confluent_api_key.registry["reader"].secret }
    flink             = { key = confluent_api_key.flink.id, secret = confluent_api_key.flink.secret }
  }
}
