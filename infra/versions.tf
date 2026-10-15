terraform {
  required_version = ">= 1.9, < 2.0"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.91.0"
    }
  }
}

provider "confluent" {}
