variable "region" {
  type    = string
  default = "us-east-2"
}

variable "max_cfu" {
  type    = number
  default = 5
  validation {
    condition     = var.max_cfu >= 5 && var.max_cfu <= 10
    error_message = "O laboratorio limita o pool a 5-10 CFUs."
  }
}

variable "enable_connector" {
  type    = bool
  default = false
}

variable "pg_host" {
  type    = string
  default = ""
}
variable "pg_port" {
  type    = number
  default = 5432
}
variable "pg_database" {
  type    = string
  default = "payments"
}
variable "cdc_user" {
  type    = string
  default = "payments_cdc"
}
variable "cdc_password" {
  type      = string
  default   = ""
  sensitive = true
}
