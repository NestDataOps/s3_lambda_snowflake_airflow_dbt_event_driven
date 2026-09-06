variable "project_name" {
  type = string
}

variable "instance_type" {
  type    = string
  default = "t3a.small"
}

variable "ssh_public_key" {
  description = "Contents of your ~/.ssh/id_ed25519.pub (or similar) -- not the private key"
  type        = string
}

variable "allowed_cidr" {
  description = "Your IP in CIDR form (e.g. 1.2.3.4/32) -- SSH and the Airflow UI are restricted to this"
  type        = string
}

variable "raw_bucket_arn" {
  type = string
}

variable "processed_bucket_arn" {
  type = string
}
