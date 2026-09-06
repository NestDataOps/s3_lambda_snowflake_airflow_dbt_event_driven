output "public_ip" {
  value = aws_instance.airflow.public_ip
}

output "instance_id" {
  value = aws_instance.airflow.id
}

# Writes ansible/inventory.ini automatically so you don't hand-copy the IP
# after every apply (the IP changes on stop/start unless you add an EIP).
resource "local_file" "ansible_inventory" {
  filename = "${path.module}/../../../ansible/inventory.ini"
  content  = <<-EOT
    [airflow]
    ${aws_instance.airflow.public_ip} ansible_user=ubuntu
  EOT
}
