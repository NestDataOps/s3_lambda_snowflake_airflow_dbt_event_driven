# EC2 host running Airflow directly (no Docker): webserver + scheduler +
# triggerer as systemd services, SQLite metadata DB + SequentialExecutor.
# That combo is intentionally minimal -- fine for a single linear DAG run
# occasionally, and comfortable on a t3a.small's 2GB RAM. Swap for
# Postgres + LocalExecutor later if you need concurrent DAG runs.

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_security_group" "airflow" {
  name        = "${var.project_name}-airflow-sg"
  description = "SSH + Airflow webserver, restricted to your IP"

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  ingress {
    description = "Airflow webserver UI"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "airflow" {
  key_name   = "${var.project_name}-airflow-key"
  public_key = var.ssh_public_key
}

# Instance role: lets the S3KeySensor and boto3 calls on the box use the
# instance profile instead of long-lived AWS keys in an Airflow connection.
resource "aws_iam_role" "airflow_ec2" {
  name = "${var.project_name}-airflow-ec2"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "airflow_s3_access" {
  name = "${var.project_name}-airflow-s3-access"
  role = aws_iam_role.airflow_ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          var.raw_bucket_arn,
          "${var.raw_bucket_arn}/*",
          var.processed_bucket_arn,
          "${var.processed_bucket_arn}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "airflow_ec2" {
  name = "${var.project_name}-airflow-ec2"
  role = aws_iam_role.airflow_ec2.name
}

resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.airflow.key_name
  vpc_security_group_ids = [aws_security_group.airflow.id]
  iam_instance_profile   = aws_iam_instance_profile.airflow_ec2.name

  root_block_device {
    volume_size = 20 # dbt + airflow + venv + logs add up faster than you'd think
    volume_type = "gp3"
  }

  tags = {
    Name = "${var.project_name}-airflow"
  }
}
