terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}
provider "aws" { region = var.region }
variable "region" { default = "eu-west-1" }
variable "instance_type" { default = "t3.large" }
variable "repository_url" {
  type = string
  description = "Public HTTPS Git repository URL containing this project"
}
variable "revision" {
  type = string
  description = "Git commit hash or release tag to deploy"
}
variable "viewer_cidr" {
  type = string
  description = "Allowed dashboard viewers; use your IP/32 for a private demo, 0.0.0.0/0 for a public demo"
}
data "aws_vpc" "default" { default = true }
data "aws_subnets" "default" {
  filter {
    name = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}
data "aws_ami" "linux" {
  most_recent = true
  owners = ["amazon"]
  filter {
    name = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }
  filter {
    name = "virtualization-type"
    values = ["hvm"]
  }
}
resource "aws_security_group" "demo" {
  name_prefix = "order-pulse-"
  vpc_id = data.aws_vpc.default.id
  ingress {
    from_port = 8090
    to_port = 8090
    protocol = "tcp"
    cidr_blocks = [var.viewer_cidr]
  }
  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
resource "aws_iam_role" "ssm" {
  name_prefix = "order-pulse-ssm-"
  assume_role_policy = jsonencode({Version="2012-10-17",Statement=[{Effect="Allow",Principal={Service="ec2.amazonaws.com"},Action="sts:AssumeRole"}]})
}
resource "aws_iam_role_policy_attachment" "ssm" {
  role = aws_iam_role.ssm.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}
resource "aws_iam_instance_profile" "ssm" { role = aws_iam_role.ssm.name }
resource "aws_instance" "demo" {
  ami = data.aws_ami.linux.id
  instance_type = var.instance_type
  subnet_id = sort(data.aws_subnets.default.ids)[0]
  associate_public_ip_address = true
  vpc_security_group_ids = [aws_security_group.demo.id]
  iam_instance_profile = aws_iam_instance_profile.ssm.name
  user_data = templatefile("${path.module}/user-data.sh.tftpl",{repository_url=base64encode(var.repository_url),revision=base64encode(var.revision)})
  user_data_replace_on_change = true
  metadata_options { http_tokens = "required" }
  root_block_device {
    volume_size = 30
    volume_type = "gp3"
    encrypted = true
  }
  tags = { Name = "order-pulse-demo" }
}
output "demo_url" { value = "http://${aws_instance.demo.public_ip}:8090" }
output "instance_id" { value = aws_instance.demo.id }
