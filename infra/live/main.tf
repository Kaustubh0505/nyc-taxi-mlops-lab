# S0 root module: the lab uses the account's default VPC and its public subnets.
# Hosts keep stable public IPs through Elastic IPs; RDS remains private.
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default_vpc" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_subnet" "host" {
  availability_zone = var.availability_zone

  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "availability-zone"
    values = [var.availability_zone]
  }
}

module "host" {
  for_each = {
    platform = var.platform_instance_type
    app      = var.app_instance_type
    llm      = var.llm_instance_type
  }

  source             = "../modules/host"
  role               = each.key
  instance_type      = each.value
  subnet_id          = data.aws_subnet.host.id
  security_group_ids = [aws_security_group.host[each.key].id]
}

module "rds" {
  source             = "../modules/rds"
  instance_class     = var.rds_instance_class
  subnet_ids         = data.aws_subnets.default_vpc.ids
  security_group_ids = [aws_security_group.rds.id]
}

module "artifact_bucket" {
  source = "../modules/bucket"
  name   = "mlops-lab-artifacts"
  tags   = { project = "mlops-lab" }
}

module "wiki_secret" {
  source      = "../modules/secret"
  name        = "lab/wiki"
  description = "Wiki API credentials for the lab assistant."
}
