# S0 security groups. Inbound internet access is limited to the app's Traefik port;
# the scope remains configurable through ingress_cidrs for the student's decision.
resource "aws_security_group" "host" {
  for_each = toset(["platform", "app", "llm"])

  name        = "lab-${each.key}"
  description = "Lab ${each.key} host"
  vpc_id      = data.aws_vpc.default.id

  tags = { Name = "lab-${each.key}" }
}

resource "aws_vpc_security_group_ingress_rule" "app_http" {
  for_each = toset(var.ingress_cidrs)

  security_group_id = aws_security_group.host["app"].id
  description       = "Public HTTP ingress to Traefik"
  cidr_ipv4         = each.value
  from_port         = 80
  to_port           = 80
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "app_to_platform_mlflow" {
  security_group_id            = aws_security_group.host["platform"].id
  description                  = "App host to MLflow"
  referenced_security_group_id = aws_security_group.host["app"].id
  from_port                    = 5000
  to_port                      = 5000
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "clients_to_llm" {
  for_each = toset(["app", "platform"])

  security_group_id            = aws_security_group.host["llm"].id
  description                  = "${each.key} host to LLM APIs"
  referenced_security_group_id = aws_security_group.host[each.key].id
  from_port                    = 8080
  to_port                      = 8081
  ip_protocol                  = "tcp"
}

resource "aws_security_group" "rds" {
  name        = "lab-rds"
  description = "Private Postgres for platform and app services"
  vpc_id      = data.aws_vpc.default.id

  tags = { Name = "lab-rds" }
}

resource "aws_vpc_security_group_ingress_rule" "clients_to_rds" {
  for_each = toset(["app", "platform"])

  security_group_id            = aws_security_group.rds.id
  description                  = "${each.key} host to Postgres"
  referenced_security_group_id = aws_security_group.host[each.key].id
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "host_all" {
  for_each = aws_security_group.host

  security_group_id = each.value.id
  description       = "Host outbound access"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_vpc_security_group_egress_rule" "rds_all" {
  security_group_id = aws_security_group.rds.id
  description       = "RDS outbound access"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}
