# Enable the notebook when the public repository has been supplied.
module "notebook" {
  count     = var.github_repo == null ? 0 : 1
  source    = "../modules/notebook"
  subnet_id = data.aws_subnet.host.id
  repo_url  = "https://github.com/${var.github_repo}.git"
  env = {
    MLFLOW_TRACKING_URI = "http://${module.host["platform"].private_ip}:5000"
    REDSHIFT_CLUSTER_ID = module.redshift.cluster_id
    REDSHIFT_DATABASE   = module.redshift.database
    REDSHIFT_DB_USER    = "labadmin"
  }
}

resource "aws_vpc_security_group_ingress_rule" "notebook_to_mlflow" {
  count                        = var.github_repo == null ? 0 : 1
  security_group_id            = aws_security_group.host["platform"].id
  referenced_security_group_id = module.notebook[0].security_group_id
  description                  = "Notebook to MLflow"
  from_port                    = 5000
  to_port                      = 5000
  ip_protocol                  = "tcp"
}

output "notebook_name" {
  value = try(module.notebook[0].name, null)
}
