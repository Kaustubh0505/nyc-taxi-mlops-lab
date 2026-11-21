# S1 Redshift warehouse. The provided module uses LabRole and the Data API; no database password or
# ingress port is needed by Dagster.
module "redshift" {
  source            = "../modules/redshift"
  name              = "lab"
  database          = "lab"
  subnet_ids        = data.aws_subnets.default_vpc.ids
  availability_zone = var.availability_zone
}
