# S0 outputs consumed by scripts and listed in contracts/runtime.yaml.
output "app_public_ip" {
  description = "Stable public address for the app host."
  value       = module.host["app"].public_ip
}

output "app_instance_id" {
  value = module.host["app"].instance_id
}

output "platform_instance_id" {
  value = module.host["platform"].instance_id
}

output "llm_instance_id" {
  value = module.host["llm"].instance_id
}

output "rds_endpoint" {
  value = module.rds.endpoint
}

output "artifact_bucket" {
  value = module.artifact_bucket.name
}
