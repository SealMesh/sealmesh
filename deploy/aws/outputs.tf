output "router_url" {
  description = "What daemons put in [server] url."
  value       = "https://${var.domain_name}"
}

output "alb_dns_name" {
  description = "The load balancer's hostname. Without route53_zone_id, point a CNAME for domain_name here."
  value       = aws_lb.router.dns_name
}

output "log_group" {
  description = "The CloudWatch log group holding the router's output."
  value       = aws_cloudwatch_log_group.router.name
}

output "room_history_table" {
  description = "The DynamoDB table holding seven days of room messages."
  value       = aws_dynamodb_table.room_history.name
}

output "ecs_service" {
  description = "Cluster and service names, for redeploys and rollbacks."
  value       = "${aws_ecs_cluster.router.name}/${aws_ecs_service.router.name}"
}

output "image" {
  description = "The image the task runs."
  value       = local.image
}
