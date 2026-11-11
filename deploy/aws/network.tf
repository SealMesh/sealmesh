# Two security groups. The load balancer takes HTTPS from anywhere (and HTTP,
# only to redirect it), and may send only to the router's port. The task takes
# traffic only from the load balancer, and may send only HTTPS out: GitHub's
# API, ghcr.io, DynamoDB, CloudWatch Logs and SSM are all port 443.

resource "aws_security_group" "alb" {
  name        = "${var.name}-alb"
  description = "SealMesh router load balancer"
  vpc_id      = var.vpc_id
}

resource "aws_security_group" "task" {
  name        = "${var.name}-task"
  description = "SealMesh router task, reachable only from its load balancer"
  vpc_id      = var.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "alb_http" {
  #checkov:skip=CKV_AWS_260:Port 80 only answers with a 301 to HTTPS; the listener has no forward action.
  security_group_id = aws_security_group.alb.id
  description       = "HTTP from anywhere, redirected to HTTPS"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
}

resource "aws_vpc_security_group_ingress_rule" "alb_https" {
  security_group_id = aws_security_group.alb.id
  description       = "HTTPS from anywhere: daemons connect from wherever their machines are"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

resource "aws_vpc_security_group_egress_rule" "alb_to_task" {
  security_group_id            = aws_security_group.alb.id
  description                  = "To the router task only"
  referenced_security_group_id = aws_security_group.task.id
  ip_protocol                  = "tcp"
  from_port                    = local.router_port
  to_port                      = local.router_port
}

resource "aws_vpc_security_group_ingress_rule" "task_from_alb" {
  security_group_id            = aws_security_group.task.id
  description                  = "From the load balancer only"
  referenced_security_group_id = aws_security_group.alb.id
  ip_protocol                  = "tcp"
  from_port                    = local.router_port
  to_port                      = local.router_port
}

resource "aws_vpc_security_group_egress_rule" "task_https" {
  security_group_id = aws_security_group.task.id
  description       = "HTTPS out: GitHub API, ghcr.io, DynamoDB, CloudWatch Logs, SSM"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}
