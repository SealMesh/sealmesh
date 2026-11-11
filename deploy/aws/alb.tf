# The load balancer terminates TLS and forwards plain HTTP (and the WebSocket
# upgrade) to the router's one task. The router refuses to serve plaintext off
# loopback unless told what terminates TLS, so the task sets
# SEALMESH_TLS_TERMINATED_BY=alb.

resource "aws_lb" "router" {
  #checkov:skip=CKV_AWS_91:Access logs need an S3 bucket and policy; left to the operator. The router logs every refused login itself.
  #checkov:skip=CKV_AWS_150:Deletion protection would make terraform destroy fail; nothing is stored on the load balancer.
  #checkov:skip=CKV2_AWS_28:WAF is a monthly cost this one-team recipe does not assume. Every request still needs a valid GitHub token from an org member.
  name                       = var.name
  internal                   = false
  load_balancer_type         = "application"
  security_groups            = [aws_security_group.alb.id]
  subnets                    = var.alb_subnet_ids
  idle_timeout               = var.alb_idle_timeout
  drop_invalid_header_fields = true
  enable_deletion_protection = false
}

resource "aws_lb_target_group" "router" {
  name                 = "${var.name}-router"
  port                 = local.router_port
  protocol             = "HTTP"
  target_type          = "ip"
  vpc_id               = var.vpc_id
  deregistration_delay = 15

  health_check {
    path                = "/v1/healthz"
    matcher             = "200"
    interval            = 15
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.router.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = var.ssl_policy
  certificate_arn   = local.certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.router.arn
  }
}

resource "aws_lb_listener" "http_redirect" {
  load_balancer_arn = aws_lb.router.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = "redirect"

    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}

# --- Certificate: bring your own, or have one issued for domain_name. ---

resource "aws_acm_certificate" "router" {
  count = var.certificate_arn == null ? 1 : 0

  domain_name       = var.domain_name
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true

    precondition {
      condition     = var.route53_zone_id != null
      error_message = "Set certificate_arn, or set route53_zone_id so the module can validate a new certificate by DNS."
    }
  }
}

resource "aws_route53_record" "certificate_validation" {
  for_each = var.certificate_arn == null ? {
    for option in aws_acm_certificate.router[0].domain_validation_options : option.domain_name => option
  } : {}

  zone_id         = var.route53_zone_id
  name            = each.value.resource_record_name
  type            = each.value.resource_record_type
  records         = [each.value.resource_record_value]
  ttl             = 300
  allow_overwrite = true
}

resource "aws_acm_certificate_validation" "router" {
  count = var.certificate_arn == null ? 1 : 0

  certificate_arn         = aws_acm_certificate.router[0].arn
  validation_record_fqdns = [for record in aws_route53_record.certificate_validation : record.fqdn]
}

locals {
  certificate_arn = var.certificate_arn != null ? var.certificate_arn : aws_acm_certificate_validation.router[0].certificate_arn
}

resource "aws_route53_record" "router" {
  count = var.route53_zone_id != null ? 1 : 0

  zone_id = var.route53_zone_id
  name    = var.domain_name
  type    = "A"

  alias {
    name                   = aws_lb.router.dns_name
    zone_id                = aws_lb.router.zone_id
    evaluate_target_health = true
  }
}
