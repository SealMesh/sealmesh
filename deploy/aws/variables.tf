variable "region" {
  description = "The AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "name" {
  description = "Prefix for every resource this module creates."
  type        = string
  default     = "sealmesh"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,23}$", var.name))
    error_message = "name must be 2-24 lowercase letters, digits or dashes, starting with a letter (it prefixes the ALB and target group names, which AWS caps at 32 characters)."
  }
}

# --- Network: an existing VPC. The module creates none. ---

variable "vpc_id" {
  description = "The existing VPC to run in."
  type        = string
}

variable "alb_subnet_ids" {
  description = "Public subnets for the load balancer, in at least two Availability Zones."
  type        = list(string)

  validation {
    condition     = length(var.alb_subnet_ids) >= 2
    error_message = "An Application Load Balancer needs subnets in at least two Availability Zones."
  }
}

variable "task_subnet_ids" {
  description = "Subnets for the router task. Private subnets need a NAT gateway (or VPC endpoints) to reach ghcr.io, GitHub and DynamoDB; public subnets need assign_public_ip = true."
  type        = list(string)
}

variable "assign_public_ip" {
  description = "Give the task a public IP. Needed only when task_subnet_ids are public subnets with no NAT gateway. Inbound traffic is still allowed only from the load balancer."
  type        = bool
  default     = false
}

# --- TLS and the router's hostname ---

variable "domain_name" {
  description = "The router's hostname, e.g. router.example.com. Daemons connect to https://<domain_name>."
  type        = string
}

variable "certificate_arn" {
  description = "An issued ACM certificate for domain_name, in this region. Leave null to have the module request one and validate it by DNS in route53_zone_id."
  type        = string
  default     = null
}

variable "route53_zone_id" {
  description = "A Route 53 hosted zone for domain_name. When set, the module creates the alias record (and the certificate's validation records if certificate_arn is null). When null, point a CNAME at the alb_dns_name output yourself."
  type        = string
  default     = null
}

variable "ssl_policy" {
  description = "The HTTPS listener's TLS policy."
  type        = string
  default     = "ELBSecurityPolicy-TLS13-1-2-2021-06"
}

# --- The router ---

variable "router_version" {
  description = "The sealmesh-server release to run, e.g. 0.2.0. The task runs ghcr.io/allocmemory/sealmesh-server:<router_version>-dynamodb. No default: an unpinned tag would change what runs on the next deploy without anyone deciding it."
  type        = string

  validation {
    condition     = can(regex("^[0-9]+\\.[0-9]+\\.[0-9]+(-[0-9A-Za-z.]+)?$", var.router_version))
    error_message = "router_version must be a release version such as 0.2.0, not latest."
  }
}

variable "image_repository" {
  description = "The router image repository. The tag is <router_version>-dynamodb."
  type        = string
  default     = "ghcr.io/allocmemory/sealmesh-server"
}

variable "github_org" {
  description = "The GitHub organization whose active members may connect (SEALMESH_GITHUB_ORG)."
  type        = string
}

variable "projects" {
  description = "The project map (SEALMESH_PROJECTS): each project's id and the repos it spans, as owner/name."
  type = list(object({
    id    = string
    repos = list(string)
  }))
}

variable "extra_environment" {
  description = "Further non-secret router settings, e.g. { SEALMESH_ROOM_TURN_CAP = \"40\" }. See docs/install.md for the list."
  type        = map(string)
  default     = {}
}

variable "secret_parameter_arns" {
  description = "Environment variable name => ARN of an SSM Parameter Store SecureString, injected into the task by ECS at start. The router holds no secret of its own today (it checks every caller with the caller's own GitHub token), so this is empty by default. Never put a secret in extra_environment."
  type        = map(string)
  default     = {}
}

variable "secret_kms_key_arn" {
  description = "The customer-managed KMS key that encrypts the parameters in secret_parameter_arns, if any. Leave null for the AWS-managed aws/ssm key."
  type        = string
  default     = null
}

# --- Size and retention ---

variable "task_cpu" {
  description = "Fargate task CPU units (256 = 0.25 vCPU)."
  type        = number
  default     = 256
}

variable "task_memory" {
  description = "Fargate task memory in MiB."
  type        = number
  default     = 512
}

variable "cpu_architecture" {
  description = "X86_64 or ARM64. The published image is built for the architecture of the release runner; keep X86_64 unless you publish an arm64 image."
  type        = string
  default     = "X86_64"

  validation {
    condition     = contains(["X86_64", "ARM64"], var.cpu_architecture)
    error_message = "cpu_architecture must be X86_64 or ARM64."
  }
}

variable "log_retention_days" {
  description = "How long CloudWatch keeps the router's logs."
  type        = number
  default     = 14
}

variable "alb_idle_timeout" {
  description = "Seconds the load balancer keeps an idle connection. Daemons ping every 30 seconds, so anything above that keeps their WebSockets open."
  type        = number
  default     = 120
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default     = {}
}
