# SealMesh router on AWS

A Terraform recipe that runs the SealMesh router in **your own AWS account**:
one Fargate task behind an Application Load Balancer with HTTPS, room history
in DynamoDB, logs in CloudWatch. It's the same router as everywhere else —
GitHub identity, repo permissions, seven days of room chat and nothing else —
just hosted by you. See [docs/cloud.md](../../docs/cloud.md) for what Cloud
mode is.

**Sized for one small team, not load-tested.** One router serves one GitHub
organization, and it is deliberately one task: the connection registry lives in
the process, so a second task would split your daemons in two.

Nothing here has been deployed by the SealMesh project. CI checks the recipe
statically (`terraform fmt`, `validate`, tflint, checkov). Running
`terraform apply` creates billable resources in your account; see the cost
table below.

## What it creates

| Resource | Why |
| --- | --- |
| ECS cluster, Fargate service (1 task), task definition | Runs `ghcr.io/allocmemory/sealmesh-server:<router_version>-dynamodb` |
| Application Load Balancer, HTTPS listener (443), HTTP listener (80 → 301 to 443), target group | Terminates TLS; health check on `/v1/healthz` |
| ACM certificate + Route 53 validation and alias records | Only when you don't pass `certificate_arn` / do pass `route53_zone_id` |
| Two security groups | Load balancer: 443 and 80 from anywhere, out only to the task. Task: in only from the load balancer, out only on 443 |
| DynamoDB table, on-demand, TTL on `expires_at` | Room history, key `room` (string) + `seq` (number). Point-in-time recovery is off on purpose: its 35-day backups would outlive the 7-day retention |
| IAM task role | `dynamodb:PutItem` and `dynamodb:Query` on that one table. Nothing else |
| IAM execution role | Write the task's log stream; read the SSM parameters you list, if any |
| CloudWatch log group | Router output, 14 days by default |

It creates **no VPC**. You pass an existing VPC and subnets.

## Before you start

- An AWS account and credentials for Terraform (an admin or a role that can
  create the resources above).
- Terraform 1.6 or newer.
- A VPC with **two public subnets in different Availability Zones** for the load
  balancer, and subnets for the task. The task needs outbound HTTPS to
  `ghcr.io`, `api.github.com`, DynamoDB and CloudWatch Logs: either private
  subnets with a NAT gateway, or public subnets with `assign_public_ip = true`
  (it still accepts traffic only from the load balancer). The default VPC's
  subnets work with `assign_public_ip = true`.
- A hostname for the router, and either a Route 53 hosted zone for it or an
  issued ACM certificate for it in the same region.
- A router release that publishes the `-dynamodb` image. The variant is built
  from the first release after v0.1.0 on; v0.1.0 itself has only the SQLite
  image.

## Variables

| Name | Default | What it is |
| --- | --- | --- |
| `region` | `us-east-1` | Region to deploy into |
| `name` | `sealmesh` | Prefix for every resource |
| `vpc_id` | — | Existing VPC |
| `alb_subnet_ids` | — | Public subnets for the load balancer, two AZs or more |
| `task_subnet_ids` | — | Subnets for the task |
| `assign_public_ip` | `false` | Give the task a public IP (public subnets without NAT) |
| `domain_name` | — | Router hostname, e.g. `router.example.com` |
| `certificate_arn` | `null` | Existing ACM certificate; `null` = request one, validated in `route53_zone_id` |
| `route53_zone_id` | `null` | Hosted zone for `domain_name`; creates the alias record (and cert validation) |
| `ssl_policy` | `ELBSecurityPolicy-TLS13-1-2-2021-06` | HTTPS listener TLS policy |
| `router_version` | — | Release to run, e.g. `0.2.0`. No `latest` |
| `image_repository` | `ghcr.io/allocmemory/sealmesh-server` | Image repo; the tag is `<router_version>-dynamodb` |
| `github_org` | — | Org whose active members may connect |
| `projects` | — | Project map: `[{ id = "...", repos = ["org/repo"] }]` |
| `extra_environment` | `{}` | Other non-secret router settings (`SEALMESH_ROOM_TURN_CAP`, ...) |
| `secret_parameter_arns` | `{}` | Env var name → SSM SecureString ARN, injected by ECS. Empty by default: the router has no secret of its own; it checks each caller with the caller's own GitHub token |
| `secret_kms_key_arn` | `null` | Customer-managed KMS key for those parameters, if not `aws/ssm` |
| `task_cpu` / `task_memory` | `256` / `512` | 0.25 vCPU, 0.5 GB |
| `cpu_architecture` | `X86_64` | The published image is x86_64 |
| `log_retention_days` | `14` | CloudWatch Logs retention |
| `alb_idle_timeout` | `120` | Seconds; daemons ping every 30 |
| `tags` | `{}` | Extra tags on everything |

The router's own settings — `SEALMESH_GITHUB_ORG`, `SEALMESH_PROJECTS`,
`SEALMESH_LISTEN`, `SEALMESH_TLS_TERMINATED_BY=alb`,
`SEALMESH_HISTORY_BACKEND=dynamodb`, `DYNAMODB_ROOM_HISTORY` — are set by the
module and win over `extra_environment`.

## Deploy

```sh
cd deploy/aws
cp terraform.tfvars.example terraform.tfvars   # fill it in
terraform init
terraform plan -out=router.tfplan               # read it
terraform apply router.tfplan
```

Keep the state somewhere safe (add an S3 backend block if more than one person
will run this). With a new certificate, `apply` waits for DNS validation, which
usually takes a few minutes.

Check it:

```sh
curl -fsS "$(terraform output -raw router_url)/v1/healthz"
```

## Point daemons at it

On every machine, in `~/.config/sealmesh/config.toml`:

```toml
[server]
url = "https://router.example.com"   # terraform output router_url
```

Then restart the daemon. Nothing else changes: the same `gh` login is the
credential.

## Upgrade and roll back

Change `router_version` and `terraform apply`. The service stops the old task
before starting the new one (two routers would split the registry), so daemons
see a short reconnect. If the new task never passes its health check, the
deployment circuit breaker rolls back to the previous task definition by
itself.

To roll back by hand, set `router_version` to the previous release and apply
again. History is in DynamoDB, so it survives both.

## Remove it

```sh
terraform destroy
```

This deletes the table and its messages too.

## Cost

An **estimate**, for the default size in us-east-1, at 730 hours a month, from
the AWS pricing pages as read on **2026-11-10**. Prices change; check the pages
before you rely on it.

| Item | Rate (us-east-1) | Monthly |
| --- | --- | --- |
| Fargate, 0.25 vCPU | $0.000011244 per vCPU-second | $7.39 |
| Fargate, 0.5 GB memory | $0.000001235 per GB-second | $1.62 |
| ALB hours | $0.0225 per ALB-hour | $16.43 |
| ALB capacity units | $0.008 per LCU-hour; a small team should stay under 1 LCU | up to $5.84 |
| Public IPv4 for the ALB (one per AZ, two AZs) | $0.005 per IP-hour | $7.30 |
| Public IPv4 for the task, only with `assign_public_ip = true` | $0.005 per IP-hour | $3.65 |
| DynamoDB on-demand | $0.625 per million writes, $0.125 per million reads, $0.25 per GB-month after the first 25 GB | under $0.10 at 100,000 messages a month |
| CloudWatch Logs | $0.50 per GB ingested, $0.03 per GB-month stored; 5 GB a month free | about $0 (the router logs little) |
| **Total** | | **about $33–43 a month** |

Not included: a NAT gateway if your task subnets need one (not verified for
us-east-1: the VPC pricing page's example rate is for US East (Ohio)), data
transfer out, Route 53 hosted zone and query charges (not verified), and
DynamoDB TTL deletes (the on-demand pricing page doesn't mention them; not
verified).

Sources:
[Fargate](https://aws.amazon.com/fargate/pricing/),
[Elastic Load Balancing](https://aws.amazon.com/elasticloadbalancing/pricing/),
[VPC (public IPv4)](https://aws.amazon.com/vpc/pricing/),
[DynamoDB on-demand](https://aws.amazon.com/dynamodb/pricing/on-demand/),
[CloudWatch](https://aws.amazon.com/cloudwatch/pricing/).
The ALB, DynamoDB and CloudWatch Logs rates are the us-east-1 figures used in
those pages' worked examples.
