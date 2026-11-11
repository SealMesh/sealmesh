provider "aws" {
  region = var.region

  default_tags {
    tags = merge({ app = "sealmesh-router" }, var.tags)
  }
}

data "aws_caller_identity" "current" {}

locals {
  router_port = 8787
  image       = "${var.image_repository}:${var.router_version}-dynamodb"
}
