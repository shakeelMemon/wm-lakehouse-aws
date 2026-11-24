terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
  # State per environment: terraform init -backend-config=envs/<env>.backend.hcl
  backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags {
    tags = local.tags
  }
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  name       = "wm-${var.env}"
  account_id = data.aws_caller_identity.current.account_id
  tags = {
    project     = "wm-performance-lakehouse"
    environment = var.env
    domain      = "wealth-management"
    owner       = var.owner
    managed_by  = "terraform"
  }
  repo_root = "${path.module}/../.."
}
