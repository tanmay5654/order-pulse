# EC2 demo deployment

This creates a paid AWS EC2 instance unless your account credits cover it. Confirm your account's plan, credit eligibility and regional EC2/EBS/public IPv4 pricing before applying. The default is a t3.large (2 vCPU, 8 GiB) with 30 GiB gp3 storage; smaller machines can struggle to build and run all six containers. No deployment has been performed by these files alone.

Prerequisites: Terraform >=1.5, authenticated AWS CLI/profile with permission to create EC2, security groups and IAM roles; a default VPC with a public subnet and internet route in the selected region; this repository pushed to a public GitHub repository. Set a commit hash so deployments are reproducible.

```sh
cd infra
terraform init
terraform plan -out=demo.plan \
  -var='repository_url=https://github.com/YOUR_USER/event-driven-order-platform.git' \
  -var='revision=YOUR_COMMIT_HASH' \
  -var='viewer_cidr=YOUR_PUBLIC_IP/32'
# Review planned resources and cost before applying.
terraform apply demo.plan
terraform output -raw demo_url
```

Use `viewer_cidr=0.0.0.0/0` only when you want the dashboard and order creation API publicly accessible. This is a portfolio demo without authentication or TLS; keep private for development. Kafka, PostgreSQL and service ports stay inside the Docker network. Notifications are durable SQL notification records; no email provider or external delivery is required.

Startup downloads dependencies and builds images; allow several minutes. Use AWS Systems Manager Session Manager (no SSH port is opened) to inspect `/var/log/order-pulse-bootstrap.log`, then `cd /opt/order-pulse && docker compose logs`. If no default VPC exists, supply your own VPC/subnet resources before planning.

Add the actual output URL to the README only after `BASE_URL=http://HOST:8090 python3 scripts/smoke.py` passes. The instance public IP can change after a stop/start. This demo is a single host, not a highly available deployment.

To remove resources and stop future compute/storage charges:

```sh
cd infra
terraform destroy
```

Destroying the instance also deletes its data volume. Stopping EC2 alone leaves storage charges. Keep Terraform state secure: it contains resource identifiers and bootstrap configuration. Never commit credentials or state.
