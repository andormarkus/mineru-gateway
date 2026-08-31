# Onboarding a New AWS Account

From an empty AWS account to a running mineru-gateway stack. Every step is
CLI-driven and idempotent where it matters. The end state is the closed
topology from the README (private workers, SSM-only host, zero inbound ports).

```
empty account ──▶ network ──▶ bucket ──▶ quota ──▶ host stack ──▶ worker stack
                                                                   │
                            validate ◀── compose deploy (SSM) ◀────┘
```

Everything below assumes the CLI is authenticated for the target account:

```bash
export AWS_PROFILE=<new-account-profile>
export AWS_REGION=eu-central-1          # pick one and stay in it
```

## 0. Prerequisites checklist

| Requirement | Why |
|---|---|
| ≥1 private subnet (NAT route) | gateway host AND workers: SSM agent dials out, docker pulls, first boot pulls vLLM base image + models |
| ≥1 public subnet | hosts the NAT gateway only — no instance lives there |
| S3 bucket | payloads, results, dedup cache |
| `L-DB27BBAB` quota ≥ 8 vCPUs (g5/g6) | **new accounts default to 0 GPU vCPUs — request early, approval can take hours–days** |
| SSM agent connectivity | AL2023 AMIs ship it; it only needs outbound 443 via the NAT (or SSM VPC endpoints) |

No resource in this topology gets a public IP — the host and the workers all
run in private subnets, and operators reach the host through SSM Session
Manager (the agent's outbound connection), never inbound.

## 1. Network (skip if your VPC already has private subnets with a NAT route)

Both the gateway host and the workers run in private subnets with no public
IPs; their egress (SSM, docker, git, model downloads) flows through one NAT
gateway, which is the only thing living in the public subnet. If the account
has only the default VPC, the minimal layout is:

```bash
VPC_ID=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true \
  --query 'Vpcs[0].VpcId' --output text)

# Elastic IP + NAT gateway in the first public subnet (the ONLY thing there)
PUB_SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values=$VPC_ID \
  Name=map-public-on-launch,Values=true --query 'Subnets[0].SubnetId' --output text)
EIP=$(aws ec2 allocate-address --query AllocationId --output text)
NAT=$(aws ec2 create-nat-gateway --subnet-id $PUB_SUBNET \
  --allocation-id $EIP --query 'NatGateway.NatGatewayId' --output text)

# A private subnet + route via NAT — gateway host and workers both live here
PRIV_SUBNET=$(aws ec2 create-subnet --vpc-id $VPC_ID \
  --cidr-block <a free /24> --query 'Subnet.SubnetId' --output text)
RTB=$(aws ec2 create-route-table --vpc-id $VPC_ID --query 'RouteTable.RouteTableId' --output text)
aws ec2 associate-route-table --route-table-id $RTB --subnet-id $PRIV_SUBNET
aws ec2 create-route --route-table-id $RTB --destination-cidr-block 0.0.0.0/0 \
  --nat-gateway-id $NAT
```

Cost note: the NAT gateway is the main fixed network cost (~$0.045/hr +
data). Keep it if workers boot often; delete it and keep only warm golden AMIs
when the environment goes quiet.

If you use your own VPC, verify: `aws ec2 describe-route-tables` shows the
private subnet routed via the NAT. (Alternative to a NAT: SSM interface
endpoints `ssm`, `ssmmessages`, `ec2messages` + the free S3 gateway endpoint —
more setup, no per-GB NAT charges.)

## 2. S3 bucket + lifecycle safety net

```bash
BUCKET=<account>-mineru-results
aws s3api create-bucket --bucket $BUCKET --create-bucket-configuration \
  LocationConstraint=$AWS_REGION

# Orphan sweep: the scheduler deletes what it tracks; lifecycle catches crash leaks.
aws s3api put-bucket-lifecycle-configuration --bucket $BUCKET --lifecycle-configuration '{
  "Rules": [
    {"ID": "expire-payloads", "Filter": {"Prefix": "payloads/"}, "Status": "Enabled",
     "Expiration": {"Days": 7}},
    {"ID": "expire-results",  "Filter": {"Prefix": "results/"},  "Status": "Enabled",
     "Expiration": {"Days": 90}},
    {"ID": "expire-cache",    "Filter": {"Prefix": "cache/"},    "Status": "Enabled",
     "Expiration": {"Days": 90}}
  ]
}'
```

The gateway never creates or deletes buckets; it only reads/writes objects.

## 3. GPU quota (do this first in parallel — it gates everything)

Workers default to `g6.2xlarge` (8 vCPUs). New accounts have **zero**
On-Demand G/VT vCPU quota:

```bash
aws service-quotas get-service-quota --service-code ec2 \
  --quota-code L-DB27BBAB --query 'Quota.Value' --output text
```

If it prints `0`, request an increase (console: Service Quotas → EC2 →
"Running On-Demand G and VT instances", or `aws service-quotas
request-service-quota-increase`). Until approved, the scheduler's launches
fail with `InsufficientInstanceCapacity`/quota errors — visible in
`provisioning_detail` via `GET /admin/workers`.

## 4. Host stack

The host (t4g.medium, ~$0.04/hr) is created first — the worker stack grants
its SG ingress and `iam:PassRole` to the host role. It launches into the
private subnet with **no public IP**; SSM reaches it through the agent's
outbound connection:

```bash
aws cloudformation create-stack --stack-name mineru-gateway-host \
  --template-body file://deploy/cloudformation/gateway-host.yaml \
  --parameters \
    ParameterKey=VpcId,ParameterValue=$VPC_ID \
    ParameterKey=PrivateSubnetId,ParameterValue=$PRIV_SUBNET \
    ParameterKey=ResultsBucket,ParameterValue=$BUCKET \
    ParameterKey=EnvironmentName,ParameterValue=sandbox \
  --capabilities CAPABILITY_NAMED_IAM
aws cloudformation wait stack-create-complete --stack-name mineru-gateway-host

HOST_SG=$(aws cloudformation describe-stacks --stack-name mineru-gateway-host \
  --query 'Stacks[0].Outputs[?OutputKey==`SecurityGroupId`].OutputValue' --output text)
HOST_ROLE=$(aws cloudformation describe-stacks --stack-name mineru-gateway-host \
  --query 'Stacks[0].Outputs[?OutputKey==`RoleName`].OutputValue' --output text)
```

The host has **no key pair and no inbound rules** — access is
`aws ssm start-session` only.

## 5. Worker stack (private subnets)

```bash
aws cloudformation create-stack --stack-name mineru-worker-sandbox \
  --template-body file://deploy/cloudformation/mineru-worker.yaml \
  --parameters \
    ParameterKey=VpcId,ParameterValue=$VPC_ID \
    ParameterKey=PrivateSubnetId,ParameterValue=$PRIV_SUBNET \
    ParameterKey=GatewaySecurityGroupId,ParameterValue=$HOST_SG \
    ParameterKey=HostRoleName,ParameterValue=$HOST_ROLE \
    ParameterKey=ResultsBucket,ParameterValue=$BUCKET \
    ParameterKey=EnvironmentName,ParameterValue=sandbox \
  --capabilities CAPABILITY_NAMED_IAM
aws cloudformation wait stack-create-complete --stack-name mineru-worker-sandbox

LAUNCH_TEMPLATE=$(aws cloudformation describe-stacks --stack-name mineru-worker-sandbox \
  --query 'Stacks[0].Outputs[?OutputKey==`LaunchTemplateId`].OutputValue' --output text)
```

Workers: `:8000` (mineru-api) and `:8001` (bootstrap status) reachable only
from the host SG. Workers also carry `AmazonSSMManagedInstanceCore`, so you
can `aws ssm start-session` straight into a misbehaving GPU box — handy when
`provisioning_detail` shows a stuck bootstrap stage.

## 6. Deploy the gateway (over SSM) and validate

Follow the README "Sandbox deployment" steps 3–4 from here: clone the repo on
the host, fill `deploy/compose/.env` + `config.yaml` (api_key, `$BUCKET`,
`$LAUNCH_TEMPLATE`), `docker compose -f docker-compose.sandbox.yml up -d --build`.

Validate:

1. `curl http://127.0.0.1:8000/health` through the SSM port-forward.
2. Submit a real PDF via `/tasks`; the scheduler launches the **first worker
   from zero** — first boot builds the MinerU image (tens of minutes; watch
   `GET /admin/workers` `provisioning_detail` progress through stages).
3. Task completes → result ZIP in `$BUCKET/results/`.
4. Optional full simulation: run the e2e tier **from the host** (it must run
   in-VPC since workers are private-IP only) — see `tests/e2e/__init__.py`.

## 7. Cost map (hourly, eu-central-1, on-demand)

| Item | When it costs |
|---|---|
| gateway host t4g.medium | always while the stack exists (~$1/day) |
| NAT gateway + EIP | always (~$1.1/day + data) |
| GPU worker g6.2xlarge | only while queued/processing + idle cooldown (~$1.06/hr) |
| S3 / EBS | cents |

Scale-from-zero (`min_workers: 0`) keeps the GPU line at $0 when idle.
To stop everything: delete the host stack, let the fleet drain (or terminate
discovered workers), and optionally drop the NAT gateway.

## Teardown (full)

```bash
aws cloudformation delete-stack --stack-name mineru-gateway-host
aws cloudformation delete-stack --stack-name mineru-worker-sandbox
aws ec2 delete-nat-gateway --nat-gateway-id $NAT && aws ec2 release-address --allocation-id $EIP
# S3: empty then delete, or keep results and let lifecycle expire them
```
