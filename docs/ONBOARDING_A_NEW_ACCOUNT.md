# Onboarding a New AWS Account

From an empty AWS account to a running mineru-gateway stack — **two
`create-stack` calls and one compose-up**. The bucket, IAM, S3 lifecycle
rules, and the launch-template wiring are all owned by the stacks; nothing is
copy-pasted between them except stack outputs.

```
empty account ──▶ quota request (async) ──▶ network ──▶ controller stack ──▶ worker stack
      │                                              (instance + bucket)      (LT + IAM + SSM param)
      └────────────────────────────────▶ compose deploy over SSM ──▶ validate
```

Everything below assumes the CLI is authenticated for the target account:

```bash
export AWS_PROFILE=<new-account-profile>
export AWS_REGION=eu-central-1          # pick one and stay in it
```

## 0. Prerequisites

| Requirement | Why |
|---|---|
| ≥1 private subnet (NAT route) | controller AND workers: SSM agent dials out, docker pulls, first boot pulls vLLM base image + models |
| ≥1 public subnet | hosts the NAT gateway only — no instance lives there |
| `L-DB2E81BA` quota ≥ 8 vCPUs (g5/g6) | the one thing no CloudFormation stack can do (see below) |

No resource in this topology gets a public IP — the controller and the
workers all run in private subnets, and operators reach the controller
through SSM Session Manager (the agent's outbound connection), never inbound.

## 1. GPU quota — file this FIRST (it is not automatable)

Workers default to `g6.2xlarge` (8 vCPUs). New accounts have **zero**
On-Demand G/VT vCPU quota, and raising it requires AWS-side approval —
CloudFormation cannot request it, which is why it lives here instead of in a
stack. Approval commonly takes hours, sometimes days:

```bash
aws service-quotas get-service-quota --service-code ec2 \
  --quota-code L-DB2E81BA --query 'Quota.Value' --output text
# if 0 → request an increase (console: Service Quotas → EC2 →
# "Running On-Demand G and VT instances", or request-service-quota-increase)
```

Until approved, the scheduler's launches fail with quota errors — visible in
`provisioning_detail` via `GET /admin/workers`.

## 2. Network (skip if your VPC already has a private subnet with a NAT route)

Both the controller and the workers run in private subnets with no public
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

# A private subnet + route via NAT — controller and workers both live here
PRIV_SUBNET=$(aws ec2 create-subnet --vpc-id $VPC_ID \
  --cidr-block <a free /24> --query 'Subnet.SubnetId' --output text)
RTB=$(aws ec2 create-route-table --vpc-id $VPC_ID --query 'RouteTable.RouteTableId' --output text)
aws ec2 associate-route-table --route-table-id $RTB --subnet-id $PRIV_SUBNET
aws ec2 create-route --route-table-id $RTB --destination-cidr-block 0.0.0.0/0 \
  --nat-gateway-id $NAT
```

If you use your own VPC, verify: `aws ec2 describe-route-tables` shows the
private subnet routed via the NAT. (Alternative to a NAT: SSM interface
endpoints `ssm`, `ssmmessages`, `ec2messages` + the free S3 gateway endpoint —
more setup, no per-GB NAT charges.)

## 3. Controller stack

Creates the control-plane instance (t4g.medium, ~$0.04/hr: gateway +
scheduler + Postgres via compose) **and the results bucket** with public
access blocked and lifecycle rules (payloads 7d / results 90d / cache 90d) —
unless you pass an existing bucket name. Create it first; the worker stack
grants its SG ingress and `iam:PassRole` to the controller role.

```bash
aws cloudformation create-stack --stack-name mineru-controller \
  --template-body file://deploy/cloudformation/controller.yaml \
  --parameters \
    ParameterKey=VpcId,ParameterValue=$VPC_ID \
    ParameterKey=PrivateSubnetId,ParameterValue=$PRIV_SUBNET \
    ParameterKey=EnvironmentName,ParameterValue=sandbox \
  --capabilities CAPABILITY_NAMED_IAM
aws cloudformation wait stack-create-complete --stack-name mineru-controller

CTRL_OUT() { aws cloudformation describe-stacks --stack-name mineru-controller \
  --query "Stacks[0].Outputs[?OutputKey==\`$1\`].OutputValue" --output text; }
CTRL_ID=$(CTRL_OUT InstanceId); CTRL_SG=$(CTRL_OUT SecurityGroupId)
CTRL_ROLE=$(CTRL_OUT RoleName);  BUCKET=$(CTRL_OUT ResultsBucketName)
```

The controller has **no key pair, no public IP, and no inbound rules** —
access is `aws ssm start-session --target $CTRL_ID` only.

## 4. Worker stack (private subnet, publishes its launch template to SSM)

```bash
aws cloudformation create-stack --stack-name mineru-worker-sandbox \
  --template-body file://deploy/cloudformation/mineru-worker.yaml \
  --parameters \
    ParameterKey=VpcId,ParameterValue=$VPC_ID \
    ParameterKey=PrivateSubnetId,ParameterValue=$PRIV_SUBNET \
    ParameterKey=GatewaySecurityGroupId,ParameterValue=$CTRL_SG \
    ParameterKey=HostRoleName,ParameterValue=$CTRL_ROLE \
    ParameterKey=ResultsBucket,ParameterValue=$BUCKET \
    ParameterKey=EnvironmentName,ParameterValue=sandbox \
  --capabilities CAPABILITY_NAMED_IAM
```

Workers: `:8000` (mineru-api) and `:8001` (bootstrap status) reachable only
from the controller SG. The stack publishes its `LaunchTemplateId` to SSM
Parameter Store at `/mineru-gateway/sandbox/launch-template-id` — the gateway
config references that path (`ssm:/...`), so **no `lt-` id is ever
transcribed**. Workers also carry `AmazonSSMManagedInstanceCore`, so you can
`aws ssm start-session` straight into a misbehaving GPU box.

## 5. Deploy the gateway (over SSM) and validate

```bash
aws ssm send-command --instance-ids "$CTRL_ID" \
  --document-name AWS-RunShellScript --comment "deploy mineru-gateway" \
  --commands 'git clone https://github.com/andormarkus/mineru-gateway.git ~/mineru-gateway &&
              cd ~/mineru-gateway && git checkout v0.1.0 && cd deploy/compose &&
              cp .env.example .env && sed -i "s/change-me/$(openssl rand -hex 24)/" .env &&
              cp config.sandbox.yaml.example config.yaml &&
              docker compose -f docker-compose.sandbox.yml up -d --build'
```

Then set three values in `~/mineru-gateway/deploy/compose/config.yaml` on the
controller: `api_key`, `bucket: $BUCKET`, and the postgres password (same as
`.env`) — `launch_template_id` already points at the SSM parameter.

Validate:

1. `curl http://127.0.0.1:8000/health` through the SSM port-forward.
2. Submit a real PDF via `/tasks`; the scheduler launches the **first worker
   from zero** — first boot builds the MinerU image (tens of minutes; watch
   `GET /admin/workers` `provisioning_detail` progress through stages).
3. Task completes → result ZIP in `$BUCKET/results/`.
4. Optional full simulation: run the e2e tier **from the controller** (it
   must run in-VPC since workers are private-IP only) — see
   `tests/e2e/__init__.py`.

## 6. Cost map (hourly, eu-central-1, on-demand)

| Item | When it costs |
|---|---|
| controller t4g.medium | always while the stack exists (~$1/day) |
| NAT gateway + EIP | always (~$1.1/day + data) |
| GPU worker g6.2xlarge | only while queued/processing + idle cooldown (~$1.06/hr) |
| S3 / EBS | cents |

Scale-from-zero (`min_workers: 0`) keeps the GPU line at $0 when idle.

## Teardown (full)

```bash
aws cloudformation delete-stack --stack-name mineru-controller
aws cloudformation delete-stack --stack-name mineru-worker-sandbox
aws ec2 delete-nat-gateway --nat-gateway-id $NAT && aws ec2 release-address --allocation-id $EIP
# The results bucket is retained on controller-stack deletion (DeletionPolicy:
# Retain) — empty and delete it manually if you truly want everything gone:
#   aws s3 rm s3://$BUCKET --recursive && aws s3api delete-bucket --bucket $BUCKET
```
