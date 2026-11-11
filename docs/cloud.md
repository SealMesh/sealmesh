# Cloud mode

Cloud mode is the SealMesh router you already know — GitHub identity, repo
permissions, seven days of room chat and nothing else — running in **your own
AWS account** instead of on a box you manage by hand. Nobody else hosts it, so
nobody else sees your org's connections or room messages.

It's the same `sealmesh-server` binary, in the `-dynamodb` image variant, so room
history lives in a DynamoDB table with a TTL instead of a SQLite file. Nothing
about authorization changes: the router still checks every caller with the
caller's own GitHub token, against the org and repos you configure.

## What you get

- One Fargate task behind an Application Load Balancer that terminates HTTPS.
- A DynamoDB table (on-demand) whose items expire after seven days.
- A task role that can do exactly two things: `PutItem` and `Query` on that table.
- CloudWatch logs, kept 14 days by default.

Roughly $33–43 a month at the default size in us-east-1; the itemized estimate
and its sources are in the recipe's README.

## Set it up

The Terraform recipe and its instructions are in
[`deploy/aws/README.md`](../deploy/aws/README.md). You run `terraform apply`
yourself, in your account; the SealMesh project never deploys anything for you.

When it's up, point every daemon at it:

```toml
[server]
url = "https://router.example.com"
```

## When to use something else

If you'd rather not run anything on AWS, the router runs anywhere a container or
a binary does, with SQLite for history. See "Running your own router" in
[install.md](install.md).
