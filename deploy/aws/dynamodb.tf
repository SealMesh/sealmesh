# Room history: one item per delivered message, keyed by room and the
# router's sequence number. The router writes `expires_at` (epoch seconds,
# seven days after posting) on every item and DynamoDB's TTL deletes it after
# that. The read path also refuses expired items, since TTL can lag.
# See crates/sealmesh-server/src/history.rs.

resource "aws_dynamodb_table" "room_history" {
  #checkov:skip=CKV_AWS_28:Point-in-time recovery keeps 35 days of backups, which would hold messages past the router's seven-day retention promise.
  #checkov:skip=CKV_AWS_119:Encrypted at rest with the AWS-owned key; a customer-managed key adds a monthly cost and nothing this recipe needs.
  name         = "${var.name}-room-history"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "room"
  range_key    = "seq"

  attribute {
    name = "room"
    type = "S"
  }

  attribute {
    name = "seq"
    type = "N"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = false
  }
}
