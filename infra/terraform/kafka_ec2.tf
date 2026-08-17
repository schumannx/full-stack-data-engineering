# --- Ship the kafka/ layer to the ops bucket for the instance to pull ----------
# Terraform's fileset glob has no brace alternation, so match everything and
# filter to the file types the instance needs (skips README.md, __pycache__).
resource "aws_s3_object" "kafka_files" {
  for_each = var.enable_kafka ? toset([
    for f in fileset(local.kafka_dir, "**") : f
    if can(regex("\\.(py|sh|service|txt)$", f))
  ]) : toset([])
  bucket = aws_s3_bucket.ops.id
  key    = "kafka/${each.value}"
  source = "${local.kafka_dir}/${each.value}"
  etag   = filemd5("${local.kafka_dir}/${each.value}")
}

# --- Latest Amazon Linux 2023 AMI ----------------------------------------------
data "aws_ssm_parameter" "al2023" {
  count = var.enable_kafka ? 1 : 0
  name  = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

# --- Single-node Kafka broker + co-located consumer ----------------------------
resource "aws_instance" "kafka" {
  count                  = var.enable_kafka ? 1 : 0
  ami                    = data.aws_ssm_parameter.al2023[0].value
  instance_type          = var.kafka_instance_type
  subnet_id              = tolist(local.subnet_ids)[0]
  vpc_security_group_ids = [aws_security_group.kafka[0].id]
  iam_instance_profile   = aws_iam_instance_profile.kafka[0].name
  key_name               = var.key_pair_name != "" ? var.key_pair_name : null

  user_data = templatefile("${path.module}/templates/kafka_user_data.sh.tftpl", {
    region      = var.aws_region
    ops_bucket  = aws_s3_bucket.ops.id
    data_bucket = var.data_bucket
  })
  # New user_data => replace the instance so it re-bootstraps.
  user_data_replace_on_change = true

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  tags = { Name = "${local.name}-kafka" }

  depends_on = [aws_s3_object.kafka_files]
}
