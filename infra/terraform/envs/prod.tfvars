env         = "prod"
region      = "ca-central-1"
data_scale  = "medium"
alert_email = "wm-data-support@example.com"

lf_admin_arns             = ["arn:aws:iam::111122223333:role/DataPlatformAdmin", "arn:aws:iam::111122223333:role/GitHubDeploy"]
analyst_principal_arns    = ["arn:aws:iam::111122223333:role/datazone_usr_role_wm_analytics_prod"]
bi_gateway_principal_arn  = "arn:aws:iam::111122223333:role/powerbi-gateway-host"
enable_table_level_grants = true
advisor_row_filters       = {}

create_mwaa             = true
mwaa_subnet_ids         = ["subnet-aaaa1111", "subnet-bbbb2222"]
mwaa_security_group_ids = ["sg-cccc3333"]
