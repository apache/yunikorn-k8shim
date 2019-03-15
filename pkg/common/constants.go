package common

// Scheduler
const SchedulerName = "yunikorn"
const DefaultPolicyGroup = "queues"

// Cluster
const ClusterId = "my-kube-cluster"
const ClusterVersion = "0.1"
const DefaultNodeAttributeHostNameKey = "si.io/hostname"
const DefaultNodeAttributeRackNameKey = "si.io/rackname"
const DefaultRackName = "/rack-default"

// Job
const LabelJobId = "jobId"
const LabelQueueName = "queue"
const JobDefaultQueue = "root"
const DefaultPartition = "default"

// Resource
const Memory = "memory"
const CPU = "vcore"

// Spark
const SparkLabelAppId = "spark-app-id"
const SparkLabelRole = "spark-role"
const SparkLabelRoleDriver = "driver"