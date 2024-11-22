package common

type TaskID string
type JobID string
type ClusterID string

type JobConfig struct {
	RayTaskConfig RayTaskConfig `json:"ray_task_config"`
	MinReplicas   int           `json:"min_replicas"`
	MaxReplicas   int           `json:"max_replicas"`
}

type RayTaskConfig struct {
	RayVersion      string `json:"ray_version"`
	RayClusterImage string `json:"ray_cluster_image"`
}
