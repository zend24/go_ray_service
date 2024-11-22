package main

import (
	"experiment/ray-manager/common"
	"experiment/ray-manager/ray"
	"fmt"
)

func main() {
	taskID := common.TaskID("task-123")
	jobID := common.JobID("job-123")
	clusterID := common.ClusterID("cluster-123")
	jobConfig := common.JobConfig{
		RayTaskConfig: common.RayTaskConfig{
			RayClusterImage: "rayproject/ray:2.9.0",
		},
		MaxReplicas: 1,
		MinReplicas: 0,
	}

	namespace, err := ray.StartRayCluster(taskID, jobID, clusterID, &jobConfig)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
	fmt.Println("Namespace: \n", namespace)
}
