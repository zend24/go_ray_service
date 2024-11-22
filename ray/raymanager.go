package ray

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/gin-gonic/gin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

type StartRayClusterRequest struct {
	UserID            string `json:"user_id"`
	ClusterID         string `json:"cluster_id"`
	InstanceName      string `json:"instance_name"`
	RayClusterImage   string `json:"ray_cluster_image"`
	WorkerMaxReplicas int    `json:"worker_max_replicas"`
	WorkerMinReplicas int    `json:"worker_min_replicas"`
}

type StartRayServeAppRequest struct {
	AppName     string `json:"app_name"`
	Host        string `json:"host"`
	Port        int    `json:"port"`
	Image       string `json:"image"`
	NumReplicas int    `json:"num_replicas"`
}

func main() {
	r := gin.Default()
	r.POST("/start-ray-cluster", startRayCluster)
	r.POST("/start-ray-serve-app", startRayServeApp)
	r.Run(":8080")
}

func startRayCluster(c *gin.Context) {
	var req StartRayClusterRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, "Invalid request")
		return
	}

	// Create a dynamic Kubernetes client
	// config, err := clientcmd.BuildConfigFromFlags("", filepath.Join(homedir.HomeDir(), ".kube", "config"))
	config, err := clientcmd.BuildConfigFromFlags("", "/Users/zhenduan/Projects/learn-go/config")
	if err != nil {
		c.JSON(http.StatusInternalServerError, "Failed to create Kubernetes config")
		return
	}

	// Test the connection by listing namespaces
	fmt.Println("Getting namespaces...")
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, "Failed to create dynamic client")
		return
	}

	namespaces, err := dynamicClient.Resource(schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "namespaces",
	}).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, fmt.Sprintf("Failed to list namespaces: %v", err))
		return
	}

	// Print the namespaces to verify connection
	for _, ns := range namespaces.Items {
		fmt.Println(ns.GetName())
	}

	dynamicClient, err = dynamic.NewForConfig(config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, fmt.Sprintf("Failed to create dynamic client: %v", err))
		return
	}

	// list all pods in the cluster with namespace kuberay
	pods, err := dynamicClient.Resource(schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "pods",
	}).Namespace("kuberay").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, fmt.Sprintf("Failed to list pods: %v", err))
		return
	}
	for _, pod := range pods.Items {
		fmt.Println(pod.GetName())
	}

	// Define the Group-Version-Resource for the RayCluster custom resource
	rayClusterGVR := schema.GroupVersionResource{
		Group:    "ray.io",
		Version:  "v1",
		Resource: "rayclusters",
	}

	// Define the RayCluster object
	rayCluster := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "ray.io/v1",
			"kind":       "RayCluster",
			"metadata": map[string]interface{}{
				"name":      fmt.Sprintf("raycluster-%s", req.InstanceName),
				"namespace": "kuberay",
			},
			"spec": map[string]interface{}{
				"headGroupSpec": map[string]interface{}{
					"rayStartParams": map[string]interface{}{
						"dashboard-host": "0.0.0.0",
					},
					"template": map[string]interface{}{
						"spec": map[string]interface{}{
							"containers": []interface{}{
								map[string]interface{}{
									"name":  "ray-head",
									"image": req.RayClusterImage,
									"ports": []interface{}{
										map[string]interface{}{
											"containerPort": 8265,
											"protocol":      "TCP",
										},
									},
									"resources": map[string]interface{}{
										"limits": map[string]interface{}{
											"cpu":    "1",
											"memory": "10Gi",
										},
										"requests": map[string]interface{}{
											"cpu":    "1",
											"memory": "10Gi",
										},
									},
								},
							},
						},
					},
				},
				"workerGroupSpecs": []interface{}{
					map[string]interface{}{
						"groupName":      "ray-worker-group",
						"maxReplicas":    req.WorkerMaxReplicas,
						"minReplicas":    req.WorkerMinReplicas,
						"replicas":       req.WorkerMinReplicas,
						"rayStartParams": map[string]interface{}{},
						"template": map[string]interface{}{
							"spec": map[string]interface{}{
								"containers": []interface{}{
									map[string]interface{}{
										"name":  "ray-worker",
										"image": req.RayClusterImage,
										"resources": map[string]interface{}{
											"limits": map[string]interface{}{
												"cpu":            "16",
												"memory":         "64Gi",
												"nvidia.com/gpu": 1,
											},
											"requests": map[string]interface{}{
												"cpu":            "16",
												"memory":         "64Gi",
												"nvidia.com/gpu": 1,
											},
										},
										"lifecycle": map[string]interface{}{
											"preStop": map[string]interface{}{
												"exec": map[string]interface{}{
													"command": []string{"/bin/sh", "-c", "ray stop"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Create the RayCluster custom resource
	_, err = dynamicClient.Resource(rayClusterGVR).Namespace("kuberay").Create(context.Background(), rayCluster, metav1.CreateOptions{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, fmt.Sprintf("Failed to create RayCluster: %v", err))
		return
	}

	c.JSON(http.StatusOK, "RayCluster started successfully")
}

func startRayServeApp(c *gin.Context) {
	var req StartRayServeAppRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, "Invalid request")
		return
	}

	// Generate the serve config file
	serveConfig := fmt.Sprintf(`
proxy_location: EveryNode

http_options:
  host: %s
  port: %d

applications:
  - name: %s
    route_prefix: /
    import_path: text_ml:app
    runtime_env:
      pip:
        - torch
        - transformers
    deployments:
    - name: Translator
      num_replicas: %d
`, req.Host, req.Port, req.AppName, req.NumReplicas)

	// Write the serve config to a file
	tempDir, err := os.MkdirTemp("", "serve-config")
	if err != nil {
		c.JSON(http.StatusInternalServerError, "Failed to create temp directory")
		return
	}
	defer os.RemoveAll(tempDir)

	configFilePath := filepath.Join(tempDir, "serve_config.yaml")
	if err := os.WriteFile(configFilePath, []byte(serveConfig), 0644); err != nil {
		c.JSON(http.StatusInternalServerError, "Failed to write serve config file")
		return
	}

	// Execute the serve deploy command on the remote cluster
	cmd := exec.Command("kubectl", "exec", "-it", "ray-head-pod", "--", "serve", "deploy", configFilePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		c.JSON(http.StatusInternalServerError, fmt.Sprintf("Failed to deploy Ray Serve app: %v", err))
		return
	}

	c.JSON(http.StatusOK, fmt.Sprintf("Ray Serve app deployed successfully: %s", string(output)))
}
