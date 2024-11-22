package ray

import (
	"context"
	"encoding/json"
	"errors"
	"experiment/ray-manager/common"
	"fmt"
	"os"
	"path/filepath"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/yaml"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func CreateNamespace(ctx context.Context, namespace string, config *rest.Config) error {
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return errors.New("failed to create client set")
	}

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}

	_, err = clientSet.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		fmt.Println("Namespace already exists")
		return nil
	}
	return err
}

func StartRayCluster(taskID common.TaskID, jobID common.JobID, clusterID common.ClusterID, jobConfig *common.JobConfig) (namespace string, err error) {
	// hardcode the config file for k8s cluster for testing purposes
	config, err := clientcmd.BuildConfigFromFlags("", filepath.Join(homedir.HomeDir(), ".kube", "config"))
	if err != nil {
		return "", fmt.Errorf("failed to build config: %v", err)
	}

	// Create a dynamic Kubernetes client for
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return "", fmt.Errorf("failed to create dynamic client: %v", err)
	}

	ns := "kuberay"

	// Deploy the RayService on K8s
	if err := deployRayService(dynamicClient); err != nil {
		return "", fmt.Errorf("error deploying ray service: %v", err)
	}
	fmt.Println("RayService deployed successfully")

	// Get the name of the RayCluster on K8s
	rayClusterGVR := schema.GroupVersionResource{
		Group:    "ray.io",
		Version:  "v1",
		Resource: "rayclusters",
	}
	var rayClusters *unstructured.UnstructuredList
	// set a retry limit and a timeout
	retryLimit := 5
	timeout := 10 * time.Second
	startTime := time.Now()
	for retryCount := 0; retryCount < retryLimit || time.Since(startTime) < timeout; retryCount++ {
		rayClusters, err = dynamicClient.Resource(rayClusterGVR).Namespace(ns).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			return "", fmt.Errorf("failed to list ray clusters: %v", err)
		}
		if len(rayClusters.Items) > 0 {
			fmt.Printf("Found RayCluster on %vth attempt\n", retryCount+1)
			break
		}
	}
	if len(rayClusters.Items) == 0 {
		return "", fmt.Errorf("no ray clusters found")
	}
	// save the first ray cluster name
	rayClusterName := rayClusters.Items[0].GetName()

	fmt.Printf("Creating LoadBalancer with RayCluster name: %s...\n", rayClusterName)

	// create a client set for the k8s cluster
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", fmt.Errorf("failed to create client set: %v", err)
	}

	// Tested
	// create a new namespace with unique name
	// uniqueSuffix := uuid.New().String()
	// ns := fmt.Sprintf("ns-task-%s-%s", taskID, uniqueSuffix)
	// if err := CreateNamespace(context.Background(), ns, config); err != nil {
	// 	return "", fmt.Errorf("failed to create namespace: %v", err)
	// }

	// Tested
	// Define the Group-Version-Resource for the RayCluster custom resource
	// rayClusterGVR := schema.GroupVersionResource{
	// 	Group:    "ray.io",
	// 	Version:  "v1",
	// 	Resource: "rayclusters",
	// }

	// Tested
	// Define the RayCluster object
	// rayClusterObject := defineRayClusterObject(ns, taskID, jobID, jobConfig)

	// Tested
	// Create the RayCluster on K8s
	// _, err = dynamicClient.Resource(rayClusterGVR).Namespace(ns).Create(context.Background(), rayClusterObject, metav1.CreateOptions{})
	// if err != nil {
	// 	return ns, fmt.Errorf("failed to create ray cluster: %v", err)
	// }

	// Create a LoadBalancer service for the RayCluster
	rayClusterLBServiceObject := createLBServiceObject(taskID, ns, rayClusterName)

	_, err = clientSet.CoreV1().Services(ns).Create(context.Background(), rayClusterLBServiceObject, metav1.CreateOptions{})
	if err != nil {
		return ns, fmt.Errorf("failed to create ray loadbalancer service: %v", err)
	}
	fmt.Printf("RayCluster loadbalancer service created successfully at port 8265\n")

	// return the namespace
	return "kuberay", nil
}

func ScaleRayCluster(taskID common.TaskID, jobID common.JobID, clusterID common.ClusterID, jobConfig *common.JobConfig) (namespace string, err error) {
	// hardcode the config file for k8s cluster for testing purposes
	config, err := clientcmd.BuildConfigFromFlags("", filepath.Join(homedir.HomeDir(), ".kube", "config"))
	if err != nil {
		return "", fmt.Errorf("failed to build config: %v", err)
	}

	// Create a dynamic Kubernetes client for
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return "", fmt.Errorf("failed to create dynamic client: %v", err)
	}

	ns := "kuberay"

	// Deploy the RayService on K8s
	if err := deployRayService(dynamicClient); err != nil {
		return "", fmt.Errorf("error deploying ray service: %v", err)
	}
	fmt.Println("RayService deployed successfully")

	// Get the name of the RayCluster on K8s
	rayClusterGVR := schema.GroupVersionResource{
		Group:    "ray.io",
		Version:  "v1",
		Resource: "rayclusters",
	}
	var rayClusters *unstructured.UnstructuredList
	// set a retry limit and a timeout
	retryLimit := 5
	timeout := 10 * time.Second
	startTime := time.Now()
	for retryCount := 0; retryCount < retryLimit || time.Since(startTime) < timeout; retryCount++ {
		rayClusters, err = dynamicClient.Resource(rayClusterGVR).Namespace(ns).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			return "", fmt.Errorf("failed to list ray clusters: %v", err)
		}
		if len(rayClusters.Items) > 0 {
			break
		}
	}
	if len(rayClusters.Items) == 0 {
		return "", fmt.Errorf("no ray clusters found")
	}
	// save the first ray cluster name
	rayClusterName := rayClusters.Items[0].GetName()

	fmt.Printf("Creating LoadBalancer with RayCluster name: %s...\n", rayClusterName)

	// create a client set for the k8s cluster
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", fmt.Errorf("failed to create client set: %v", err)
	}

	// Create a LoadBalancer service for the RayCluster
	rayClusterLBServiceObject := createLBServiceObject(taskID, ns, rayClusterName)

	_, err = clientSet.CoreV1().Services(ns).Update(context.Background(), rayClusterLBServiceObject, metav1.UpdateOptions{})
	if err != nil {
		return ns, fmt.Errorf("failed to create ray loadbalancer service: %v", err)
	}
	fmt.Printf("RayCluster loadbalancer service created successfully at port 8265\n")

	// return the namespace
	return "kuberay", nil
}

// deployRayService deploys the RayService on K8s
func deployRayService(client *dynamic.DynamicClient) error {
	// Define the Group-Version-Resource for the RayService custom resource
	rayServiceGVR := schema.GroupVersionResource{
		Group:    "ray.io",
		Version:  "v1",
		Resource: "rayservices",
	}

	// Define the RayService object
	rayServiceObject := createRayServiceObject()

	// print the RayService name
	fmt.Printf("RayService name: %s\n", rayServiceObject.GetName())

	// Retrieve the existing RayService to get the resourceVersion
	existingRayService, err := client.Resource(rayServiceGVR).Namespace("kuberay").Get(context.Background(), "rayservice-abc11", metav1.GetOptions{})
	if err != nil {
		fmt.Printf("No existing RayService found. Creating a new one...\n")

		_, err = client.Resource(rayServiceGVR).Namespace("kuberay").Create(context.Background(), rayServiceObject, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create ray service: %v", err)
		}
		return nil
	}

	// Set the resourceVersion from the existing RayService
	rayServiceObject.SetResourceVersion(existingRayService.GetResourceVersion())

	// Deploy the RayService using the dynamic client
	_, err = client.Resource(rayServiceGVR).Namespace("kuberay").Update(context.Background(), rayServiceObject, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update ray service: %v", err)
	}
	return nil
}

// defineRayClusterObject defines the RayCluster object
func defineRayClusterObject(namespace string, taskID common.TaskID, jobID common.JobID, jobConfig *common.JobConfig) *unstructured.Unstructured {
	rayCluster := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "ray.io/v1",
			"kind":       "RayCluster",
			"metadata": map[string]interface{}{
				"name":      fmt.Sprintf("raycluster-%s", taskID),
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"headGroupSpec": map[string]interface{}{
					"rayStartParams": map[string]interface{}{
						"dashboard-host": "0.0.0.0",
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"task-id":      taskID,
								"job-id":       jobID,
								"ray-pod-type": "head",
							},
						},
						"spec": map[string]interface{}{
							"containers": []interface{}{
								map[string]interface{}{
									"name":  "ray-head",
									"image": jobConfig.RayTaskConfig.RayClusterImage,
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
						"maxReplicas":    jobConfig.MaxReplicas,
						"minReplicas":    jobConfig.MinReplicas,
						"replicas":       jobConfig.MinReplicas,
						"rayStartParams": map[string]interface{}{},
						"template": map[string]interface{}{
							"metadata": map[string]interface{}{
								"labels": map[string]interface{}{
									"task-id":      taskID,
									"job-id":       jobID,
									"ray-pod-type": "worker",
								},
							},
							"spec": map[string]interface{}{
								"containers": []interface{}{
									map[string]interface{}{
										"name":  "ray-worker",
										"image": jobConfig.RayTaskConfig.RayClusterImage,
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
	return rayCluster
}

// createLBServiceObject defines the RayCluster loadbalancer service object
func createLBServiceObject(taskID common.TaskID, ns string, clusterName string) *corev1.Service {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("rayloadbalancer-%s", taskID),
			Namespace: ns,
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"ray.io/cluster":   clusterName,
				"ray.io/node-type": "head",
			},
			Type: corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{
				{
					Protocol:   corev1.ProtocolTCP,
					Port:       8265,
					TargetPort: intstr.FromInt(8265),
					Name:       "dashboard",
				},
				{
					Protocol:   corev1.ProtocolTCP,
					Port:       8000,
					TargetPort: intstr.FromInt(8000),
					Name:       "serve",
				},
				{
					Protocol:   corev1.ProtocolTCP,
					Port:       6379,
					TargetPort: intstr.FromInt(6379),
					Name:       "gcs",
				},
				{
					Protocol:   corev1.ProtocolTCP,
					Port:       10001,
					TargetPort: intstr.FromInt(10001),
					Name:       "client",
				},
			},
		},
	}
	return service
}

// createRayServiceObject creates the RayService object
func createRayServiceObject() *unstructured.Unstructured {

	serveConfig := map[string]interface{}{
		"applications": []interface{}{
			map[string]interface{}{
				"name":         "faker_app",
				"import_path":  "fake.app",
				"route_prefix": "/",
			},
			// map[string]interface{}{
			// 	"name":         "fruit_app",
			// 	"import_path":  "fruit.deployment_graph",
			// 	"route_prefix": "/fruit",
			// 	"runtime_env": map[string]interface{}{
			// 		"working_dir": "https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip",
			// 	},
			// },
			// map[string]interface{}{
			// 	"name":         "math_app",
			// 	"import_path":  "conditional_dag.serve_dag",
			// 	"route_prefix": "/calc",
			// 	"runtime_env": map[string]interface{}{
			// 		"working_dir": "https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip",
			// 	},
			// },
		},
	}

	serveConfigJSON, err := json.Marshal(serveConfig)
	if err != nil {
		fmt.Printf("failed to marshal serve config: %v", err)
		return nil
	}

	rayService := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "ray.io/v1",
			"kind":       "RayService",
			"metadata": map[string]interface{}{
				"name":      "rayservice-abc11",
				"namespace": "kuberay",
			},
			"spec": map[string]interface{}{
				// "serviceUnhealthySecondThreshold":    900,
				// "deploymentUnhealthySecondThreshold": 300,
				// "serveConfigV2": map[string]interface{}{
				// 	"applications": []interface{}{
				// 		map[string]interface{}{
				// 			"name":        "text_summarizer",
				// 			"import_path": "text_summarizer.text_summarizer:deployment",
				// 			"runtime_env": map[string]interface{}{
				// 				"working_dir": "https://github.com/ray-project/serve_config_examples/archive/refs/heads/master.zip",
				// 			},
				// 		},
				// 	},
				// },
				"serveConfigV2": string(serveConfigJSON),
				"rayClusterConfig": map[string]interface{}{
					"rayVersion": "2.9.0",
					"headGroupSpec": map[string]interface{}{
						"rayStartParams": map[string]interface{}{
							"dashboard-host": "0.0.0.0",
						},
						"template": map[string]interface{}{
							"spec": map[string]interface{}{
								"containers": []interface{}{
									map[string]interface{}{
										"name": "ray-head",
										// "image": "rayproject/ray:2.9.0",
										"image": "zendd/faker_app:latest",
										"ports": []interface{}{
											map[string]interface{}{"containerPort": 6379, "name": "gcs"},
											map[string]interface{}{"containerPort": 8265, "name": "dashboard"},
											map[string]interface{}{"containerPort": 10001, "name": "client"},
											map[string]interface{}{"containerPort": 8000, "name": "serve"},
										},
										// "volumeMounts": []interface{}{
										// 	map[string]interface{}{
										// 		"mountPath": "/tmp/ray",
										// 		"name":      "ray-logs",
										// 	},
										// },
										"resources": map[string]interface{}{
											"limits": map[string]interface{}{
												"cpu":    "2",
												"memory": "8G",
											},
											"requests": map[string]interface{}{
												"cpu":    "2",
												"memory": "8G",
											},
										},
									},
								},
								// "volumes": []interface{}{
								// 	map[string]interface{}{
								// 		"name":     "ray-logs",
								// 		"emptyDir": map[string]interface{}{},
								// 	},
								// },
							},
						},
					},
					"workerGroupSpecs": []interface{}{
						map[string]interface{}{
							"replicas":       1,
							"minReplicas":    1,
							"maxReplicas":    1,
							"groupName":      "gpu-group",
							"rayStartParams": map[string]interface{}{},
							"template": map[string]interface{}{
								"spec": map[string]interface{}{
									"containers": []interface{}{
										map[string]interface{}{
											"name": "ray-worker",
											// "image": "rayproject/ray:2.9.0",
											"image": "zendd/faker_app:latest",
											"resources": map[string]interface{}{
												"limits": map[string]interface{}{
													"cpu":    4,
													"memory": "16G",
												},
												"requests": map[string]interface{}{
													"cpu":    3,
													"memory": "12G",
												},
											},
										},
									},
									"tolerations": []interface{}{
										map[string]interface{}{
											"key":      "ray.io/node-type",
											"operator": "Equal",
											"value":    "worker",
											"effect":   "NoSchedule",
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
	return rayService
}

func parseRayServiceYAML() (*unstructured.Unstructured, error) {
	yamlFilePath := "../common/fruit_rayservice.yaml"
	yamlBytes, err := os.ReadFile(yamlFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read yaml file: %v", err)
	}

	var obj unstructured.Unstructured
	if err := yaml.Unmarshal(yamlBytes, &obj); err != nil {
		return nil, fmt.Errorf("failed to unmarshal yaml file: %v", err)
	}
	return &obj, nil
}
