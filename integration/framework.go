// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integration

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang/glog"
	kapi "k8s.io/kubernetes/pkg/api"
	kclient "k8s.io/kubernetes/pkg/client/unversioned"
	kclientcmd "k8s.io/kubernetes/pkg/client/unversioned/clientcmd"
	kclientcmdkapi "k8s.io/kubernetes/pkg/client/unversioned/clientcmd/api"
	"k8s.io/kubernetes/pkg/fields"
	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/runtime"
)

type kubeFramework interface {
	// Kube client
	Client() *kclient.Client

	// Parses and Returns a replication Controller object contained in 'filePath'
	ParseRC(filePath string) (*kapi.ReplicationController, error)

	// Parses and Returns a service object contained in 'filePath'
	ParseService(filePath string) (*kapi.Service, error)

	// Creates a kube service.
	CreateService(ns string, service *kapi.Service) (*kapi.Service, error)

	// Creates a namespace.
	CreateNs(ns *kapi.Namespace) (*kapi.Namespace, error)

	// Creates a kube replication controller.
	CreateRC(ns string, rc *kapi.ReplicationController) (*kapi.ReplicationController, error)

	// Deletes a namespace
	DeleteNs(ns string) error

	// Destroy cluster
	DestroyCluster()

	// Returns a url that provides access to a kubernetes service via the proxy on the apiserver.
	// This url requires master auth.
	GetProxyUrlForService(service *kapi.Service) string

	// Returns the node hostnames.
	GetNodes() ([]string, error)

	// Returns pod names in the cluster.
	// TODO: Remove, or mix with namespace
	GetRunningPodNames() ([]string, error)

	// Returns pods in the cluster.
	GetRunningPods() ([]*kapi.Pod, error)

	WaitUntilPodRunning(ns string, podLabels map[string]string, timeout time.Duration) error
	WaitUntilServiceActive(svc *kapi.Service, timeout time.Duration) error
}

type realKubeFramework struct {
	// Kube client.
	kubeClient *kclient.Client

	// The version of the kube cluster
	version string

	// Master IP for this framework
	masterIP string

	// The base directory of current kubernetes release.
	baseDir string
}

const imageUrlTemplate = "https://github.com/kubernetes/kubernetes/releases/download/v%s/kubernetes.tar.gz"

var (
	kubeConfig = flag.String("kube_config", os.Getenv("HOME")+"/.kube/config", "Path to cluster info file.")
	workDir    = flag.String("work_dir", "/tmp/heapster_test", "Filesystem path where test files will be stored. Files will persist across runs to speed up tests.")
)

func exists(path string) bool {
	if _, err := os.Stat(path); err != nil {
		return false
	}
	return true
}

const pathToGCEConfig = "cluster/gce/config-default.sh"

func disableClusterMonitoring(kubeBaseDir string) error {
	kubeConfigFilePath := filepath.Join(kubeBaseDir, pathToGCEConfig)
	input, err := ioutil.ReadFile(kubeConfigFilePath)
	if err != nil {
		return err
	}

	lines := strings.Split(string(input), "\n")

	for i, line := range lines {
		if strings.Contains(line, "ENABLE_CLUSTER_MONITORING") {
			lines[i] = "ENABLE_CLUSTER_MONITORING=false"
		} else if strings.Contains(line, "NUM_MINIONS=") {
			lines[i] = "NUM_MINIONS=2"
		}
	}
	output := strings.Join(lines, "\n")
	return ioutil.WriteFile(kubeConfigFilePath, []byte(output), 0644)
}

func runKubeClusterCommand(kubeBaseDir, command string) ([]byte, error) {
	cmd := exec.Command(filepath.Join(kubeBaseDir, "cluster", command))
	glog.V(2).Infof("about to run %v", cmd)
	return cmd.CombinedOutput()
}

func setupNewCluster(kubeBaseDir string) error {
	cmd := "kube-up.sh"
	destroyCluster(kubeBaseDir)
	out, err := runKubeClusterCommand(kubeBaseDir, cmd)
	if err != nil {
		glog.Errorf("failed to bring up cluster - %q\n%s", err, out)
		return fmt.Errorf("failed to bring up cluster - %q", err)
	}
	glog.V(2).Info(string(out))
	glog.V(2).Infof("Giving the cluster 30 sec to stabilize")
	time.Sleep(30 * time.Second)
	return nil
}

func destroyCluster(kubeBaseDir string) error {
	if kubeBaseDir == "" {
		glog.Infof("Skipping cluster tear down since kubernetes repo base path is not set.")
		return nil
	}
	glog.V(1).Info("Bringing down any existing kube cluster")
	out, err := runKubeClusterCommand(kubeBaseDir, "kube-down.sh")
	if err != nil {
		glog.Errorf("failed to tear down cluster - %q\n%s", err, out)
		return fmt.Errorf("failed to tear down kube cluster - %q", err)
	}

	return nil
}

func downloadRelease(workDir, version string) error {
	// Temporary download path.
	filename := "kubernetes.tar.gz"
	tmpDirPath := filepath.Join(workDir, "kube")
	downloadPath := filepath.Join(tmpDirPath, filename)

	// Format url.
	downloadUrl := fmt.Sprintf(imageUrlTemplate, version)
	glog.V(1).Infof("About to download kube release using url: %q", downloadUrl)

	if err := exec.Command("mkdir", "-p", tmpDirPath).Run(); err != nil {
		return fmt.Errorf("failed to mkdir -p %s - %v", tmpDirPath, err)
	}

	// Download kube code and store it in a temp dir.
	if err := exec.Command("curl", "-L", downloadUrl, "-o", downloadPath).Run(); err != nil {
		return fmt.Errorf("failed to curl kubernetes release @ %q to %s  - %v", downloadUrl, downloadPath, err)
	}

	// Un-tar kube release.
	if err := exec.Command("tar", "-xf", downloadPath, "-C", workDir).Run(); err != nil {
		return fmt.Errorf("failed to un-tar kubernetes release at %q - %v", downloadPath, err)
	}
	return nil
}

func getKubeClient() (string, *kclient.Client, error) {
	c, err := kclientcmd.LoadFromFile(*kubeConfig)
	if err != nil {
		return "", nil, fmt.Errorf("error loading kubeConfig: %v", err.Error())
	}
	config, err := kclientcmd.NewDefaultClientConfig(
		*c,
		&kclientcmd.ConfigOverrides{
			ClusterInfo: kclientcmdkapi.Cluster{
				APIVersion: "v1",
			},
		}).ClientConfig()
	if err != nil {
		return "", nil, fmt.Errorf("error parsing kubeConfig: %v", err.Error())
	}
	kubeClient, err := kclient.New(config)
	if err != nil {
		return "", nil, fmt.Errorf("error creating client - %q", err)
	}

	return c.Clusters[c.CurrentContext].Server, kubeClient, nil
}

func validateCluster(baseDir string) bool {
	glog.V(1).Info("validating existing cluster")
	out, err := runKubeClusterCommand(baseDir, "validate-cluster.sh")
	if err != nil {
		glog.V(1).Infof("cluster validation failed - %q\n %s", err, out)
		return false
	}
	return true
}

func requireNewCluster(baseDir, version string) bool {
	// Setup kube client
	_, kubeClient, err := getKubeClient()
	if err != nil {
		glog.V(1).Infof("kube client creation failed - %q", err)
		return true
	}
	glog.V(1).Infof("checking if existing cluster can be used")
	versionInfo, err := kubeClient.ServerVersion()
	if err != nil {
		glog.Errorf("failed to get kube version info - %q", err)
		return true
	}
	return !strings.Contains(versionInfo.GitVersion, version)
}

func downloadAndSetupCluster(version string) (baseDir string, err error) {
	// Create a temp dir to store the kube release files.
	tempDir := filepath.Join(*workDir, version)
	if !exists(tempDir) {
		if err := os.MkdirAll(tempDir, 0700); err != nil {
			return "", fmt.Errorf("failed to create a temp dir at %s - %q", tempDir, err)
		}
		glog.V(1).Infof("Successfully setup work dir at %s", tempDir)
	}

	kubeBaseDir := filepath.Join(tempDir, "kubernetes")

	if !exists(kubeBaseDir) {
		if err := downloadRelease(tempDir, version); err != nil {
			return "", err
		}
		glog.V(1).Infof("Successfully downloaded kubernetes release at %s", tempDir)
	}

	// Disable monitoring
	if err := disableClusterMonitoring(kubeBaseDir); err != nil {
		return "", fmt.Errorf("failed to disable cluster monitoring in kube cluster config - %q", err)
	}
	glog.V(1).Info("Disabled cluster monitoring")
	if !requireNewCluster(kubeBaseDir, version) {
		glog.V(1).Infof("skipping cluster setup since a cluster with required version already exists")
		return kubeBaseDir, nil
	}

	// Setup kube cluster
	glog.V(1).Infof("Setting up new kubernetes cluster version: %s", version)
	if err := setupNewCluster(kubeBaseDir); err != nil {
		// Cluster setup failed for some reason.
		// Attempting to validate the cluster to see if it failed in the validate phase.
		sleepDuration := 10 * time.Second
		clusterReady := false
		for i := 0; i < int(time.Minute/sleepDuration); i++ {
			if !validateCluster(kubeBaseDir) {
				glog.Infof("Retry validation after %v seconds.", sleepDuration/time.Second)
				time.Sleep(sleepDuration)
			} else {
				clusterReady = true
				break
			}
		}
		if !clusterReady {
			return "", fmt.Errorf("failed to setup cluster - %q", err)
		}
	}
	glog.V(1).Infof("Successfully setup new kubernetes cluster version %s", version)

	return kubeBaseDir, nil
}

func newKubeFramework(version string) (kubeFramework, error) {
	var err error
	kubeBaseDir := ""
	if version != "" {
		if len(strings.Split(version, ".")) != 3 {
			return nil, fmt.Errorf("invalid kubernetes version specified - %q", version)
		}
		kubeBaseDir, err = downloadAndSetupCluster(version)
		if err != nil {
			return nil, err
		}
	}

	// Setup kube client
	masterIP, kubeClient, err := getKubeClient()
	if err != nil {
		return nil, err
	}
	return &realKubeFramework{
		kubeClient: kubeClient,
		baseDir:    kubeBaseDir,
		version:    version,
		masterIP:   masterIP,
	}, nil
}

func (self *realKubeFramework) Client() *kclient.Client {
	return self.kubeClient
}

func (self *realKubeFramework) loadObject(filePath string) (runtime.Object, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read object: %v", err)
	}
	return runtime.YAMLDecoder(kapi.Codec).Decode(data)
}

func (self *realKubeFramework) ParseRC(filePath string) (*kapi.ReplicationController, error) {
	obj, err := self.loadObject(filePath)
	if err != nil {
		return nil, err
	}

	rc, ok := obj.(*kapi.ReplicationController)
	if !ok {
		return nil, fmt.Errorf("Failed to cast replicationController: %v", obj)
	}
	return rc, nil
}

func (self *realKubeFramework) ParseService(filePath string) (*kapi.Service, error) {
	obj, err := self.loadObject(filePath)
	if err != nil {
		return nil, err
	}
	service, ok := obj.(*kapi.Service)
	if !ok {
		return nil, fmt.Errorf("Failed to cast service: %v", obj)
	}
	return service, nil
}

func (self *realKubeFramework) CreateService(ns string, service *kapi.Service) (*kapi.Service, error) {
	service.Namespace = ns
	newSvc, err := self.kubeClient.Services(ns).Create(service)
	return newSvc, err
}

func (self *realKubeFramework) DeleteNs(ns string) error {
	if _, err := self.kubeClient.Namespaces().Get(ns); err != nil {
		glog.V(0).Infof("Cannot get namespace %q. Skipping deletion.", ns)
		return nil
	}

	glog.V(0).Infof("Deleting namespace %s", ns)
	self.kubeClient.Namespaces().Delete(ns)

	for i := 0; i < 5; i++ {
		glog.Infof("Checking for namespace %s", ns)
		_, err := self.kubeClient.Namespaces().Get(ns)
		if err != nil {
			glog.V(0).Infof("%s doesn't exist", ns)
			return nil
		}
		time.Sleep(10 * time.Second)
	}
	return fmt.Errorf("Namespace %s still exists", ns)
}

func (self *realKubeFramework) CreateNs(ns *kapi.Namespace) (*kapi.Namespace, error) {
	return self.kubeClient.Namespaces().Create(ns)
}

func (self *realKubeFramework) CreateRC(ns string, rc *kapi.ReplicationController) (*kapi.ReplicationController, error) {
	rc.Namespace = ns
	return self.kubeClient.ReplicationControllers(ns).Create(rc)
}

func (self *realKubeFramework) DestroyCluster() {
	destroyCluster(self.baseDir)
}

func (self *realKubeFramework) GetProxyUrlForService(service *kapi.Service) string {
	return fmt.Sprintf("%s/api/v1/proxy/namespaces/default/services/%s/", self.masterIP, service.Name)
}

func (self *realKubeFramework) GetNodes() ([]string, error) {
	var nodes []string
	nodeList, err := self.kubeClient.Nodes().List(labels.Everything(), fields.Everything())
	if err != nil {
		return nodes, err
	}

	for _, node := range nodeList.Items {
		nodes = append(nodes, node.Name)
	}
	return nodes, nil
}

func (self *realKubeFramework) GetRunningPods() ([]*kapi.Pod, error) {
	podList, err := self.kubeClient.Pods(kapi.NamespaceAll).List(labels.Everything(), fields.Everything())
	if err != nil {
		return nil, err
	}
	pods := []*kapi.Pod{}
	for _, pod := range podList.Items {
		if pod.Status.Phase == kapi.PodRunning && !strings.Contains(pod.Spec.NodeName, "kubernetes-master") {
			pods = append(pods, &pod)
		}
	}
	return pods, nil
}

func (self *realKubeFramework) GetRunningPodNames() ([]string, error) {
	var pods []string
	podList, err := self.GetRunningPods()
	if err != nil {
		return pods, err
	}
	for _, pod := range podList {
		pods = append(pods, string(pod.Name))
	}
	return pods, nil
}

func (rkf *realKubeFramework) WaitUntilPodRunning(ns string, podLabels map[string]string, timeout time.Duration) error {
	glog.V(2).Infof("Waiting for pod %v in %s...", podLabels, ns)
	podsInterface := rkf.Client().Pods(ns)
	for i := 0; i < int(timeout/time.Second); i++ {
		selector := labels.Set(podLabels).AsSelector()
		podList, err := podsInterface.List(selector, fields.Everything())
		if err != nil {
			glog.V(1).Info(err)
			return err
		}
		if len(podList.Items) > 0 {
			podSpec := podList.Items[0]
			if podSpec.Status.Phase == kapi.PodRunning {
				return nil
			}
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("pod not in running state after %d", timeout/time.Second)
}

func (rkf *realKubeFramework) WaitUntilServiceActive(svc *kapi.Service, timeout time.Duration) error {
	glog.V(2).Infof("Waiting for endpoints in service %s/%s", svc.Namespace, svc.Name)
	for i := 0; i < int(timeout/time.Second); i++ {
		e, err := rkf.Client().Endpoints(svc.Namespace).Get(svc.Name)
		if err != nil {
			return err
		}
		if len(e.Subsets) > 0 {
			return nil
		}
		time.Sleep(time.Second)
	}

	return fmt.Errorf("Service %q not active after %d seconds - no endpoints found", svc.Name, timeout/time.Second)

}
