package main

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

const PluginName = "startree-pinot-datasource"

const ArtifactoryUrl = "https://repo.startreedata.io/artifactory"
const ArtifactoryRepository = "startree-grafana-plugin"

const PackageJsonFile = "package.json"
const BuildArtifactsDir = "dist"
const PodInstallerFile = "pod_installer.sh"

const DemoK8sNamespace = "cell-9itmgf-default"
const DemoK8sPod = "pinot-grafana-demo-0"
const DemoContainerPluginPath = "/var/lib/grafana/plugins"
const DemoGrafanaUrl = "https://pinot-grafana-demo.9itmgf.cp.s7e.startree.cloud"
const DemoK8sContext = "scp-acc-2j9kjvzye-2j9itmj9e:Apps_Team:startree-apps"

const ZipCmd = "zip"
const UnzipCmd = "unzip"
const KubectlCmd = "kubectl"

const CloudVersionV1 = "v1"
const CloudVersionV2 = "v2"

func printUsage() {
	fmt.Printf("Usage: %s create|resign|install|demo [ROOT_URLS...]\n", os.Args[0])
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	releaseManager := new(ReleaseManager)

	switch action := os.Args[1]; action {
	case "create":
		releaseManager.CreateInternalRelease()

	case "resign":
		rootUrls := parseRootUrls(os.Args[2:])
		releaseManager.ResignRelease(rootUrls)

	case "deploy_demo":
		releaseManager.DeployDemo()

	case "pod_installer":
		releaseManager.uploadFile("cmd/release/" + PodInstallerFile)

	case "install":
		releaseManager.InstallPluginIntoDataPlane()

	case "demo":
		releaseManager.DeployDemo()

	default:
		printUsage()
		os.Exit(1)
	}
}

type ReleaseManager struct {
	releaseName          string
	currentVersion       string
	latestReleaseVersion string
	artifactoryToken     string

	cloudVersion                  string
	deploymentNamespace           string
	k8sDeploymentArtifactoryToken string
	grafanaDeploymentRootUrl      string
}

func (x *ReleaseManager) CreateInternalRelease() {
	_ = x.getArtifactoryToken()

	version := x.getCurrentVersion()

	releaseArchive := fmt.Sprintf("%s-%s.zip", PluginName, version)
	fmt.Println("Preparing release", releaseArchive)

	rootUrls := []string{DemoGrafanaUrl}
	fmt.Println("Valid for urls:", strings.Join(rootUrls, " "))

	x.buildPlugin()
	x.signPlugin(rootUrls)
	x.zipPlugin(releaseArchive)
	x.uploadFile(releaseArchive)
}

func (x *ReleaseManager) ResignRelease(rootUrls []string) {
	_ = x.getArtifactoryToken()

	internalRelease := x.getInternalArchiveName()
	customerRelease := x.getCustomerArchiveName()

	fmt.Println("Preparing release", customerRelease)
	fmt.Println("Valid for urls:", strings.Join(rootUrls, " "))

	x.downloadFile(internalRelease)
	x.unzipPlugin(internalRelease)
	x.signPlugin(rootUrls)
	x.zipPlugin(customerRelease)
	x.uploadFile(customerRelease)
}

func (x *ReleaseManager) DeployDemo() {
	fmt.Println("Preparing demo...")

	if x.getK8sContext() != DemoK8sContext {
		fmt.Println("Please set k8s context from https://admin-portal.startree.cloud/accounts/acct_2X0clUhn6jdER10iW0YFZuEbnvt/organizations/org-2j9iooe5zcog/environments/env-2j9itlsbtdax/account")
		os.Exit(1)
	}

	localArchive := fmt.Sprintf("%s.zip", PluginName)
	containerArchive := path.Join(DemoContainerPluginPath, localArchive)

	x.buildPlugin()
	x.signPlugin([]string{DemoGrafanaUrl})
	x.zipPlugin(localArchive)

	runCmd(nil, nil, KubectlCmd, "--namespace", DemoK8sNamespace,
		"cp", localArchive, DemoK8sPod+":"+containerArchive)
	runCmd(nil, nil, KubectlCmd, "--namespace", DemoK8sNamespace,
		"exec", DemoK8sPod, "--", "rm", "-rf", path.Join(DemoContainerPluginPath, PluginName))
	runCmd(nil, nil, KubectlCmd, "--namespace", DemoK8sNamespace,
		"exec", DemoK8sPod, "--", "unzip", containerArchive, "-d", DemoContainerPluginPath)
	runCmd(nil, nil, KubectlCmd, "--namespace", DemoK8sNamespace,
		"delete", "pod", DemoK8sPod)

	fmt.Println("Demo deployed.")
}

func (x *ReleaseManager) InstallPluginIntoDataPlane() {
	_ = x.getArtifactoryTokenForK8sDeployment()
	_ = x.getReleaseName()

	customerArchive := x.getCustomerArchiveName()
	if !x.fileExistsInRepo(customerArchive) {
		fmt.Println("Please create the customer release.")
		fmt.Printf("Run `RELEASE_NAME=\"%s\" go run cmd/release/main.go resign https://ROOT_URL`\n", x.getReleaseName())
		os.Exit(1)
	}

	fmt.Println("Installing plugin into Grafana...")
	fmt.Println("Using k8s context", x.getK8sContext())
	deployment := x.getK8sDeployment(x.getGrafanaDeploymentName())
	x.patchGrafanaDeployment(deployment)
	x.applyK8sConfig(deployment)
	fmt.Println("Installation complete.")
}

func (x *ReleaseManager) getInternalArchiveName() string {
	version := x.getLatestReleaseVersion()
	return fmt.Sprintf("%s-%s.zip", PluginName, version)
}

func (x *ReleaseManager) getCustomerArchiveName() string {
	version := x.getLatestReleaseVersion()
	releaseName := x.getReleaseName()
	return fmt.Sprintf("%s-%s-%s.zip", PluginName, version, releaseName)
}

func (x *ReleaseManager) getCurrentVersion() string {
	if x.currentVersion != "" {
		return x.currentVersion
	}

	file, err := os.Open(PackageJsonFile)
	if err != nil {
		handleError(fmt.Errorf("os.Open(`%s`) failed: %w", PackageJsonFile, err))
	}
	defer func() { handleError(file.Close()) }()

	var data struct {
		Version string `json:"version"`
	}
	decodeJson(file, &data)

	if data.Version == "" {
		handleError(errors.New("unable to determine package version"))
	}

	x.currentVersion = data.Version
	return x.currentVersion
}

func (x *ReleaseManager) getLatestReleaseVersion() string {
	if x.latestReleaseVersion != "" {
		return x.latestReleaseVersion
	}

	var buf bytes.Buffer
	runCmd(nil, &buf, "git", "tag", "--list", "--sort=-v:refname")
	version := strings.SplitN(buf.String(), "\n", 2)[0]
	version = strings.TrimPrefix(version, "v")
	if version == "" {
		handleError(fmt.Errorf("no version found in git tags"))
	}
	x.latestReleaseVersion = version
	return x.latestReleaseVersion
}

func (x *ReleaseManager) getArtifactoryToken() string {
	if x.artifactoryToken != "" {
		return x.artifactoryToken
	}

	artifactoryToken := os.Getenv("ARTIFACTORY_TOKEN")
	if artifactoryToken == "" {
		handleError(errors.New("environment variable `ARTIFACTORY_TOKEN` is required"))
	}
	x.artifactoryToken = artifactoryToken
	return artifactoryToken
}

func (x *ReleaseManager) getArtifactoryTokenForK8sDeployment() string {
	if x.k8sDeploymentArtifactoryToken != "" {
		return x.k8sDeploymentArtifactoryToken
	}
	token := os.Getenv("ARTIFACTORY_TOKEN_FOR_K8S_DEPLOYMENT")
	if token == "" {
		handleError(errors.New("environment variable `ARTIFACTORY_TOKEN_FOR_K8S_DEPLOYMENT` is required"))
	}
	x.k8sDeploymentArtifactoryToken = token
	return x.k8sDeploymentArtifactoryToken
}

func (x *ReleaseManager) grafanaAccessPolicyTokenExists() {
	if os.Getenv("GRAFANA_ACCESS_POLICY_TOKEN") == "" {
		handleError(errors.New("environment variable `GRAFANA_ACCESS_POLICY_TOKEN` is required"))
	}
}

func (x *ReleaseManager) getReleaseName() string {
	if x.releaseName != "" {
		return x.releaseName
	}

	releaseName := os.Getenv("RELEASE_NAME")
	if releaseName == "" {
		handleError(fmt.Errorf("environment variable `RELEASE_NAME` is required"))
	}
	if ok, _ := regexp.Match(`^[\w-]+$`, []byte(releaseName)); !ok {
		handleError(fmt.Errorf("invalid release name `%s`: release name can only contain alphanumeric characters", releaseName))
	}
	x.releaseName = releaseName
	return x.releaseName
}

func (x *ReleaseManager) uploadFile(path string) {
	fmt.Printf("Uploading %s...\n", path)

	file, err := os.Open(path)
	if err != nil {
		handleError(fmt.Errorf("os.Open(`%s`) failed: %w", path, err))
	}

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(file); err != nil {
		handleError(fmt.Errorf("file.Read failed: %w", err))
	}
	sha256sum := sha256.Sum256(buf.Bytes())
	sha1sum := sha1.Sum(buf.Bytes())
	md5sum := md5.Sum(buf.Bytes())

	dest := fmt.Sprintf("%s/%s/%s", ArtifactoryUrl, ArtifactoryRepository, filepath.Base(path))
	req := x.newArtifactoryRequest(http.MethodPut, dest, &buf)
	req.Header.Add("Content-Type", "application/zip")
	req.Header.Add("X-Checksum-Sha256", fmt.Sprintf("%x", sha256sum))
	req.Header.Add("X-Checksum-Sha1", fmt.Sprintf("%x", sha1sum))
	req.Header.Add("X-Checksum-Md5", fmt.Sprintf("%x", md5sum))

	resp, err := x.doRequest(req)
	if err != nil {
		handleError(fmt.Errorf("failed to upload file: %w", err))
	}
	defer func() { handleError(resp.Body.Close()) }()

	if resp.StatusCode != http.StatusCreated {
		handleError(fmt.Errorf("failed to upload file: %s", resp.Status))
	}

	fmt.Println("File uploaded to", dest)
}

func (x *ReleaseManager) downloadFile(fileName string) {
	fmt.Printf("Downloading %s...\n", fileName)

	req := x.newArtifactoryRequest(http.MethodGet, x.getRepoFileUrl(fileName), nil)

	resp, err := x.doRequest(req)
	if err != nil {
		handleError(fmt.Errorf("failed to download file: %w", err))
	}
	defer func() { handleError(resp.Body.Close()) }()

	if resp.StatusCode != http.StatusOK {
		handleError(fmt.Errorf("failed to download file: %s", resp.Status))
	}

	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		handleError(fmt.Errorf("os.OpenFile(`%s`) failed: %w", fileName, err))
	}
	defer func() { handleError(file.Close()) }()

	if _, err = io.Copy(file, resp.Body); err != nil {
		handleError(fmt.Errorf("io.Copy failed: %w", err))
	}

	fmt.Println("File downloaded to", fileName)

	x.checkFileIntegrity(fileName, resp.Header.Get("x-checksum-sha256"))
}

func (x *ReleaseManager) fileExistsInRepo(fileName string) bool {
	req := x.newArtifactoryRequest(http.MethodHead, x.getRepoFileUrl(fileName), nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		handleError(fmt.Errorf("failed to download file: %w", err))
	}
	defer func() { handleError(resp.Body.Close()) }()

	if resp.StatusCode == http.StatusOK {
		return true
	} else if resp.StatusCode == http.StatusNotFound {
		return false
	} else {
		handleError(fmt.Errorf("unexpected status code: %d", resp.StatusCode))
		return false
	}
}

func (x *ReleaseManager) checkFileIntegrity(fileName string, expectedChecksum string) {
	fmt.Println("Checking file integrity...")
	file, err := os.Open(fileName)
	if err != nil {
		handleError(fmt.Errorf("os.OpenFile(`%s`) failed: %w", fileName, err))
	}
	defer func() { handleError(file.Close()) }()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		handleError(fmt.Errorf("io.Copy failed: %w", err))
	}
	fileChecksum := fmt.Sprintf("%x", hash.Sum(nil))
	if fileChecksum != expectedChecksum {
		handleError(errors.New("file integrity check failed"))
	}
	fmt.Println("File integrity verified.")
}

func (x *ReleaseManager) newArtifactoryRequest(method string, path string, body io.Reader) *http.Request {
	token := x.getArtifactoryToken()
	req, err := http.NewRequest(method, path, body)
	handleError(err)
	req.Header.Add("X-JFrog-Art-Api", token)
	return req
}

func (x *ReleaseManager) doRequest(req *http.Request) (*http.Response, error) {
	fmt.Println("HTTP", req.Method, req.URL)
	return http.DefaultClient.Do(req)
}

func (x *ReleaseManager) getRepoFileUrl(fileName string) string {
	return fmt.Sprintf("%s/%s/%s", ArtifactoryUrl, ArtifactoryRepository, fileName)
}

func (x *ReleaseManager) buildPlugin() {
	fmt.Println("Building plugin...")

	removeAll(BuildArtifactsDir)
	runCmd(nil, nil, "mage", "-v")
	runCmd(nil, nil, "npm", "run", "build")
}

func (x *ReleaseManager) unzipPlugin(src string) {
	removeAll(PluginName)
	runCmd(nil, nil, UnzipCmd, src)
	removeAll(BuildArtifactsDir)
	renameFile(PluginName, BuildArtifactsDir)
}

func (x *ReleaseManager) signPlugin(rootUrls []string) {
	fmt.Println("Signing plugin...")
	runCmd(nil, nil,
		"npx", "@grafana/sign-plugin@latest", "--rootUrls", strings.Join(rootUrls, ","))
}

func (x *ReleaseManager) zipPlugin(dest string) {
	removeAll(PluginName)
	renameFile(BuildArtifactsDir, PluginName)
	removeAll(dest)
	runCmd(nil, nil, ZipCmd, "-r", dest, PluginName)
	renameFile(PluginName, BuildArtifactsDir)
}

func (x *ReleaseManager) getCloudVersion() string {
	if x.cloudVersion != "" {
		return x.cloudVersion
	}

	if x.getK8sNamespace() == "default" {
		x.cloudVersion = CloudVersionV1
	} else {
		x.cloudVersion = CloudVersionV2
	}
	return x.cloudVersion
}

func (x *ReleaseManager) getGrafanaDeploymentName() string {
	if x.getCloudVersion() == CloudVersionV1 {
		return "startree-platform-grafana"
	} else {
		return "startree-infra-grafana"
	}
}

func (x *ReleaseManager) getK8sNamespace() string {
	if x.deploymentNamespace != "" {
		return x.deploymentNamespace
	}

	var buf bytes.Buffer
	runCmd(nil, &buf, KubectlCmd, "get", "configmaps", "--all-namespaces", "-o", "json")

	var data struct {
		Items []struct {
			Metadata struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
		} `json:"items"`
	}
	decodeJson(&buf, &data)

	for _, item := range data.Items {
		name := item.Metadata.Name
		if strings.HasPrefix(name, "startree-") && strings.HasSuffix(name, "-dex") {
			x.deploymentNamespace = item.Metadata.Namespace
			fmt.Println("Deployment namespace:", item.Metadata.Namespace)
			return x.deploymentNamespace
		}
	}
	handleError(errors.New("could not find deployment namespace"))
	return ""
}

type DeploymentConfig map[string]interface{}

func (x *ReleaseManager) getK8sDeployment(deploymentName string) DeploymentConfig {
	var buf bytes.Buffer
	runCmd(nil, &buf, KubectlCmd, "--namespace", x.getK8sNamespace(),
		"get", "deployment", deploymentName, "--output", "json")

	var deployment map[string]interface{}
	decodeJson(&buf, &deployment)
	return deployment
}

func (x *ReleaseManager) patchGrafanaDeployment(deployment DeploymentConfig) {
	spec := deployment["spec"].(map[string]interface{})["template"].(map[string]interface{})["spec"].(map[string]interface{})

	spec["volumes"] = append(spec["volumes"].([]interface{}), []interface{}{
		map[string]interface{}{
			"name":     "plugins",
			"emptyDir": map[string]interface{}{},
		},
		map[string]interface{}{
			"name":     "files",
			"emptyDir": map[string]interface{}{},
		},
	}...)

	spec["initContainers"] = []map[string]interface{}{x.getInitContainerConfig()}

	grafanaContainer := spec["containers"].([]interface{})[0].(map[string]interface{})
	var grafanaVolumeMounts []interface{}
	for _, vol := range grafanaContainer["volumeMounts"].([]interface{}) {
		if x.isInitVolume(vol) {
			continue
		}
		grafanaVolumeMounts = append(grafanaVolumeMounts, vol.(map[string]interface{}))
	}
	grafanaContainer["volumeMounts"] = append(grafanaVolumeMounts, x.getInitVolumeMountsConfig()...)
}

func (x *ReleaseManager) getInitContainerConfig() map[string]interface{} {
	env := []map[string]interface{}{
		{
			"name":  "WORK_DIR",
			"value": "/files",
		},
		{
			"name":  "INSTALL_DIR",
			"value": "/var/lib/grafana/plugins",
		},
		{
			"name":  "ARCHIVE_SRC",
			"value": x.getRepoFileUrl(x.getCustomerArchiveName()),
		},
		{
			"name":  "ARTIFACTORY_TOKEN",
			"value": x.getArtifactoryTokenForK8sDeployment(),
		},
	}

	return map[string]interface{}{
		"image": "curlimages/curl:7.73.0",
		"name":  "install-plugins",
		"env":   env,
		"command": []string{
			"sh",
			"-c",
			`curl -XGET https://repo.startreedata.io/artifactory/startree-grafana-plugin/pod_installer.sh --silent --header "X-JFrog-Art-Api: ${ARTIFACTORY_TOKEN}" | sh`,
		},
		"volumeMounts": x.getInitVolumeMountsConfig(),
	}
}

func (x *ReleaseManager) isInitVolume(volMount interface{}) bool {
	initVolumes := x.getInitVolumeMountsConfig()
	name := volMount.(map[string]interface{})["name"].(string)
	for i := range initVolumes {
		if initVolumes[i].(map[string]interface{})["name"] == name {
			return true
		}
	}
	return false
}

func (x *ReleaseManager) getInitVolumeMountsConfig() []interface{} {
	return []interface{}{
		map[string]interface{}{
			"name":      "plugins",
			"mountPath": "/var/lib/grafana/plugins",
		},
		map[string]interface{}{
			"name":      "files",
			"mountPath": "/files",
		},
	}
}

func (x *ReleaseManager) getK8sContext() string {
	var buf bytes.Buffer
	runCmd(nil, &buf, KubectlCmd, "config", "current-context")
	return strings.TrimSpace(buf.String())
}

func (x *ReleaseManager) getGrafanaDeploymentRootUrl() string {
	if x.grafanaDeploymentRootUrl != "" {
		return x.grafanaDeploymentRootUrl
	}

	var buf bytes.Buffer

	runCmd(nil, &buf, KubectlCmd, "--namespace", x.getK8sNamespace(),
		"get", "configmap", x.getGrafanaConfigmapName(), "--output", "json")

	var data struct {
		Data map[string]string `json:"data"`
	}
	decodeJson(&buf, &data)
	grafanaIniContents := data.Data["grafana.ini"]

	var rootUrl string
	for _, entry := range strings.Split(grafanaIniContents, "\n") {
		entry = strings.TrimSpace(entry)
		if !strings.HasPrefix(entry, "root_url") {
			continue
		}
		fields := strings.SplitN(entry, "=", 2)
		if len(fields) == 2 {
			rootUrl = strings.TrimSpace(fields[1])
		}
		break
	}

	if rootUrl == "" {
		handleError(errors.New("could not find root url of grafana instance"))
	}
	_, err := url.Parse(rootUrl)
	handleError(err)

	x.grafanaDeploymentRootUrl = rootUrl
	return x.grafanaDeploymentRootUrl
}

func (x *ReleaseManager) getGrafanaConfigmapName() string {
	if x.cloudVersion == CloudVersionV1 {
		return "startree-platform-grafana"
	} else {
		return "startree-infra-grafana"
	}
}

func (x *ReleaseManager) applyK8sConfig(data map[string]interface{}) {
	var buf bytes.Buffer
	handleError(json.NewEncoder(&buf).Encode(data))
	runCmd(&buf, nil, KubectlCmd, "--namespace", x.getK8sNamespace(),
		"apply", "--filename", "-")
}

func parseRootUrls(args []string) []string {
	re := regexp.MustCompile(`[ ,]`)
	var rawUrls []string
	for i := range args {
		rawUrls = append(rawUrls, re.Split(args[i], -1)...)
	}

	rootUrls := make([]string, 0, len(rawUrls))
	for _, u := range rawUrls {
		u = strings.TrimSpace(u)
		u = strings.TrimSuffix(u, "/")
		if u != "" {
			rootUrls = append(rootUrls, u)
		}
	}
	rootUrls = rootUrls[:]

	for _, u := range rootUrls {
		if _, err := url.Parse(u); err != nil {
			handleError(fmt.Errorf("invalid URL: `%s`", u))
		}
	}

	if len(rootUrls) == 0 {
		handleError(errors.New("no signing urls provided; nothing to do"))
	}

	return rootUrls
}

func runCmd(input io.Reader, output io.Writer, command string, args ...string) {
	cmd := exec.Command(command, args...)
	cmd.Stdin = input
	cmd.Stdout = output
	fmt.Println("Executing:", cmd.String())
	if err := cmd.Run(); err != nil {
		handleError(fmt.Errorf("command failed: %w", err))
	}
}

func decodeJson(src io.Reader, dest interface{}) {
	if err := json.NewDecoder(src).Decode(dest); err != nil {
		handleError(fmt.Errorf("json.Decode() failed: %v", err))
	}
}

func encodeJson(src interface{}, dest io.Writer) {
	if err := json.NewEncoder(dest).Encode(src); err != nil {
		handleError(fmt.Errorf("json.Encode() failed: %v", err))
	}
}

func removeAll(path string) {
	if err := os.RemoveAll(path); err != nil {
		handleError(fmt.Errorf("os.RemoveAll(`%s`) failed: %w", path, err))
	}
}

func renameFile(oldpath string, newpath string) {
	if err := os.Rename(oldpath, newpath); err != nil {
		handleError(fmt.Errorf("os.Rename(`%s`, `%s`) failed: %w", oldpath, newpath, err))
	}
}

func handleError(err error) {
	if err != nil {
		fmt.Printf("Error: %s.\n", err)
		os.Exit(1)
	}
}
