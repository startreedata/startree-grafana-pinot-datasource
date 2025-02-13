package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/magefile/mage/mage"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const PluginName = "startree-pinot-datasource"

const ArtifactoryUrl = "https://repo.startreedata.io/artifactory"
const ArtifactoryRepository = "startree-grafana-plugin"

type ExternalRelease struct {
	name string
	repo string
	urls []string
}

// Hard coded external releases

var ExternalReleases = []ExternalRelease{{
	name: "epicgames",
	repo: "external-startree-releases-for-epicgames",
	urls: []string{"https://grafana.ol.epicgames.net", "https://grafana.pilot.epicgames.startree.cloud/"},
}, {
	name: "doordash",
	repo: "external-startree-releases-for-doordash",
	urls: []string{"https://grafana.doordash.team/"},
}}

const PackageJsonFile = "package.json"
const BuildArtifactsDir = "dist"
const PodInstallerFile = "pod_installer.sh"

type DemoRelease struct {
	Name         string
	K8sNamespace string
	K8sContext   string
	GrafanaUrl   string
	AdminUrl     string
}

var DemoReleases = []DemoRelease{{
	Name:         "internal",
	K8sNamespace: "default",
	K8sContext:   "arn:aws:eks:us-west-2:381492139006:cluster/sc-analytics-metrics",
	GrafanaUrl:   "https://pinot-grafana-demo.metrics.analytics.startree.cloud",
	AdminUrl:     "https://admin.startree.cloud/admin/organizations/dd56b223-3018-4beb-bb1a-7084220bf556/environments/dd56b223-3018-4beb-bb1a-7084220bf556-metrics",
}, {
	Name:         "external",
	K8sNamespace: "cell-wvqikq-default",
	K8sContext:   "scs-acc-2mpxfsc0y-2owvqjimo:ConsistentPush:galileo-demo-setup",
	GrafanaUrl:   "https://pinot-grafana-demo.wvqikq.cp.s7e.startree-staging.cloud/",
	AdminUrl:     "https://admin-portal.startree-staging.cloud/accounts/acct-2mpg1mgad538/organizations/org-2mpg7mzzzum9/environments/env-2owvqjdb5ptd/account",
}}

const DemoK8sPod = "pinot-grafana-demo-0"
const DemoContainerPluginPath = "/var/lib/grafana/plugins"

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

	case "external":
		target := os.Args[2]
		for _, release := range ExternalReleases {
			if release.name == target {
				releaseManager.CreateExternalRelease(release)
				return
			}
		}
		fmt.Printf("No release for `%s` found!\n", target)
		os.Exit(1)

	case "resign":
		rootUrls := parseRootUrls(os.Args[2:])
		releaseManager.ResignRelease(rootUrls)

	case "pod_installer":
		releaseManager.uploadInternalFile("cmd/release/" + PodInstallerFile)

	case "install":
		releaseManager.InstallPluginIntoDataPlane()

	case "demo":
		target := os.Args[2]
		for _, demo := range DemoReleases {
			if demo.Name == target {
				releaseManager.DeployDemo(demo)
				return
			}
		}
		fmt.Printf("No demo for `%s` found!\n", target)
		os.Exit(1)

	case "build_backend":
		releaseManager.buildBackend()

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

	var rootUrls []string
	for _, demo := range DemoReleases {
		rootUrls = append(rootUrls, demo.GrafanaUrl)
	}
	fmt.Println("Valid for urls:", strings.Join(rootUrls, " "))

	x.buildPlugin()
	x.signPlugin(rootUrls)
	x.zipPlugin(releaseArchive)
	x.uploadInternalFile(releaseArchive)
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
	x.uploadInternalFile(customerRelease)
}

func (x *ReleaseManager) CreateExternalRelease(release ExternalRelease) {
	x.releaseName = release.name
	x.ResignRelease(release.urls)
	if release.repo != "" {
		x.uploadExternalFile(x.getCustomerArchiveName(), release.repo)
	}
}

func (x *ReleaseManager) DeployDemo(demo DemoRelease) {
	fmt.Println("Preparing demo...")

	if x.getK8sContext() != demo.K8sContext {
		fmt.Printf("Please set k8s context from %s\n", demo.AdminUrl)
		os.Exit(1)
	}

	localArchive := fmt.Sprintf("%s.zip", PluginName)
	containerArchive := path.Join(DemoContainerPluginPath, localArchive)

	x.buildPlugin()
	x.signPlugin([]string{demo.GrafanaUrl})
	x.zipPlugin(localArchive)

	runCmd(nil, nil, KubectlCmd, "--namespace", demo.K8sNamespace,
		"cp", localArchive, DemoK8sPod+":"+containerArchive)
	runCmd(nil, nil, KubectlCmd, "--namespace", demo.K8sNamespace,
		"exec", DemoK8sPod, "--", "rm", "-rf", path.Join(DemoContainerPluginPath, PluginName))
	runCmd(nil, nil, KubectlCmd, "--namespace", demo.K8sNamespace,
		"exec", DemoK8sPod, "--", "unzip", containerArchive, "-d", DemoContainerPluginPath)
	runCmd(nil, nil, KubectlCmd, "--namespace", demo.K8sNamespace,
		"delete", "pod", DemoK8sPod)

	fmt.Printf("Waiting for Grafana...")
	for {
		resp, err := http.Get(demo.GrafanaUrl)
		if err == nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusFound) {
			fmt.Println("Demo deployed!\nVisit", demo.GrafanaUrl)
			return
		}
		time.Sleep(1 * time.Second)
		fmt.Print(".")
	}
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
	fmt.Printf("Use this k8s context: %s [N/y]? ", x.getK8sContext())
	if !readYesNo() {
		return
	}

	oldDeployment := x.getK8sDeployment(x.getGrafanaDeploymentName())
	// TODO: Deep copy
	newDeployment := x.getK8sDeployment(x.getGrafanaDeploymentName())
	x.patchGrafanaDeployment(newDeployment)

	fmt.Println("Makes these changes to the grafana deployment:")
	printDiff(oldDeployment, newDeployment)

	fmt.Printf("Are you sure you want to continue [N/y]? ")
	if !readYesNo() {
		return
	}

	x.applyK8sConfig(newDeployment)
	fmt.Println("Installation complete.")
}

func readYesNo() bool {
	ans, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	switch strings.TrimSpace(ans) {
	case "y", "Y":
		return true
	default:
		return false
	}
}

func printDiff(old, new DeploymentConfig) {
	writeConfig := func(name string, config DeploymentConfig) {
		f, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		handleError(err)
		defer f.Close()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		handleError(enc.Encode(config))
	}

	handleError(os.MkdirAll("tmp", 0700))
	defer os.RemoveAll("tmp")

	writeConfig("tmp/before", old)
	writeConfig("tmp/after", new)

	cmd := exec.Command("diff", "-u", "--color=always", "tmp/before", "tmp/after")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			if exitError.ExitCode() > 1 {
				handleError(err)
			}
		} else {
			handleError(err)
		}
	}
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

func (x *ReleaseManager) uploadInternalFile(fileName string) {
	fmt.Printf("Uploading %s...\n", fileName)

	file, err := os.Open(fileName)
	if err != nil {
		handleError(fmt.Errorf("os.Open(`%s`) failed: %w", fileName, err))
	}
	defer func() { handleError(file.Close()) }()

	dest := fmt.Sprintf("%s/%s/%s", ArtifactoryUrl, ArtifactoryRepository, filepath.Base(fileName))
	x.uploadToArtifactory(file, dest)
}

func (x *ReleaseManager) uploadExternalFile(fileName string, repo string) {
	fmt.Printf("Uploading %s...\n", fileName)

	file, err := os.Open(fileName)
	if err != nil {
		handleError(fmt.Errorf("os.Open(`%s`) failed: %w", fileName, err))
	}
	defer func() { handleError(file.Close()) }()

	dest := fmt.Sprintf("%s/%s/startree-grafana-plugin/%s", ArtifactoryUrl, repo, filepath.Base(fileName))
	x.uploadToArtifactory(file, dest)
}

func (x *ReleaseManager) uploadToArtifactory(file io.Reader, dest string) {
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(file); err != nil {
		handleError(fmt.Errorf("file.Read failed: %w", err))
	}

	req := x.newArtifactoryRequest(http.MethodPut, dest, &buf)
	req.Header.Add("Content-Type", "application/zip")
	req.Header.Add("X-Checksum-Sha256", fmt.Sprintf("%x", sha256.Sum256(buf.Bytes())))
	req.Header.Add("X-Checksum-Sha1", fmt.Sprintf("%x", sha1.Sum(buf.Bytes())))
	req.Header.Add("X-Checksum-Md5", fmt.Sprintf("%x", md5.Sum(buf.Bytes())))

	resp, err := x.doRequest(req)
	if err != nil {
		handleError(fmt.Errorf("failed to upload file: %w", err))
	}
	defer func() { handleError(resp.Body.Close()) }()

	if resp.StatusCode != http.StatusCreated {
		handleError(fmt.Errorf("failed to upload file: %s", resp.Status))
	}

	fmt.Println("Upload complete.", dest)
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
	fmt.Printf("Checking file integrity... ")
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
		fmt.Println("Failed! 😵")
		handleError(errors.New("file integrity check failed"))
	}
	fmt.Println("Ok!")
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
	x.buildBackend()
	x.buildFrontend()
}

func (x *ReleaseManager) buildBackend() {
	fmt.Println("Building backend...")
	mage.Invoke(mage.Invocation{Verbose: true, Stdout: os.Stdout, Stderr: os.Stderr})
}

func (x *ReleaseManager) buildFrontend() {
	fmt.Println("Building frontend...")
	runCmd(nil, os.Stdout, "npm", "run", "build")
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

	var pluginsVolumeExists bool
	for _, vol := range spec["volumes"].([]interface{}) {
		if vol.(map[string]interface{})["name"].(string) == "plugins" {
			pluginsVolumeExists = true
			break
		}
	}
	if !pluginsVolumeExists {
		spec["volumes"] = append(spec["volumes"].([]interface{}), map[string]interface{}{
			"name":     "plugins",
			"emptyDir": map[string]interface{}{},
		})
	}

	var filesVolumeExists bool
	for _, vol := range spec["volumes"].([]interface{}) {
		if vol.(map[string]interface{})["name"].(string) == "files" {
			filesVolumeExists = true
			break
		}
	}
	if !filesVolumeExists {
		spec["volumes"] = append(spec["volumes"].([]interface{}), map[string]interface{}{
			"name":     "files",
			"emptyDir": map[string]interface{}{},
		})
	}

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
