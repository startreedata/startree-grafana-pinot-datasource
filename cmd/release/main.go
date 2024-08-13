package main

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

const PluginName = "startree-pinot-datasource"

const ArtifactoryUrl = "https://repo.startreedata.io/artifactory"
const ArtifactoryRepository = "startree-grafana-plugin"

const PackageJsonFile = "package.json"
const BuildArtifactsDir = "dist"

var InternalUrls = []string{
	// Wildcard urls supported per https://github.com/grafana/grafana/issues/50652.
	"https://**.startree.cloud/",
	"https://**.startree-staging.cloud/",
	"https://**.startree-dev.cloud/",
}

func printUsage() {
	fmt.Printf("Usage: %s create|resign [ROOT_URLS...]\n", os.Args[0])
}

func main() {
	artifactoryToken := os.Getenv("ARTIFACTORY_TOKEN")

	if artifactoryToken == "" {
		handleError(fmt.Errorf("environment variable `ARTIFACTORY_TOKEN` is required"))
	} else if os.Getenv("GRAFANA_ACCESS_POLICY_TOKEN") == "" {
		handleError(fmt.Errorf("environment variable `GRAFANA_ACCESS_POLICY_TOKEN` is required"))
	}

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	releaseManager := ReleaseManager{ArtifactoryToken: artifactoryToken}

	action := os.Args[1]
	switch action {
	case "create":
		releaseManager.CreateInternalRelease()

	case "resign":
		releaseName := os.Getenv("RELEASE_NAME")
		if releaseName == "" {
			handleError(fmt.Errorf("environment variable `RELEASE_NAME` is required"))
		}

		var rootUrls []string
		if len(os.Args) > 2 {
			rootUrls = os.Args[2:]
		} else {
			rootUrls = InternalUrls
		}

		releaseManager.ResignRelease(releaseName, rootUrls)
	default:
		printUsage()
		os.Exit(1)
	}
}

type ReleaseManager struct {
	ArtifactoryToken string
}

func (r *ReleaseManager) CreateInternalRelease() {
	version, err := getPackageVersion()
	handleError(err)

	releaseArchive := fmt.Sprintf("%s-%s.zip", PluginName, version)
	fmt.Println("Preparing release", releaseArchive)
	fmt.Println("Valid for urls:", strings.Join(InternalUrls, " "))

	handleError(r.buildPlugin())
	handleError(r.signPlugin(InternalUrls))
	handleError(r.zipPlugin(releaseArchive))

	handleError(r.uploadArchive(releaseArchive))
}

func (r *ReleaseManager) ResignRelease(releaseName string, rootUrls []string) {
	handleError(validateReleaseName(releaseName))

	rootUrls, err := cleanUrls(rootUrls)
	handleError(err)

	if len(rootUrls) == 0 {
		fmt.Printf("No signing urls provided; nothing to do.")
		os.Exit(1)
	}

	version, err := getLatestReleaseVersion()
	handleError(err)

	internalRelease := fmt.Sprintf("%s-%s.zip", PluginName, version)
	customerRelease := fmt.Sprintf("%s-%s-%s.zip", PluginName, version, releaseName)

	fmt.Println("Preparing release", customerRelease)
	fmt.Println("Valid for urls:", strings.Join(rootUrls, " "))

	handleError(r.downloadArchive(internalRelease))
	handleError(r.unzipPlugin(internalRelease))
	handleError(r.signPlugin(rootUrls))
	handleError(r.zipPlugin(customerRelease))
	handleError(r.uploadArchive(customerRelease))
}

func (r *ReleaseManager) buildPlugin() error {
	fmt.Println("Building plugin...")

	if err := os.RemoveAll(BuildArtifactsDir); err != nil {
		return fmt.Errorf("os.RemoveAll(`%s`) failed: %w", BuildArtifactsDir, err)
	}
	if err := execute("mage", "-v"); err != nil {
		return err
	}
	return execute("npm", "run", "build")
}

func (r *ReleaseManager) downloadArchive(archive string) error {
	fmt.Println("Downloading plugin archive...")

	src := fmt.Sprintf("%s/%s/%s", ArtifactoryUrl, ArtifactoryRepository, archive)
	req, err := r.newArtifactoryRequest(http.MethodGet, src, nil)
	if err != nil {
		return err
	}

	fmt.Println("HTTP GET", req.URL)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download plugin archive: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download plugin archive: %s", resp.Status)
	}

	file, err := os.OpenFile(archive, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("os.OpenFile(`%s`) failed: %w", archive, err)
	}

	if _, err = io.Copy(file, resp.Body); err != nil {
		return fmt.Errorf("io.Copy failed: %w", err)
	}

	fmt.Println("Plugin archive downloaded to", archive)
	return nil
}

func (r *ReleaseManager) uploadArchive(archive string) error {
	fmt.Println("Uploading plugin archive...")

	file, err := os.Open(archive)
	if err != nil {
		return fmt.Errorf("os.Open(`%s`) failed: %w", archive, err)
	}

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(file); err != nil {
		return fmt.Errorf("file.Read failed: %w", err)
	}
	sha256sum := sha256.Sum256(buf.Bytes())
	sha1sum := sha1.Sum(buf.Bytes())
	md5sum := md5.Sum(buf.Bytes())

	dest := fmt.Sprintf("%s/%s/%s", ArtifactoryUrl, ArtifactoryRepository, archive)
	req, err := r.newArtifactoryRequest(http.MethodPut, dest, &buf)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/zip")
	req.Header.Add("X-Checksum-Sha256", fmt.Sprintf("%x", sha256sum))
	req.Header.Add("X-Checksum-Sha1", fmt.Sprintf("%x", sha1sum))
	req.Header.Add("X-Checksum-Md5", fmt.Sprintf("%x", md5sum))

	fmt.Println("HTTP PUT", req.URL)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload plugin archive: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to upload plugin archive: %s", resp.Status)
	}

	if err := os.Remove(archive); err != nil {
		return fmt.Errorf("os.Remove(`%s`) failed: %w", archive, err)
	}

	fmt.Println("Plugin archive uploaded to", dest)
	viewUrl := fmt.Sprintf("https://repo.startreedata.io/ui/repos/tree/General/%s%%2F%s", ArtifactoryRepository, archive)
	fmt.Println("View in JFrog at", viewUrl)
	return nil
}

func (r *ReleaseManager) newArtifactoryRequest(method string, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("X-JFrog-Art-Api", r.ArtifactoryToken)
	return req, nil
}

func (r *ReleaseManager) unzipPlugin(src string) error {
	if err := os.RemoveAll(PluginName); err != nil {
		return fmt.Errorf("os.RemoveAll(`%s`) failed: %w", PluginName, err)
	}
	if err := execute("unzip", src); err != nil {
		return err
	}
	if err := os.RemoveAll(BuildArtifactsDir); err != nil {
		return fmt.Errorf("os.RemoveAll(`%s`) failed: %w", BuildArtifactsDir, err)
	}
	if err := os.Rename(PluginName, BuildArtifactsDir); err != nil {
		return fmt.Errorf("os.Rename(`%s`, `%s`) failed: %w", PluginName, BuildArtifactsDir, err)
	}
	return nil
}

func (r *ReleaseManager) signPlugin(rootUrls []string) error {
	fmt.Println("Signing plugin...")
	return execute("npx", "@grafana/sign-plugin@latest", "--rootUrls", strings.Join(rootUrls, ","))
}

func (r *ReleaseManager) zipPlugin(dest string) error {
	if err := os.RemoveAll(PluginName); err != nil {
		return fmt.Errorf("os.RemoveAll(`%s`) failed: %w", PluginName, err)
	}
	if err := os.RemoveAll(dest); err != nil {
		return fmt.Errorf("os.RemoveAll(`%s`) failed: %w", dest, err)
	}
	if err := os.Rename(BuildArtifactsDir, PluginName); err != nil {
		return fmt.Errorf("os.Rename(`%s`, `%s`) failed: %w", BuildArtifactsDir, PluginName, err)
	}
	if err := execute("zip", "-r", dest, PluginName); err != nil {
		return err
	}
	if err := os.Rename(PluginName, BuildArtifactsDir); err != nil {
		return fmt.Errorf("os.Rename(`%s`, `%s`) failed: %w", BuildArtifactsDir, PluginName, err)
	}
	return nil
}

func cleanUrls(args []string) ([]string, error) {
	if len(args) == 0 {
		return InternalUrls, nil
	}

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
			return nil, fmt.Errorf("invalid URL: `%s`", u)
		}
	}

	return rootUrls, nil
}

func validateReleaseName(releaseName string) error {
	if ok, _ := regexp.Match(`^[\w-]+$`, []byte(releaseName)); !ok {
		return fmt.Errorf("invalid release name `%s`: release name can only contain alphanumeric characters", releaseName)
	}
	return nil
}

func getPackageVersion() (string, error) {
	file, err := os.Open(PackageJsonFile)
	if err != nil {
		return "", fmt.Errorf("os.Open(`%s`) failed: %w", PackageJsonFile, err)
	}
	defer file.Close()

	var data map[string]interface{}
	if err = json.NewDecoder(file).Decode(&data); err != nil {
		return "", fmt.Errorf("json.Decode failed: %w", err)
	}
	return data["version"].(string), nil
}

func getLatestReleaseVersion() (string, error) {
	cmd := exec.Command("git", "tag", "--list", "--sort=-v:refname")

	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = os.Stderr
	fmt.Println("Executing:", cmd.String())
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("command failed: %w", err)
	}
	version := strings.SplitN(buf.String(), "\n", 2)[0]
	if version == "" {
		return "", fmt.Errorf("no version found in git tags")
	}
	version = strings.TrimPrefix(version, "v")
	return version, nil
}

func execute(command string, args ...string) error {
	cmd := exec.Command(command, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	fmt.Println("Executing:", cmd.String())
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("command failed: %w", err)
	}
	return nil
}

func handleError(err error) {
	if err != nil {
		fmt.Printf("Error: %s.\n", err)
		os.Exit(1)
	}
}
