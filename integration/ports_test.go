//go:build integration

package integration

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"
)

// TestIntegrationPortsEnforced guards the dedicated integration stack host
// ports. The suite must only use source 5435, destination 5436, and Kafka
// 9094 on the host, so it can never collide with the production fixture stack
// (5433/5434/9092) or anything else running on the machine.
//
// Every file under integration/ is scanned recursively. The patterns cover
// host connections written as "localhost:P", "127.0.0.1:P", "[::1]:P", and
// docker-compose host bindings ("HOST:CONTAINER"). PostgreSQL/Kafka
// container-internal ports used only inside the Compose network (for example
// "5435:5432" or "zookeeper:2181") are not host ports and are not flagged.
func TestIntegrationPortsEnforced(t *testing.T) {
	allowed := map[string]bool{"5435": true, "5436": true, "9094": true}

	hostPort := []*regexp.Regexp{
		regexp.MustCompile(`localhost:([0-9]{2,5})`),
		regexp.MustCompile(`127\.0\.0\.1:([0-9]{2,5})`),
		regexp.MustCompile(`\[::1\]:([0-9]{2,5})`),
	}
	// Accept quoted or unquoted Compose short syntax, including an optional
	// loopback bind address. Capture group 1 is always the host port.
	composePort := regexp.MustCompile(`(?m)^\s*-\s*["']?(?:(?:127\.0\.0\.1|\[::1\]):)?([0-9]{2,5}):[0-9]{2,5}(?:/(?:tcp|udp))?["']?\s*(?:#.*)?$`)

	offending := map[string]string{} // "file:port" -> description
	files := 0
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		files++
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		content := string(data)
		for _, re := range hostPort {
			for _, m := range re.FindAllStringSubmatch(content, -1) {
				port := m[1]
				if !allowed[port] {
					offending[path+":"+port] = fmt.Sprintf("%s connects to unapproved host port %s (allowed: 5435, 5436, 9094)", path, port)
				}
			}
		}
		for _, m := range composePort.FindAllStringSubmatch(content, -1) {
			port := m[1]
			if !allowed[port] {
				offending[path+":"+port] = fmt.Sprintf("%s binds unapproved host port %s (allowed: 5435, 5436, 9094)", path, port)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk integration files: %v", err)
	}
	if files == 0 {
		t.Fatal("no files discovered under integration/")
	}

	// The compose file must actually bind the three host ports the suite uses.
	bound := map[string]bool{}
	for _, m := range composePort.FindAllStringSubmatch(readFile(t, "docker-compose.yml"), -1) {
		bound[m[1]] = true
	}
	for _, port := range []string{"5435", "5436", "9094"} {
		if !bound[port] {
			t.Errorf("docker-compose.yml does not bind expected host port %s", port)
		}
	}

	if len(offending) > 0 {
		keys := make([]string, 0, len(offending))
		for k := range offending {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			t.Error(offending[k])
		}
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}
