// Run it by executing the following in the chuckha root directory:
// $ go run example/server/start.go

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

type Host struct {
	HostName     string
	InstanceName string
	ListenAddr   string
	Arch         string
	DirName      string
}

var hosts = []Host{
	{
		HostName:     "localhost",
		InstanceName: "Moscow",
		ListenAddr:   "127.0.0.1:8080",
		Arch:         "amd64",
		DirName:      os.ExpandEnv("$HOME/chukcha-data/moscow"),
	},
	{
		HostName:     "localhost",
		InstanceName: "Voronezh",
		ListenAddr:   "127.0.0.1:8081",
		Arch:         "amd64",
		DirName:      os.ExpandEnv("$HOME/chukcha-data/voronezh/"),
	},
	{
		HostName:     "z",
		InstanceName: "Peking",
		ListenAddr:   "127.0.0.1:8082",
		Arch:         "amd64",
	},
	{
		HostName:     "g",
		InstanceName: "Bengaluru",
		ListenAddr:   "127.0.0.1:8083",
		Arch:         "amd64",
	},
	{
		HostName:     "a",
		InstanceName: "Phaenus",
		ListenAddr:   "127.0.0.1:8084",
		Arch:         "arm",
	},
}

func runInParallel(cb func(h Host) error) error {
	errCh := make(chan error, len(hosts))

	for _, h := range hosts {
		go func(h Host) {
			if err := cb(h); err != nil {
				log.Printf("%s: %v", h.InstanceName, err)
				errCh <- fmt.Errorf("%s: %w", h.InstanceName, err)
			} else {
				errCh <- nil
			}
		}(h)
	}

	var errorsList []string
	for i := 0; i < len(hosts); i++ {
		err := <-errCh
		if err != nil {
			errorsList = append(errorsList, err.Error())
		}
	}

	if len(errorsList) > 0 {
		return fmt.Errorf("%s", strings.Join(errorsList, "; "))
	}
	return nil
}

func SliceMap[T any, K comparable](s []T, cb func(T) K) []K {
	res := make([]K, len(s))
	for i, v := range s {
		res[i] = cb(v)
	}
	return res
}

func SliceUnique[T comparable](s []T) []T {
	m := make(map[T]struct{})
	var res []T

	for _, v := range s {
		if _, ok := m[v]; ok {
			continue
		}

		res = append(res, v)
		m[v] = struct{}{}
	}

	return res
}

func main() {
	var aliveHostsChan = make(chan Host, len(hosts))

	err := runInParallel(func(h Host) error {
		cmd := exec.Command("ssh", "-oBatchMode=yes", h.HostName, "killall", "chukcha", "chukcha-amd64", "||", "true")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err == nil {
			aliveHostsChan <- h
		}
		return err
	})
	close(aliveHostsChan)

	hosts = nil
	for h := range aliveHostsChan {
		hosts = append(hosts, h)
	}

	if err != nil {
		log.Printf("Errors during Hello world: %v", err)
	}

	for _, arch := range SliceUnique(SliceMap(hosts, func(t Host) string { return t.Arch })) {
		cmd := exec.Command("env", fmt.Sprintf("GOARCH=%s", arch), "go", "build", "-v", "-o", "/tmp/chukcha-"+arch, ".")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		log.Printf("Running %s", cmd)

		if err := cmd.Run(); err != nil {
			log.Fatalf("Failed to build Chukcha: %v", err)
		}
	}

	err = runInParallel(func(h Host) error {
		if h.HostName == "localhost" {
			return nil
		}
		cmd := exec.Command("scp", "-oBatchMode=yes", "/tmp/chukcha-"+h.Arch, h.HostName+":chukcha")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		log.Printf("Running %s", cmd)
		return cmd.Run()
	})

	if err != nil {
		log.Fatalf("Failed to copy Chukcha: %v", err)
	}

	var peers []string
	for _, h := range hosts {
		peers = append(peers, fmt.Sprintf("%s=%s", h.InstanceName, h.ListenAddr))
	}

	commonParams := []string{"-cluster=AllComrads", "-rotate-chunk-interval=10s", "-peers=" + strings.Join(peers, ",")}

	err = runInParallel(func(h Host) error {
		binaryLocation := "./chukcha"
		if h.HostName == "localhost" {
			binaryLocation = "/tmp/chukcha-" + h.Arch
		}
		dirname := "./chukcha-data"
		if h.DirName != "" {
			dirname = h.DirName
		}

		args := []string{"-oBatchMode=yes", h.HostName, binaryLocation}
		args = append(args, commonParams...)
		args = append(args, "-dirname="+dirname, "-instance="+h.InstanceName, "-listen="+h.ListenAddr)

		cmd := exec.Command("ssh", args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		log.Printf("Running %s", cmd)
		return cmd.Run()
	})

	if err != nil {
		log.Fatalf("Failed to copy Chukcha: %v", err)
	}
}
