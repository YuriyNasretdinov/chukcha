package integration

import (
	"os"
	"os/exec"
	"testing"
)

func TestEtcdExists(t *testing.T) {
	etcdPath, err := exec.LookPath("etcd")
	if err != nil {
		t.Fatalf("Etcd is not found in PATH")
	}

	cmd := exec.Command(etcdPath, "--help")
	cmd.Env = append(cmd.Env, os.Environ()...)
	cmd.Env = append(cmd.Env, "ETCD_UNSUPPORTED_ARCH=arm64")

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Errorf("running `etcd --help` failed (%v): %s", err, out)
	}
}
