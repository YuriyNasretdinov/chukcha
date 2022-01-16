package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/websocket"
)

var timeout = flag.Duration("timeout", time.Second*20, "timeout for tests")
var clientID = flag.String("client-id", "", "YouTube OAuth Client ID")
var clientSecretFile = flag.String("client-secret", "", "YouTube OAuth Client Secret File Path")
var testYoutubeFlag = flag.Bool("testyoutube", false, "Test youtube")

func main() {
	flag.Parse()

	if *testYoutubeFlag {
		testYoutube()
		return
	}

	http.HandleFunc("/", indexHandler)
	http.Handle("/events", websocket.Handler(eventsHandler))

	log.Printf("Running")
	log.Fatal(http.ListenAndServe(":80", nil))
}

func eventsHandler(ws *websocket.Conn) {
	eofCh := make(chan bool, 1)
	go func() {
		ws.Read(make([]byte, 100))
		eofCh <- true
	}()

	enc := json.NewEncoder(ws)

	var lastSize int64

	for {
		select {
		case <-eofCh:
			return
		default:
		}

		time.Sleep(time.Millisecond * 100)

		st, err := os.Stat("/tmp/saved")
		if err != nil {
			log.Printf("saved file: %v", err)
			time.Sleep(time.Second * 5)
			continue
		}

		if st.Size() == lastSize {
			continue
		}

		enc.Encode(map[string]interface{}{
			"running": true,
		})

		lastSize = st.Size()
		err = runTest()
		log.Printf("runTest() result: %v", err)

		if err != nil {
			enc.Encode(map[string]interface{}{
				"testsError": err.Error(),
			})
		} else {
			enc.Encode(map[string]interface{}{
				"testsSuccess": true,
			})
		}

	}

}

func indexHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", `text/html; charset="UTF-8"`)

	io.WriteString(w, `
	<html>
	<head><title>Yuri's stream</title></head>
	<body>
	<div style="position: absolute; right: 0; top: 0; font-size: 40px; padding: 5px; color: white; font-family: 'Helvetica'; text-shadow: 2px 2px 4px #000000; background-color:rgba(0, 0, 0, 0.5);">
	github.com/YuriyNasretdinov/chukcha
	</div>

	<div id="tests" style="position: absolute; bottom: 0; right: 0; font-size: 40px; font-family: 'Helvetica'; text-shadow: 2px 2px 4px #000000; background-color: black; padding: 5px;">
	
	</div>

	<script>
	function setWebsocketConnection() {
		var websocket = new WebSocket("ws" + (window.location.protocol.indexOf("https") >= 0 ? "s" : "") + "://" + window.location.host + "/events")
		websocket.onopen = function(evt) {
			console.log("open")
		}
		websocket.onclose = function(evt) {
			console.log("close")
			setTimeout(setWebsocketConnection, 1000)
		}
		websocket.onmessage = onMessage
		websocket.onerror = function(evt) { console.log("Error: " + evt) }
	}

	function onMessage(evt) {
		var reply = JSON.parse(evt.data)
		var el = document.getElementById("tests")
		if (reply.running) {
			el.innerHTML = 'running tests'
			el.style.color = 'white'
		} else if (reply.testsSuccess) {
			el.innerHTML = 'tests passing'
			el.style.color = 'green'
		} else if (reply.testsError) {
			el.innerHTML = reply.testsError
			el.style.color = 'red'
		}
	}

	setWebsocketConnection();
	</script>
	</body>
	</html>
	`)
}

func runTest() error {
	tmpdir, err := os.MkdirTemp(os.TempDir(), "chukchatest")
	if err != nil {
		return fmt.Errorf("creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpdir)

	log.Printf("Created temp dir %s", tmpdir)

	env := append([]string{}, os.Environ()...)
	for idx, e := range env {
		if strings.HasPrefix(e, "TMPDIR=") {
			env[idx] = "TMPDIR=" + tmpdir
		}
	}

	cmd := exec.Command("go", "test", "-v", "./...")
	cmd.Dir = os.ExpandEnv("$HOME/chukcha")
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting go test: %v", err)
	}

	// Kill all rogue processes left if any.
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		return fmt.Errorf("getting process group id: %v", err)
	}
	defer syscall.Kill(-pgid, syscall.SIGKILL)

	log.Printf("Process group id: %v", pgid)

	errCh := make(chan error, 1)
	go func() { errCh <- cmd.Wait() }()

	select {
	case err := <-errCh:
		log.Printf("tests result: %v", err)
		if err != nil {
			return fmt.Errorf("error running tests: %v", err)
		}
	case <-time.After(*timeout):
		return errors.New("tests timed out")
	}

	log.Printf("No errors executing tests")

	return nil
}
