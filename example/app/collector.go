package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/YuriyNasretdinov/chukcha/client"
)

func CollectEventsThread(cl *client.Simple, clickHouseAddr string, stateFilePath string) {
	ctx := context.Background()

	st, err := ioutil.ReadFile(stateFilePath)
	if err == nil {
		cl.RestoreSavedState(st)
	} else if !errors.Is(err, os.ErrNotExist) {
		log.Printf("accessing chukcha client state file: %v", stateFilePath)
	}

	// ClickHouse likes as much of a chunk as you can
	// but Chukcha server defaults to 20 MiB for the chunk size,
	// so there is no reason to use bigger buffer here.
	scratch := make([]byte, 20*1024*1024)

	for {
		if err := doCollectEvents(ctx, scratch, cl, clickHouseAddr); err != nil && !errors.Is(err, io.EOF) {
			log.Printf("doCollectEvents: %v", err)
		}

		st, err := cl.MarshalState()
		if err != nil {
			panic(fmt.Errorf("Could not marshal Chukcha state: this should never happen: %v", err))
		}

		if err := ioutil.WriteFile(stateFilePath+".tmp", st, 0666); err != nil {
			log.Printf("writing chukcha client state: %v", err)
		} else if err := os.Rename(stateFilePath+".tmp", stateFilePath); err != nil {
			log.Printf("rename chukcha client state file: %v", err)
		}

		time.Sleep(time.Second * 2)
	}
}

func doCollectEvents(parentCtx context.Context, scratch []byte, cl *client.Simple, clickHouseAddr string) error {
	ctx, cancel := context.WithTimeout(parentCtx, time.Minute)
	defer cancel()

	return cl.Process(ctx, "events", scratch, func(b []byte) error {
		log.Printf("INSERTING %d bytes into ClickHouse", len(b))

		postURL := clickHouseAddr + "/?query=" + url.QueryEscape("INSERT INTO Events FORMAT JSONEachRow")
		req, err := http.NewRequest("POST", postURL, bytes.NewReader(b))
		if err != nil {
			return fmt.Errorf(`http.NewRequest("POST", %q): %v`, postURL, err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf(`sending request to ClickHouse %q: %v`, postURL, err)
		}

		defer resp.Body.Close()

		var errBuf bytes.Buffer
		io.Copy(&errBuf, resp.Body)

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf(`ClickHouse returned status %d: %s`, resp.StatusCode, errBuf.Bytes())
		}

		return nil
	})
}
