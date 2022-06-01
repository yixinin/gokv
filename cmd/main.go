// Copyright 2018 The tiglabs raft Authors.
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
// limitations under the License.package wal

package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"

	"github.com/sirupsen/logrus"
	raftlog "github.com/tiglabs/raft/logger"
	"github.com/yixinin/gokv"
	"github.com/yixinin/gokv/logger"
)

var nodeID = flag.Uint64("node", 1, "current node id")
var confFile = flag.String("conf", "conf/kvs.toml", "config file path")
var debug = flag.Bool("debug", false, "debug log")

func main() {
	flag.Parse()

	// load config
	cfg := gokv.LoadConfig(*confFile, *nodeID)

	if strings.ToLower(cfg.ServerCfg.LogLevel) == "debug" || *debug {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	if cfg.ServerCfg.LogPath != "" {
		//todo
	}

	for _, hook := range logger.Hooks() {
		logger.AddHook(hook)
	}

	raftlog.SetLogger(logger.NewRaftLogger())

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	// start kv
	kv := gokv.NewRaftKv(*nodeID, cfg)
	go kv.Run(ctx)
	server := gokv.NewServer(kv)
	go server.Run(ctx, cfg.FindClusterNode(*nodeID).HTTPPort)

	<-ch
	cancel()
	kv.Stop(ctx)
}
