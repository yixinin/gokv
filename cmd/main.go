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
	"flag"

	"github.com/yixinin/gokv"
)

var nodeID = flag.Uint64("node", 1, "current node id")
var confFile = flag.String("conf", "conf/kvs.toml", "config file path")

func main() {
	flag.Parse()

	// load config
	cfg := gokv.LoadConfig(*confFile, *nodeID)

	// init log
	// log.InitFileLog(cfg.ServerCfg.LogPath, fmt.Sprintf("node%d", *nodeID), cfg.ServerCfg.LogLevel)

	// start server
	server := gokv.NewServer(*nodeID, cfg)
	server.Run()
}
