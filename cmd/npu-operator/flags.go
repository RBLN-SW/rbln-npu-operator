/*
Copyright 2026 Rebellions Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"

	"go.uber.org/zap/zapcore"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

type startOptions struct {
	MetricsAddr          string
	ProbeAddr            string
	EnableLeaderElection bool
	ZapOpts              zap.Options
}

func parseFlags() startOptions {
	opts := startOptions{
		MetricsAddr:          "0",
		ProbeAddr:            ":8081",
		EnableLeaderElection: false,
		ZapOpts: zap.Options{
			StacktraceLevel: zapcore.PanicLevel,
		},
	}

	flag.StringVar(&opts.MetricsAddr, "metrics-bind-address", opts.MetricsAddr,
		"Metrics bind address, e.g. :8443. Served over HTTPS with authn/authz. Leave as 0 to disable.")
	flag.StringVar(&opts.ProbeAddr, "health-probe-bind-address", opts.ProbeAddr,
		"The address the probe endpoint binds to.")
	flag.BoolVar(&opts.EnableLeaderElection, "leader-elect", opts.EnableLeaderElection,
		"Enable leader election so only one controller manager is active.")

	// client-go logs through klog; registering its flags exposes -v/-vmodule
	// so redirected klog records (see initLogger) can be raised past V(0).
	// Surfacing them still requires --zap-log-level >= the same verbosity.
	klog.InitFlags(flag.CommandLine)
	opts.ZapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	return opts
}
