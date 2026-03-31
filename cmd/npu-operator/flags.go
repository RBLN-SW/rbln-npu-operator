package main

import (
	"flag"

	"go.uber.org/zap/zapcore"
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
		"Metrics bind address. Use :8443 for HTTPS, or :8080 for HTTP, or leave as 0 to disable.")
	flag.StringVar(&opts.ProbeAddr, "health-probe-bind-address", opts.ProbeAddr,
		"The address the probe endpoint binds to.")
	flag.BoolVar(&opts.EnableLeaderElection, "leader-elect", opts.EnableLeaderElection,
		"Enable leader election so only one controller manager is active.")

	opts.ZapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	return opts
}
