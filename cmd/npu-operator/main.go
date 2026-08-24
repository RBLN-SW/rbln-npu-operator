/*
Copyright 2025.

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
	"os"

	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	// +kubebuilder:scaffold:imports
)

var setupLog = ctrl.Log.WithName("setup")

func initLogger(zapOpts *zap.Options) {
	logger := zap.New(zap.UseFlagOptions(zapOpts))
	ctrl.SetLogger(logger)
	// client-go logs via klog; route it into the same zap JSON stream.
	// ContextualLogger lets klog.Background()/FromContext callers hit the
	// zap sink directly instead of going through the klogr shim.
	klog.SetLoggerWithOptions(logger.WithName("klog"), klog.ContextualLogger(true))
}

func main() {
	opts := parseFlags()
	initLogger(&opts.ZapOpts)

	if err := run(opts); err != nil {
		setupLog.Error(err, "Operator failed")
		os.Exit(1)
	}
}

func run(opts startOptions) error {
	ctx := ctrl.SetupSignalHandler()

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		// HTTPS + authn/authz; scrapers need a SA token.
		Metrics: metricsserver.Options{
			BindAddress:    opts.MetricsAddr,
			SecureServing:  true,
			FilterProvider: filters.WithAuthenticationAndAuthorization,
		},
		WebhookServer:          webhook.NewServer(webhook.Options{}),
		HealthProbeBindAddress: opts.ProbeAddr,
		LeaderElection:         opts.EnableLeaderElection,
		LeaderElectionID:       leaderElectionID,
	})
	if err != nil {
		return err
	}

	if err := registerControllers(ctx, mgr); err != nil {
		return err
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}

	setupLog.Info("Starting manager")
	return mgr.Start(ctx)
}
