package e2e

import (
	"context"
	"fmt"
	"time"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
)

type HelmClient struct {
	settings *cli.EnvSettings
	config   *action.Configuration
	chart    string
}

func NewHelmClient(namespace, kubeconfig, chart string) (*HelmClient, error) {
	settings := cli.New()
	settings.SetNamespace(namespace)
	settings.KubeConfig = kubeconfig

	cfg := new(action.Configuration)
	if err := cfg.Init(
		settings.RESTClientGetter(),
		namespace,
		"secret",
		func(format string, v ...interface{}) {}); err != nil {
		return nil, fmt.Errorf("init helm configuration: %w", err)
	}

	return &HelmClient{
		settings: settings,
		config:   cfg,
		chart:    chart,
	}, nil
}

type ChartOptions struct {
	CleanupOnFail bool
	ReleaseName   string
	Timeout       time.Duration
	Wait          bool
	Values        map[string]interface{}
}

func (c *HelmClient) Install(ctx context.Context, opts ChartOptions) (string, error) {
	install := action.NewInstall(c.config)
	install.Namespace = c.settings.Namespace()
	install.CreateNamespace = false
	install.Atomic = opts.CleanupOnFail
	install.ReleaseName = opts.ReleaseName
	install.Wait = opts.Wait
	install.Timeout = opts.Timeout

	chart, err := loader.Load(c.chart)
	if err != nil {
		return "", fmt.Errorf("load chart: %w", err)
	}

	rel, err := install.RunWithContext(ctx, chart, opts.Values)
	if err != nil {
		return "", fmt.Errorf("install chart: %w", err)
	}

	return rel.Name, nil
}

func (c *HelmClient) Uninstall(ctx context.Context, releaseName string) error {
	uninstall := action.NewUninstall(c.config)
	_, err := uninstall.Run(releaseName)
	return err
}
