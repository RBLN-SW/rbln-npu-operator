package driver

import "log/slog"

type Config struct {
	OutputDir            string
	SleepIntervalSeconds int
}

type Result struct {
	IsHostDriver         bool
	DriverRoot           string
	DriverRootCtrPath    string
	ContainerLibraryPath string
}

var (
	validateHostDriverFn      = validateHostDriver
	validateDriverContainerFn = validateDriverContainer
)

func Validate(cfg Config) (Result, error) {
	return validate(cfg, false)
}

func validate(cfg Config, silent bool) (Result, error) {
	if err := validateHostDriverFn(); err == nil {
		slog.Info("Detected a pre-installed driver on the host")
		return Result{
			IsHostDriver:         true,
			DriverRoot:           "/",
			DriverRootCtrPath:    "/host",
			ContainerLibraryPath: driverContainerLibraryPath,
		}, nil
	}

	if err := validateDriverContainerFn(cfg, silent); err != nil {
		return Result{}, err
	}

	return Result{
		IsHostDriver:         false,
		DriverRoot:           driverInstallDirDefault,
		DriverRootCtrPath:    "/host",
		ContainerLibraryPath: driverContainerLibraryPath,
	}, nil
}
