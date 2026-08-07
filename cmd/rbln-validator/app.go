package main

import "github.com/spf13/cobra"

func NewRBLNValidatorApp() *cobra.Command {
	config := newRootConfig()

	cmd := &cobra.Command{
		Use:           "rbln-validator",
		Short:         "RBLN NPU operator validator.",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(
		newDriverCommand(config),
		newGateCommand(config),
		newToolkitCommand(config),
		newVFIOPCICommand(config),
	)

	config.bindFlags(cmd)

	return cmd
}
