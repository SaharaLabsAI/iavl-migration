package main

import (
	"fmt"
	"os"

	// v0 "github.com/cosmos/iavl/v2/migrate/v0"
	v2 "github.com/SaharaLabsAI/iavl-migration/v2"
	"github.com/spf13/cobra"
)

func main() {
	root := cobra.Command{
		Use:   "migrate",
		Short: "migrate application.db to IAVL v2",
	}
	root.AddCommand(v2.Command())

	if err := root.Execute(); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		os.Exit(1)
	}
}
