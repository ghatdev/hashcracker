package main

import (
	"log"

	"github.com/ghatdev/hashcracker/cracker"
	"github.com/ghatdev/hashcracker/worker"
	"github.com/spf13/cobra"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "hc [crack|work]",
		Short: "hash cracker using kafka and workers",
	}

	var (
		salt   string
		host   []string
		length int
		topic  string
	)

	crackCmd := &cobra.Command{
		Use:   "crack [hash]",
		Short: "Run brute-force cracking",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cracker.RunCrack(args[0], salt, length, host)
		},
	}

	crackCmd.Flags().StringVarP(&salt, "salt", "s", "", "salt (if exists)")
	crackCmd.Flags().IntVarP(&length, "length", "l", 5, "length")
	crackCmd.Flags().StringVarP(&topic, "topic", "t", "Hashs", "Topic for join")

	workCmd := &cobra.Command{
		Use:   "work",
		Short: "participate as worker",
		Run: func(cmd *cobra.Command, args []string) {
			err := worker.Work(host, topic)
			if err != nil {
				log.Println(err)
			}
		},
	}

	workCmd.Flags().StringVarP(&topic, "topic", "t", "Hashs", "Topic for join")

	rootCmd.AddCommand(crackCmd, workCmd)

	rootCmd.Flags().StringSliceVarP(&host, "host", "b", []string{"localhost:6237"}, "kafka brocker cluster address")
	rootCmd.Execute()
}
