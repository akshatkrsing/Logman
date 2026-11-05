
package main

import (
	"log"

	"github.com/akshatkrsing/logman/internal/api"
)

func main() {
	log.Println("Starting Logman...")

	if err := api.StartGRPCServer("50051"); err != nil {
		log.Fatalf("Failed to start Logman: %v", err)
	}
}
