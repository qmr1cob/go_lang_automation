package main

import (
	"fmt"
	"net/http"
)

func main() {
	// Define a handler function to handle incoming HTTP requests
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Write the response message to the client
		fmt.Fprintf(w, "Welcome to the backend!")
	})

	// Start the HTTP server on port 8080
	fmt.Println("Starting server on port 8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		// Print any errors that occur while starting the server
		fmt.Printf("Error starting server: %s\n", err)
	}
}
