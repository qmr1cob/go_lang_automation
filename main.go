package main

import (
	"fmt"
	"net/http"
	"os/exec"
)

func main() {
	// Define a handler function to handle incoming HTTP requests
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Write the response message to the client
		fmt.Fprintf(w, "Welcome to the backend!\n")

		// Run the Python script
		output, err := runPythonScript()
		if err != nil {
			fmt.Printf("Error running Python script: %s\n", err)
		} else {
			// Print the Python script output to the client
			fmt.Fprintf(w, "Python script output: %s\n", output)
		}
	})

	// Start the HTTP server on port 8080
	fmt.Println("Starting server on port 8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		// Print any errors that occur while starting the server
		fmt.Printf("Error starting server: %s\n", err)
	} else {
		fmt.Println("Server started successfully!")
	}
}

func runPythonScript() (string, error) {
	// Command to run the Python script
	cmd := exec.Command("python", "script.py")
	//scriptCmd := exec.Command("python3", "xorder_logMessage.py", triggerName, body.OrderID)

	// Run the command and capture its output
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("error running Python script: %s, output: %s", err, output)
	}

	// Convert the output bytes to a string
	outputStr := string(output)

	// Print the output
	fmt.Println("Python script output:", outputStr)

	// Return the output as a string
	return outputStr, nil
}
