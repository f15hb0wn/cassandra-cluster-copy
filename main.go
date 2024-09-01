package main

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/joho/godotenv"
)

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func loadTablesToSkip(filename string) ([]string, error) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return []string{}, nil
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(data), "\n"), nil
}

func saveTableToSkip(filename, table string) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(table + "\n"); err != nil {
		return err
	}
	return nil
}

func copyData(session *gocql.Session, l_session *gocql.Session, keyspace string, table string) {
	// Create a query to select all data from the table with a page size of 10
	query := fmt.Sprintf("SELECT * FROM %s.%s", keyspace, table)

	// Load the page state from disk if it exists
	pageStateFile := fmt.Sprintf("%s_%s_page_state.txt", keyspace, table)
	var pageState []byte
	if _, err := os.Stat(pageStateFile); err == nil {
		data, err := os.ReadFile(pageStateFile)
		if err != nil {
			log.Fatalf("Failed to read page state file: %v", err)
		}
		pageState, err = base64.StdEncoding.DecodeString(string(data))
		if err != nil {
			log.Fatalf("Failed to decode page state: %v", err)
		}
	}

	iter := session.Query(query).PageSize(10).PageState(pageState).Consistency(gocql.One).Iter()

	// Iterate over the rows in the result set
	for {
		// Initialize the row map
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			// Check if there are more pages to fetch
			if iter.NumRows() == 0 {
				break
			}
			// Save the current page state to disk
			pageState = iter.PageState()
			data := base64.StdEncoding.EncodeToString(pageState)
			if err := os.WriteFile(pageStateFile, []byte(data), 0644); err != nil {
				log.Fatalf("Failed to write page state to file: %v", err)
			}
			// Fetch the next page
			iter = session.Query(query).PageState(pageState).PageSize(10).Consistency(gocql.One).Iter()
			continue
		}
		// if the row is empty, break the loop
		if len(row) == 0 {
			break
		}
		// Create an insert query that is specific to the table schema
		insert := fmt.Sprintf("INSERT INTO %s.%s (", keyspace, table)
		values := "VALUES ("
		var args []interface{}

		for k, v := range row {
			insert += k + ", "
			values += "?, "
			args = append(args, v)
		}
		insert = insert[:len(insert)-2] + ") "
		values = values[:len(values)-2] + ")"
		query := insert + values

		// Execute the insert query with retry logic
		for attempt := 1; attempt <= 3; attempt++ {
			if err := l_session.Query(query, args...).Exec(); err != nil {
				// Print the insert query and error
				fmt.Printf("Error executing query (attempt %d): %v\nQuery: %s\n", attempt, err, query)
				if attempt < 3 {
					// Sleep for 5 seconds before retrying
					time.Sleep(5 * time.Second)
				} else {
					// If it's the last attempt, log the error and continue
					fmt.Printf("Failed to execute query after %d attempts\n", attempt)
				}
			} else {
				// If the query succeeds, break out of the retry loop
				break
			}
		}
	}

	// Remove the page state file after successful completion
	if err := os.Remove(pageStateFile); err != nil {
		fmt.Printf("Failed to remove page state file: %v\n", err)
	}

	// Save the completed table to the skip list
	if err := saveTableToSkip("tables_to_skip.txt", table); err != nil {
		log.Fatalf("Failed to save table to skip list: %v", err)
	}
}

func main() {
	// If .env file exists, load the environment variables from it
	if _, err := os.Stat(".env"); err == nil {
		if err := godotenv.Load(); err != nil {
			log.Fatalf("Failed to load environment variables from .env file: %v", err)
		}
	}
	// Initialize the source and destination sessions
	session := connectToSourceCluster()
	l_session := connectToDestinationCluster()

	// Prompt the user for keyspaces to copy
	fmt.Print("Enter the keyspaces to copy (comma-separated): ")
	var keyspace_string string
	fmt.Scanln(&keyspace_string)
	keyspaces := strings.Split(keyspace_string, ",")

	tablesToSkip, err := loadTablesToSkip("tables_to_skip.txt")
	if err != nil {
		log.Fatalf("Failed to load tables to skip: %v", err)
	}

	// Create a WaitGroup to manage concurrency
	var wg sync.WaitGroup

	// Create a buffered channel to limit the number of concurrent goroutines to 10
	sem := make(chan struct{}, 10)

	// Iterate over the keyspaces getting a list of tables then copying the data from the source cluster to the destination cluster
	for _, keyspace := range keyspaces {
		fmt.Println("Copying data from the source cluster to the destination cluster for keyspace:", keyspace)
		tables := getTables(session, keyspace)
		for _, table := range tables {
			// Skip tables in the tablesToSkip list
			if contains(tablesToSkip, table) {
				fmt.Println("Skipping table:", table)
				continue
			}

			//Don't truncate if page state file exists
			pageStateFile := fmt.Sprintf("%s_%s_page_state.txt", keyspace, table)
			if _, err := os.Stat(pageStateFile); err == nil {
				fmt.Println("Skipping truncation of table with copy in progress:", table)
			} else {
				// Truncate the table in the destination cluster
				fmt.Println("Truncating table in the destination cluster:", table)
				if err := l_session.Query(fmt.Sprintf("TRUNCATE %s.%s", keyspace, table)).Exec(); err != nil {
					log.Fatal(err)
				}
			}
			fmt.Println("Copying data from the source cluster to the destination cluster for table:", table)

			// Add a goroutine to the WaitGroup
			wg.Add(1)

			// Send a value into the semaphore channel to limit concurrency
			sem <- struct{}{}

			// Launch a goroutine to copy data
			go func(keyspace, table string) {
				defer wg.Done()
				defer func() { <-sem }() // Receive from the semaphore channel to free up a slot
				copyData(session, l_session, keyspace, table)
				fmt.Println("Copying complete for table:", table)
			}(keyspace, table)
		}
	}

	// Wait for all goroutines to finish
	wg.Wait()
}
func connectToSourceCluster() *gocql.Session {
	// If the source variables are not set in the environment, prompt the user for input
	if os.Getenv("SOURCE_HOST") == "" {
		fmt.Print("Enter the source cluster host: ")
		var SOURCE_HOST string
		fmt.Scanln(&SOURCE_HOST)
		os.Setenv("SOURCE_HOST", SOURCE_HOST)
	}
	if os.Getenv("SOURCE_USER") == "" {
		fmt.Print("Enter the source cluster username: ")
		var SOURCE_USER string
		fmt.Scanln(&SOURCE_USER)
		os.Setenv("SOURCE_USER", SOURCE_USER)
	}
	if os.Getenv("SOURCE_PASSWORD") == "" {
		fmt.Print("Enter the source cluster password: ")
		var SOURCE_PASSWORD string
		fmt.Scanln(&SOURCE_PASSWORD)
		os.Setenv("SOURCE_PASSWORD", SOURCE_PASSWORD)
	}
	SOURCE_HOST := os.Getenv("SOURCE_HOST")
	SOURCE_USER := os.Getenv("SOURCE_USER")
	SOURCE_PASSWORD := os.Getenv("SOURCE_PASSWORD")

	// Create the TLS configuration with InsecureSkipVerify set to true
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	// Connect to the source cluster
	cluster := gocql.NewCluster(SOURCE_HOST)
	cluster.Consistency = gocql.One
	cluster.NumConns = 25
	cluster.Compressor = gocql.SnappyCompressor{}
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: SOURCE_USER,
		Password: SOURCE_PASSWORD,
	}
	cluster.SslOpts = &gocql.SslOptions{
		Config: tlsConfig,
	}

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("Connected to the source cluster.")
	}

	return session
}

func connectToDestinationCluster() *gocql.Session {
	// If the destination variables are not set in the environment, prompt the user for input
	if os.Getenv("DESTINATION_HOST") == "" {
		fmt.Print("Enter the destination cluster host: ")
		var DESTINATION_HOST string
		fmt.Scanln(&DESTINATION_HOST)
		os.Setenv("DESTINATION_HOST", DESTINATION_HOST)
	}
	if os.Getenv("DESTINATION_USER") == "" {
		fmt.Print("Enter the destination cluster username: ")
		var DESTINATION_USER string
		fmt.Scanln(&DESTINATION_USER)
		os.Setenv("DESTINATION_USER", DESTINATION_USER)
	}
	if os.Getenv("DESTINATION_PASSWORD") == "" {
		fmt.Print("Enter the destination cluster password: ")
		var DESTINATION_PASSWORD string
		fmt.Scanln(&DESTINATION_PASSWORD)
		os.Setenv("DESTINATION_PASSWORD", DESTINATION_PASSWORD)
	}
	DESTINATION_HOST := os.Getenv("DESTINATION_HOST")
	DESTINATION_USER := os.Getenv("DESTINATION_USER")
	DESTINATION_PASSWORD := os.Getenv("DESTINATION_PASSWORD")
	// Create the TLS configuration with InsecureSkipVerify set to true
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	// Connect to the destination cluster
	l_cluster := gocql.NewCluster(DESTINATION_HOST)
	l_cluster.Consistency = gocql.One
	l_cluster.NumConns = 25
	l_cluster.Compressor = gocql.SnappyCompressor{}
	l_cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: DESTINATION_USER,
		Password: DESTINATION_PASSWORD,
	}
	l_cluster.SslOpts = &gocql.SslOptions{
		Config: tlsConfig,
	}

	l_session, err := l_cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("Connected to the destination cluster.")
	}

	return l_session
}

func getTables(session *gocql.Session, keyspace string) []string {
	var tables []string
	iter := session.Query("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", keyspace).Iter()
	var table string
	for iter.Scan(&table) {
		tables = append(tables, table)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
	return tables
}
