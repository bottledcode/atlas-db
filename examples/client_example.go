package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/withinboredom/atlas-db-2/pkg/client"
)

func main() {
	// Connect to Atlas DB
	db, err := client.Connect("localhost:8080", client.WithTimeout(10*time.Second))
	if err != nil {
		log.Fatalf("Failed to connect to Atlas DB: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Example 1: Basic Put and Get operations
	fmt.Println("=== Basic Operations ===")
	
	// Put a key-value pair
	putResult, err := db.Put(ctx, "user:123", []byte(`{"name": "John Doe", "email": "john@example.com"}`))
	if err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	fmt.Printf("Put successful, version: %d\n", putResult.Version)

	// Get the value back
	getResult, err := db.Get(ctx, "user:123")
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	
	if getResult.Found {
		fmt.Printf("Get successful: %s (version: %d)\n", string(getResult.Value), getResult.Version)
	} else {
		fmt.Println("Key not found")
	}

	// Example 2: Consistent reads
	fmt.Println("\n=== Consistent Read ===")
	consistentResult, err := db.Get(ctx, "user:123", client.WithConsistentRead())
	if err != nil {
		log.Fatalf("Consistent get failed: %v", err)
	}
	fmt.Printf("Consistent read: %s (version: %d)\n", string(consistentResult.Value), consistentResult.Version)

	// Example 3: Conditional updates with versioning
	fmt.Println("\n=== Conditional Updates ===")
	
	// Update with expected version
	updatedData := []byte(`{"name": "John Doe", "email": "john.doe@newdomain.com", "updated": true}`)
	updateResult, err := db.Put(ctx, "user:123", updatedData, client.WithExpectedVersion(putResult.Version))
	if err != nil {
		log.Fatalf("Conditional update failed: %v", err)
	}
	fmt.Printf("Conditional update successful, new version: %d\n", updateResult.Version)

	// Try to update with wrong version (should fail)
	_, err = db.Put(ctx, "user:123", []byte("bad update"), client.WithExpectedVersion(putResult.Version))
	if err != nil {
		fmt.Printf("Expected failure for wrong version: %v\n", err)
	}

	// Example 4: Batch operations
	fmt.Println("\n=== Batch Operations ===")
	
	batchResult, err := db.Batch(ctx,
		client.Put("product:1", []byte(`{"name": "Laptop", "price": 999.99}`)),
		client.Put("product:2", []byte(`{"name": "Mouse", "price": 29.99}`)),
		client.Put("product:3", []byte(`{"name": "Keyboard", "price": 79.99}`)),
		client.Get("user:123"),
	)
	if err != nil {
		log.Fatalf("Batch operation failed: %v", err)
	}

	fmt.Println("Batch operation results:")
	for i, result := range batchResult.Results {
		if result.Success {
			if result.Value != nil {
				fmt.Printf("  Operation %d: Success - %s (version: %d)\n", i, string(result.Value), result.Version)
			} else {
				fmt.Printf("  Operation %d: Success (version: %d)\n", i, result.Version)
			}
		} else {
			fmt.Printf("  Operation %d: Failed - %s\n", i, result.Error)
		}
	}

	// Example 5: Scan operations
	fmt.Println("\n=== Scan Operations ===")
	
	scanResult, err := db.Scan(ctx, 
		client.WithStartKey("product:"),
		client.WithEndKey("product:z"),
		client.WithLimit(10),
	)
	if err != nil {
		log.Fatalf("Scan failed: %v", err)
	}

	fmt.Printf("Scan found %d items:\n", len(scanResult.Items))
	for _, item := range scanResult.Items {
		fmt.Printf("  %s: %s (version: %d)\n", item.Key, string(item.Value), item.Version)
	}

	// Example 6: Delete operation
	fmt.Println("\n=== Delete Operations ===")
	
	// Delete a product
	err = db.Delete(ctx, "product:2")
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	fmt.Println("Product:2 deleted successfully")

	// Verify deletion
	deletedResult, err := db.Get(ctx, "product:2")
	if err != nil {
		log.Fatalf("Get after delete failed: %v", err)
	}
	
	if !deletedResult.Found {
		fmt.Println("Confirmed: product:2 is no longer found")
	} else {
		fmt.Println("Unexpected: product:2 still exists")
	}

	// Example 7: Conditional delete with version
	fmt.Println("\n=== Conditional Delete ===")
	
	// Get current version of user:123
	currentUser, err := db.Get(ctx, "user:123")
	if err != nil {
		log.Fatalf("Get for conditional delete failed: %v", err)
	}

	// Delete with expected version
	err = db.Delete(ctx, "user:123", client.WithExpectedVersionForDelete(currentUser.Version))
	if err != nil {
		log.Fatalf("Conditional delete failed: %v", err)
	}
	fmt.Println("User:123 deleted with version check")

	fmt.Println("\n=== Example Complete ===")
}