// Go implementation of the Data Processing System
package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Task represents an individual unit of work
type Task struct {
	ID int
}

// Simulate 1-second processing delay
func (t Task) Process() string {
	time.Sleep(1 * time.Second)
	return fmt.Sprintf("Processed Task %d", t.ID)
}

func main() {
	total_workers := 4
	task_count := 10

	// Shared task channel
	task_queue := make(chan Task, task_count)

	// Shared result slice with a mutex for synchronization
	var completed_results []string
	var result_mutex sync.Mutex

	// WaitGroup to wait for all workers to finish
	var wg sync.WaitGroup

	// Start worker goroutines
	for worker_num := 1; worker_num <= total_workers; worker_num++ {
		wg.Add(1)
		worker_id := worker_num

		go func() {
			defer wg.Done()
			for {
				select {
				case current_task, ok := <-task_queue:
					if !ok {
						fmt.Printf("Worker %d has finished all tasks.\n", worker_id)
						return
					}

					fmt.Printf("Worker %d picked up Task: %d\n", worker_id, current_task.ID)
					task_output := current_task.Process()

					// Lock result slice before writing
					result_mutex.Lock()
					completed_results = append(completed_results, task_output)
					result_mutex.Unlock()

					fmt.Printf("Worker %d finished Task: %d\n", worker_id, current_task.ID)
				case <-time.After(2 * time.Second):
					fmt.Printf("Worker %d waited too long. Exiting...\n", worker_id)
					return
				}
			}
		}()
	}

	// Send tasks to the queue
	for task_id := 1; task_id <= task_count; task_id++ {
		task_queue <- Task{ID: task_id}
	}
	close(task_queue)

	// Wait for all workers to finish
	wg.Wait()

	// Write results to a file
	file, err := os.Create("results.txt")
	if err != nil {
		log.Fatalf("Error creating results file: %v", err)
	}
	defer file.Close()

	for _, output_line := range completed_results {
		_, err := fmt.Fprintln(file, output_line)
		if err != nil {
			log.Printf("Error writing result: %v", err)
		}
	}

	fmt.Println("Results written to results.txt")
}
