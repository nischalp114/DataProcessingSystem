import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class DataSystem {

    // Number of worker
    private static final int total_workers = 4;

    // task queue
    private static final BlockingQueue<Task> shared_task_queue = new ArrayBlockingQueue<>(10);

    //list that stores completed task outputs
    private static final List<String> completed_results = Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) {

        // thread pool for worker management
        ExecutorService thread_pool = Executors.newFixedThreadPool(total_workers);

        // Submit worker threads to the thread pool
        for (int worker_num = 0; worker_num < total_workers; worker_num++) {
            int worker_id = worker_num + 1;

            thread_pool.submit(() -> {
                while (true) {
                    try {
                        //looking for task
                        Task current_task = shared_task_queue.poll(2, TimeUnit.SECONDS);

                        // Exit if no task is found in time
                        if (current_task == null) {
                            System.out.println("Worker " + worker_id + " has finished all the task.");
                            break;
                        }

                        System.out.println("Worker " + worker_id + " has gotten Task :" + current_task.getId());

                        // Process the task (with simulated delay)
                        String task_output = current_task.process();

                        // Add result to the shared result list
                        completed_results.add(task_output);

                        System.out.println("Worker " + worker_id + " has finished Task :" + current_task.getId());

                    } catch (InterruptedException e) {//Interuption exception
                        System.err.println("Worker " + worker_id + " was interrupted.");
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {//IOE exception
                        System.err.println("Worker " + worker_id + " error: " + e.getMessage());
                    }
                }
            });
        }

        // Add tasks to the queue for processing
        for (int task_id = 1; task_id <= 10; task_id++) {
            try {
                shared_task_queue.put(new Task(task_id)); // blocks if queue is full
            } catch (InterruptedException e) {
                System.err.println("Main thread interrupted while adding Task " + task_id);
                Thread.currentThread().interrupt();
            }
        }

        // Gracefully shut down the worker threads
        thread_pool.shutdown();
        try {
            thread_pool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Main thread was interrupted while waiting.");
        }

        // Write the results to a file after processing
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("results.txt"))) {
            for (String output_line : completed_results) {
                writer.write(output_line);
                writer.newLine();
            }
            System.out.println("Results written to results.txt");
        } catch (IOException e) {
            System.err.println("Error writing results: " + e.getMessage());
        }
    }
}

// Represents an individual unit of work
class Task {
    private final int task_id;

    public Task(int id) {
        this.task_id = id;
    }

    public int getId() {
        return task_id;
    }

    // 1-second delay
    public String process() throws InterruptedException {
        Thread.sleep(1000);
        return "Processed Task " + task_id;
    }
}
