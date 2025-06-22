import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class DataSystem {

    private static final int total_workers = 4;
    private static final BlockingQueue<Task> shared_task_queue = new ArrayBlockingQueue<>(10);
    private static final List<String> completed_results = Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) {
        ExecutorService thread_pool = Executors.newFixedThreadPool(total_workers);

        for (int worker_num = 0; worker_num < total_workers; worker_num++) {
            int worker_id = worker_num + 1;

            thread_pool.submit(() -> {
                while (true) {
                    try {
                        Task current_task = shared_task_queue.poll(2, TimeUnit.SECONDS);
                        if (current_task == null) {
                            System.out.println("Worker " + worker_id + " has finished all tasks.");
                            break;
                        }

                        System.out.println("Worker " + worker_id + " picked up Task: " + current_task.getId());
                        String task_output = current_task.process();
                        completed_results.add(task_output);
                        System.out.println("Worker " + worker_id + " finished Task: " + current_task.getId());

                    } catch (InterruptedException e) {
                        System.err.println("Worker " + worker_id + " was interrupted.");
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        System.err.println("Worker " + worker_id + " error: " + e.getMessage());
                    }
                }
            });
        }

        for (int task_id = 1; task_id <= 10; task_id++) {
            try {
                shared_task_queue.put(new Task(task_id));
            } catch (InterruptedException e) {
                System.err.println("Main thread interrupted while adding Task " + task_id);
                Thread.currentThread().interrupt();
            }
        }

        thread_pool.shutdown();
        try {
            thread_pool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Main thread was interrupted while waiting.");
        }

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

class Task {
    private final int task_id;

    public Task(int id) {
        this.task_id = id;
    }

    public int getId() {
        return task_id;
    }

    public String process() throws InterruptedException {
        Thread.sleep(1000);
        return "Processed Task " + task_id;
    }
}
