import { IExecutor } from './Executor';
import ITask from './Task';

export default async function run(
  executor: IExecutor,
  queue: AsyncIterable<ITask>,
  maxThreads = 0
): Promise<void> {
  maxThreads = Math.max(0, maxThreads);

  // Track active tasks by targetId
  const activeTargets = new Set<number>();
  // Track ongoing tasks
  const runningTasks: Promise<void>[] = [];

  // Helper function to process a single task
  const processTask = async (task: ITask) => {
    const { targetId } = task;

    // Wait until no other task with the same targetId is active
    while (activeTargets.has(targetId)) {
      console.log(task)
      await Promise.race(runningTasks);
    }

    // Mark targetId as active
    activeTargets.add(targetId);

    try {
      await executor.executeTask(task); // Execute the task
    } finally {
      // Remove targetId from active set after execution
      activeTargets.delete(targetId);
    }
  };

  // Iterate over tasks from the queue
  for await (const task of queue) {
    const taskPromise = processTask(task);

    runningTasks.push(taskPromise);

    // Limit the number of concurrent tasks if maxThreads > 0
    if (maxThreads > 0 && runningTasks.length >= maxThreads) {
      await Promise.race(runningTasks); // Wait for one task to complete
    }

    // Remove completed tasks from runningTasks
    runningTasks.splice(
      0,
      runningTasks.findIndex((task) => task !== taskPromise)
    );
  }

  // Wait for all tasks to complete
  await Promise.all(runningTasks);
}