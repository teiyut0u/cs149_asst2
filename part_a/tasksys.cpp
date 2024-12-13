#include "tasksys.h"
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name() { return "Serial"; }

TaskSystemSerial::TaskSystemSerial(int num_threads)
    : ITaskSystem(num_threads) {}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks) {
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable,
                                          int num_total_tasks,
                                          const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemSerial::sync() {
  // You do not need to implement this method.
  return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name() {
  return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads), n_workers(num_threads) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //

  this->workers = std::vector<std::thread>(num_threads);
  this->task_queue = std::vector<std::queue<task>>(num_threads);
  this->is_queue_empty = std::vector<std::atomic_bool>(num_threads);
  this->queue_mutex = std::vector<std::mutex>(num_threads);
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks) {

  //
  // TODO: CS149 students will modify the implementation of this
  // method in Part A.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //
  std::atomic_int task_id{-1};
  for (int i = 1; i < n_workers; ++i) {
    workers[i] =
        std::thread(std::bind(&TaskSystemParallelSpawn::runThread, this,
                              &task_id, runnable, num_total_tasks));
  }

  this->runThread(&task_id, runnable, num_total_tasks);
  for (int i = 1; i < n_workers; ++i) {
    workers[i].join();
  }
  // this->reference(runnable, num_total_tasks);
  // this->dynamicRun(runnable, num_total_tasks);
  // this->staticRunInterleave(runnable, num_total_tasks);
  // -----------------------------------
  //   for (int i = 0; i < num_total_tasks; i++) {
  //     runnable->runTask(i, num_total_tasks);
  //   }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemParallelSpawn::sync() {
  // You do not need to implement this method.
  return;
}

void TaskSystemParallelSpawn::interleaveWork(IRunnable *runnable, int i,
                                             int num_total_tasks) {
  for (; i < num_total_tasks; i += n_workers) {
    runnable->runTask(i, num_total_tasks);
  }
}

void TaskSystemParallelSpawn::staticRunInterleave(IRunnable *runnable,
                                                  int num_total_tasks) {
  for (auto i = 1; i < n_workers; ++i) {
    workers[i] = std::thread(std::bind(&TaskSystemParallelSpawn::interleaveWork,
                                       this, runnable, i, num_total_tasks));
  }

  this->interleaveWork(runnable, 0, num_total_tasks);

  for (auto i = 1; i < n_workers; ++i) {
    workers[i].join();
  }
}

void TaskSystemParallelSpawn::dynamicRun(IRunnable *runnable,
                                         int num_total_tasks) {
  for (int i = 0; i < n_workers; ++i) {
    is_queue_empty[i] = false;
  }
  int n_works_per_worker = num_total_tasks / n_workers;
  for (int i = 0; i < n_workers - 1; ++i) {
    task_queue[i].push(
        task{i * n_works_per_worker, (i + 1) * n_works_per_worker});
  }
  task_queue[n_workers - 1].push(
      task{(n_workers - 1) * n_works_per_worker, num_total_tasks});
  working_count = 0;
  for (int i = 1; i < n_workers; ++i) {
    workers[i] = std::thread(std::bind(&TaskSystemParallelSpawn::queueWork,
                                       this, runnable, num_total_tasks, i));
  }
  this->queueWork(runnable, num_total_tasks, 0);
  for (int i = 1; i < n_workers; ++i) {
    workers[i].join();
  }
}

void TaskSystemParallelSpawn::queueWork(IRunnable *runnable,
                                        int num_total_tasks, int id) {
  bool is_empty;

work:
  ++working_count;
  while (true) {
    task current_task;
    queue_mutex[id].lock();
    if (task_queue[id].empty()) {
      queue_mutex[id].unlock();
      break;
    }

    current_task = task_queue[id].front();
    task_queue[id].pop();

    while (current_task.end - current_task.start > task_threshold) {
      int half = (current_task.start + current_task.end) >> 1;
      task_queue[id].push(task{half, current_task.end});
      current_task.end = half;
    }

    is_empty = task_queue.empty();

    queue_mutex[id].unlock();

    // work
    for (int i = current_task.start; i < current_task.end; ++i) {
      runnable->runTask(i, num_total_tasks);
    }

    if (is_empty) {
      break;
    }
#ifdef DEBUG
    printf("id: %d, done work start %d end %d\n", id, current_task.start,
           current_task.end);
#endif
  }

  is_queue_empty[id] = true;
  --working_count;

  while (true) {
    if (working_count == 0) {
#ifdef DEBUG
      printf("id: %d, finish\n", id);
#endif
      return;
    }
    // steal
    // for (int i = 0; i < n_workers; ++i)
    {
      int i = (rand() / RAND_MAX) * n_workers;
      if (!is_queue_empty[i]) {
        queue_mutex[i].lock();
        if (!task_queue[i].empty()) {
          task stolen_task = task_queue[i].front();
          task_queue[i].pop();
          queue_mutex[i].unlock();

          queue_mutex[id].lock();
          task_queue[id].push(stolen_task);
          queue_mutex[id].unlock();
          goto work;
        }

        queue_mutex[i].unlock();
      }
    }
  }
}

void TaskSystemParallelSpawn::reference(IRunnable *runnable,
                                        int num_total_tasks) {
  std::atomic<int> taskId(0);
  std::thread threads[this->n_workers];

  for (auto &thread : threads) {
    thread = std::thread([&taskId, num_total_tasks, runnable] {
      for (int id = taskId++; id < num_total_tasks; id = taskId++)
        runnable->runTask(id, num_total_tasks);
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }
}

void TaskSystemParallelSpawn::runThread(std::atomic_int *task_id,
                                        IRunnable *runnable,
                                        int num_total_tasks) {
  for (int i = ++(*task_id); i < num_total_tasks; i = ++(*task_id)) {
    runnable->runTask(i, num_total_tasks);
  }
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name() {
  return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(
    int num_threads)
    : ITaskSystem(num_threads), n_workers(num_threads), stop(false) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //

  this->workers = std::vector<std::thread>(num_threads);
  for (auto &worker : workers) {
    worker = std::thread(
        std::bind(&TaskSystemParallelThreadPoolSpinning::runThread, this));
  }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
  stop = true;
  for (auto &worker : workers) {
    worker.join();
  }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable,
                                               int num_total_tasks) {

  //
  // TODO: CS149 students will modify the implementation of this
  // method in Part A.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //

  queue_mutex.lock();
  for (int i = 0; i < num_total_tasks; ++i) {
    task_queue.push(task{runnable, i, num_total_tasks});
  }
  n_rest_tasks = num_total_tasks;
  queue_mutex.unlock();

  while (n_rest_tasks)
    ;
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
  // You do not need to implement this method.
  return;
}

void TaskSystemParallelThreadPoolSpinning::runThread() {
  task current_task;
  while (!stop) {
    queue_mutex.lock();
    if (!task_queue.empty()) {
      current_task = task_queue.front();
      task_queue.pop();
    } else {
      queue_mutex.unlock();
      continue;
    }
    queue_mutex.unlock();
    current_task.runnable->runTask(current_task.id,
                                   current_task.num_total_tasks);
    --n_rest_tasks;
  }
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name() {
  return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads)
    : ITaskSystem(num_threads) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  //
  // TODO: CS149 student implementations may decide to perform cleanup
  // operations (such as thread pool shutdown construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable,
                                               int num_total_tasks) {

  //
  // TODO: CS149 students will modify the implementation of this
  // method in Parts A and B.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //

  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {

  //
  // TODO: CS149 students will implement this method in Part B.
  //

  return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

  //
  // TODO: CS149 students will modify the implementation of this method in Part
  // B.
  //

  return;
}
