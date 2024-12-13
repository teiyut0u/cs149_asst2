#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem {
public:
  TaskSystemSerial(int num_threads);
  ~TaskSystemSerial();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem {
public:
  TaskSystemParallelSpawn(int num_threads);
  ~TaskSystemParallelSpawn();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();

private:
  struct task {
    int start, end;
  };
  static constexpr int task_threshold = 8;
  static constexpr int steal_threshold = 16;
  std::vector<std::thread> workers;
  int n_workers;
  std::vector<std::queue<task>> task_queue;
  std::vector<std::atomic_bool> is_queue_empty;
  std::vector<std::mutex> queue_mutex;
  std::atomic_uint working_count;

  void interleaveWork(IRunnable *runnable, int i, int num_total_tasks);
  void staticRunInterleave(IRunnable *runnable, int num_total_tasks);

  void dynamicRun(IRunnable *runnable, int num_total_tasks);
  void queueWork(IRunnable *runnable, int num_total_tasks, int id);
  void reference(IRunnable *runnable, int num_total_tasks);
  void runThread(std::atomic_int *task_id, IRunnable *runnable,
                 int num_total_tasks);
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSpinning(int num_threads);
  ~TaskSystemParallelThreadPoolSpinning();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();

private:
  struct task {
    IRunnable *runnable;
    int id, num_total_tasks;
  };
  unsigned int n_workers;
  std::vector<std::thread> workers;
  static constexpr int task_size = 2;
  std::queue<task> task_queue;
  std::mutex queue_mutex;
  bool stop;
  std::atomic_uint n_rest_tasks;
  void runThread();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSleeping(int num_threads);
  ~TaskSystemParallelThreadPoolSleeping();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

#endif
