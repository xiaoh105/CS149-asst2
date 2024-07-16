#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>

#include "itasksys.h"

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem {
public:
  TaskSystemSerial(int num_threads);
  ~TaskSystemSerial();
  const char* name();
  void run(IRunnable* runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                          const std::vector<TaskID>& deps);
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
  const char* name();
  void run(IRunnable* runnable, int num_total_tasks);
  void runThread();
  TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                          const std::vector<TaskID>& deps);
  void sync();

private:
  int num_threads_;
  std::thread *worker_threads_;
  std::atomic_int current_task_;
  int num_total_tasks_{};
  IRunnable *runnable_{};
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
  const char* name();
  void run(IRunnable* runnable, int num_total_tasks);
  void runThread();
  TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                          const std::vector<TaskID>& deps);
  void sync();
private:
  int num_threads_;
  std::thread *worker_threads_;
  int current_task_{};
  int num_total_tasks_{};
  IRunnable *runnable_{};
  std::mutex latch_;
  std::atomic_bool shutdown_{false};
  std::atomic_int done_{0};
};

struct Task {
  IRunnable *runnable_{};
  int num_total_tasks_{};
  Task(IRunnable *runnable, int num_total_tasks) : runnable_(runnable), num_total_tasks_(num_total_tasks) {}
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
  const char* name();
  void runThread();
  void runAsyncThread();
  void run(IRunnable* runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                          const std::vector<TaskID>& deps);
  void sync();

private:
  int num_threads_;
  std::thread *worker_threads_;
  int current_task_{};
  int num_total_tasks_{};
  int current_task_id_{};
  IRunnable *runnable_{};
  std::mutex latch_;
  bool shutdown_{false};
  std::condition_variable invoke_;
  std::unordered_map<int, int> done_{0};
  int current_term_{0};

  std::unordered_map<int, std::vector<int>> dependency_;
  std::unordered_map<int, int> in_degree_;
  std::unordered_map<int, bool> async_done_;
  std::queue<int> waiting_queue_;
  std::queue<int> ready_queue_;

  std::vector<Task> async_tasks_;
  std::condition_variable async_end_;
};

#endif
