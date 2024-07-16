#include "tasksys.h"

#include <thread>


IRunnable::~IRunnable() {
}

ITaskSystem::ITaskSystem(int num_threads) {
}

ITaskSystem::~ITaskSystem() {
}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
  return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {
}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
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

const char* TaskSystemParallelSpawn::name() {
  return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), num_threads_(num_threads) {
  worker_threads_ = new std::thread[num_threads_];
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
  delete [] worker_threads_;
}

void TaskSystemParallelSpawn::runThread() {
  while (true) {
    int task_id = current_task_.fetch_add(1);
    if (task_id >= num_total_tasks_) {
      return;
    }
    runnable_->runTask(task_id, num_total_tasks_);
  }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
  num_total_tasks_ = num_total_tasks;
  current_task_.store(0);
  runnable_ = runnable;
  for (int i = 0; i < num_threads_; ++i) {
    worker_threads_[i] = std::thread(&TaskSystemParallelSpawn::runThread, this);
  }
  for (int i = 0; i < num_threads_; ++i) {
    worker_threads_[i].join();
  }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemParallelSpawn::sync() {
  // You do not need to implement this method.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
  return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), num_threads_(num_threads) {
  worker_threads_ = new std::thread[num_threads_];
  for (int i = 0; i < num_threads_; ++i) {
    worker_threads_[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::runThread, this);
  }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
  shutdown_.store(true);
  for (int i = 0; i < num_threads_; ++i) {
    worker_threads_[i].join();
  }
  delete []worker_threads_;
}

void TaskSystemParallelThreadPoolSpinning::runThread() {
  while (!shutdown_.load()) {
    latch_.lock();
    int task_id = current_task_++;
    int total_num = num_total_tasks_;
    auto runnable = runnable_;
    latch_.unlock();
    if (task_id >= total_num || runnable == nullptr) {
      std::this_thread::yield();
    } else {
      runnable->runTask(task_id, total_num);
      done_.fetch_add(1);
    }
  }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
  latch_.lock();
  runnable_ = runnable;
  num_total_tasks_ = num_total_tasks;
  current_task_ = 0;
  done_.store(0);
  latch_.unlock();
  while (true) {
    if (done_.load() == num_total_tasks) {
      return;
    }
    std::this_thread::yield();
  }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
  // You do not need to implement this method.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
  return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), num_threads_(num_threads) {
  worker_threads_ = new std::thread[num_threads_];
  for (int i = 0; i < num_threads_; ++i) {
    worker_threads_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::runThread, this);
  }
  async_thread_ = std::thread(&TaskSystemParallelThreadPoolSleeping::runAsyncThread, this);
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  shutdown_.store(true);
  invoke_.notify_all();
  for (int i = 0; i < num_threads_; ++i) {
    worker_threads_[i].join();
  }
  delete []worker_threads_;
  async_invoke_.notify_all();
  async_thread_.join();
}

void TaskSystemParallelThreadPoolSleeping::runThread() {
  while (!shutdown_.load()) {
    latch_.lock();
    int task_id = current_task_++;
    int total_num = num_total_tasks_;
    auto runnable = runnable_;
    latch_.unlock();
    if (task_id >= total_num || runnable == nullptr) {
      std::unique_lock<std::mutex> lck(done_latch_);
      done_ += 1;
      if (done_ == num_threads_) {
        done_signal_.notify_all();
      }
      invoke_.wait(lck);
    } else {
      runnable->runTask(task_id, total_num);
    }
  }
}

void TaskSystemParallelThreadPoolSleeping::runAsyncThread() {
  while (!shutdown_.load()) {
    std::unique_lock<std::mutex> lck(async_latch_);
    if (waiting_list_.empty()) {
      async_end_.notify_all();
      async_invoke_.wait(lck);
    } else {
      auto task_id = waiting_list_.back();
      auto &task = async_tasks_[task_id];
      waiting_list_.pop_back();
      lck.unlock();
      run(task.runnable_, task.num_total_tasks_);
      lck.lock();
      for (const auto &i : dependency_[task_id]) {
        --in_degree_[i];
        if (in_degree_[i] == 0) {
          in_degree_.erase(i);
          waiting_list_.push_back(i);
        }
      }
      async_done_[task_id] = true;
      lck.unlock();
    }
  }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
  latch_.lock();
  current_task_ = 0;
  num_total_tasks_ = num_total_tasks;
  runnable_ = runnable;
  done_ = 0;
  latch_.unlock();
  invoke_.notify_all();
  std::unique_lock<std::mutex> lck(done_latch_);
  done_signal_.wait(lck, [&]{ return done_ == num_threads_; });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
  async_latch_.lock();
  int task_id = static_cast<int>(async_tasks_.size());
  Task task{runnable, num_total_tasks};
  async_tasks_.push_back(task);
  in_degree_[task_id] = static_cast<int>(deps.size());
  for (const auto &i: deps) {
    if (async_done_.find(i) != async_done_.end()) {
      --in_degree_[task_id];
    } else {
      dependency_[i].push_back(task_id);
    }
  }
  async_latch_.unlock();
  async_invoke_.notify_all();
  return task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
  std::unique_lock<std::mutex> lck(async_latch_);
  async_invoke_.notify_all();
  async_end_.wait(lck, [&] { return async_done_.size() == async_tasks_.size(); });
}
