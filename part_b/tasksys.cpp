#include "tasksys.h"

#include <cassert>
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
}

#include <iostream>

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  // std::cout << "Deleting" << std::endl;
  latch_.lock();
  shutdown_ = true;
  latch_.unlock();
  invoke_.notify_all();
  for (int i = 0; i < num_threads_; ++i) {
    worker_threads_[i].join();
  }
  delete []worker_threads_;
}

void TaskSystemParallelThreadPoolSleeping::runThread() {
  while (true) {
    latch_.lock();
    if (shutdown_) {
      latch_.unlock();
      return;
    }
    int task_id = current_task_++;
    int total_num = num_total_tasks_;
    int term = current_term_;
    int task = current_task_id_;
    auto runnable = runnable_;
    if (task_id >= total_num && !ready_queue_.empty()) {
      ++term;
      task = current_task_id_ = ready_queue_.front();
      ready_queue_.pop();
      runnable = runnable_ = async_tasks_[current_task_id_].runnable_;
      num_total_tasks_ = async_tasks_[current_task_id_].num_total_tasks_;
      current_task_ = 0;
      task_id = current_task_++;
      ++current_term_;
      total_num = num_total_tasks_;
      // std::cout << "Putting " << current_task_id_ << " into conext" << std::endl;
      invoke_.notify_all();
    }
    latch_.unlock();
    if (task_id >= total_num || runnable == nullptr) {
      std::unique_lock<std::mutex> lck(latch_);
      if (term == current_term_ && !shutdown_) {
        // std::cout << "Start waiting: " << task_id << " " << total_num << " " << shutdown_ << std::endl;
        // invoke_.wait(lck);
        std::this_thread::yield();
        // std::cout << "Finished waiting" << std::endl;
      }
    } else {
      // std::cout << "Executing task " << task_id << ", num " << total_num << std::endl;
      runnable->runTask(task_id, total_num);
      latch_.lock();
      done_[task] += 1;
      if (done_[task] == async_tasks_[task].num_total_tasks_) {
        // std::cout << "Finished task " << task << " : " << async_done_.size() << " " << async_tasks_.size() << std::endl;
        async_done_[task] = true;
        if (async_done_.size() == async_tasks_.size()) {
          async_end_.notify_all();
        } else {
          bool flag = false;
          for (const auto &i : dependency_[task]) {
            --in_degree_[i];
            if (in_degree_[i] == 0) {
              in_degree_.erase(i);
              ready_queue_.push(i);
              flag = true;
            }
          }
          if (flag) {
            // std::cout << "New available task found, invoking" << std::endl;
            invoke_.notify_all();
          }
        }
      }
      latch_.unlock();
    }
  }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
  runAsyncWithDeps(runnable, num_total_tasks, {});
  sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
  latch_.lock();
  int task_id = static_cast<int>(async_tasks_.size());
  Task task{runnable, num_total_tasks};
  async_tasks_.push_back(task);
  waiting_queue_.push(task_id);
  in_degree_[task_id] = static_cast<int>(deps.size());
  for (const auto &i: deps) {
    if (async_done_.find(i) != async_done_.end()) {
      --in_degree_[task_id];
    } else {
      dependency_[i].push_back(task_id);
    }
  }
  if (in_degree_[task_id] == 0) {
    ready_queue_.push(task_id);
    in_degree_.erase(task_id);
  }
  // std::cout << "Created task " << task_id << std::endl;
  latch_.unlock();
  invoke_.notify_all();
  return task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
  // std::cout << "Receiving sync command" << std::endl;
  std::unique_lock<std::mutex> lck(latch_);
  invoke_.notify_all();
  async_end_.wait(lck, [&] { return async_done_.size() == async_tasks_.size(); });
  runnable_ = nullptr;
  async_tasks_.clear();
  dependency_.clear();
  in_degree_.clear();
  async_done_.clear();
  done_.clear();
}
