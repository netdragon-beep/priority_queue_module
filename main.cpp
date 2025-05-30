// priority_queue_module.cpp
// 基于线程安全优先队列的调度器，支持公平性（老化）和溢出告警。
// 编译：g++ -std=c++20 -pthread priority_queue_module.cpp -o scheduler

#include <iostream>
#include <queue>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <atomic>
#include <functional>

//---------------------------------------------------------------------
// 任务结构定义
//---------------------------------------------------------------------
struct Task {
    int id; // 任务唯一标识
    int priority; // 0 表示最高优先级
    std::chrono::steady_clock::time_point arrival; // 到达时间，用于老化/FIFO
    std::function<void()> work; // 任务执行函数

    // 用于 std::priority_queue 的比较器（最小堆：优先级小在前，若相等则先到先服务）
    bool operator>(const Task &other) const {
        if (priority == other.priority)
            return arrival > other.arrival; // 较早到达的任务优先
        return priority > other.priority;
    }
};

//---------------------------------------------------------------------
// 线程安全的最小堆优先队列
//---------------------------------------------------------------------
class ThreadSafePriorityQueue {
public:
    explicit ThreadSafePriorityQueue(std::size_t capacity)
        : max_capacity(capacity) {
    }

    // 插入任务；队列满时返回 false
    bool push(const Task &task) {
        std::lock_guard<std::mutex> lock(mtx); //加锁，防止多个线程同时操作 q 队列。
        if (q.size() >= max_capacity) {
            std::cerr << "[WARN] Queue overflow (capacity=" << max_capacity << ")\n";
            return false;
        }
        q.push(task);
        cv.notify_one();
        return true;
    }

    // 阻塞弹出；当队列已停止且为空时返回 false
    bool pop(Task &task) {
        std::unique_lock<std::mutex> lock(mtx); //加锁，保护对共享优先队列 q 的访问。
        cv.wait(lock, [this] { return stopped || !q.empty(); });
        if (stopped && q.empty()) return false;
        task = q.top();
        q.pop();
        return true;
    }

    // 老化机制：将等待超过阈值的任务提升优先级
    void promoteWaitingTasks(std::chrono::milliseconds threshold) {
        std::lock_guard<std::mutex> lock(mtx);
        if (q.empty()) return;

        std::vector<Task> tmp;
        auto now = std::chrono::steady_clock::now();
        while (!q.empty()) {
            Task t = q.top();
            q.pop();
            //计算任务等待时间
            auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(now - t.arrival);
            if (waited >= threshold && t.priority > 0) {
                --t.priority; // 向更高优先级提升一级
            }
            tmp.push_back(t);
        }
        for (auto &t: tmp) q.push(t);
    }

    // 停止队列，唤醒所有等待线程
    void stop() { {
            std::lock_guard<std::mutex> lock(mtx);
            stopped = true;
        }
        cv.notify_all();
    }

private:
    struct Compare {
        bool operator()(const Task &a, const Task &b) const { return a > b; }
    };

    std::priority_queue<Task, std::vector<Task>, Compare> q;
    const std::size_t max_capacity;
    std::mutex mtx;
    std::condition_variable cv;
    bool stopped{false};
};

//---------------------------------------------------------------------
// 调度器：工作线程池 + 老化监控线程
//---------------------------------------------------------------------
class Scheduler {
public:
    Scheduler(std::size_t worker_threads,
              std::size_t capacity = 1024,
              std::chrono::milliseconds aging = std::chrono::seconds(10))
        : queue(capacity), promote_interval(aging) {
        // 创建工作线程
        for (std::size_t i = 0; i < worker_threads; ++i) {
            workers.emplace_back(&Scheduler::workerLoop, this, i);
        }
        // 启动老化监控线程
        monitor = std::thread(&Scheduler::agingLoop, this);
    }

    ~Scheduler() {
        queue.stop();
        for (auto &t: workers) t.join(); //阻塞等待每个工作线程执行完毕并退出，确保没有线程悬挂。
        stop_monitor = true; //使 agingLoop() 循环条件失效，监控线程将会终止。
        if (monitor.joinable()) monitor.join(); //调用 join() 等待监控线程退出。
    }

    // 提交任务到调度器
    bool submit(Task task) {
        bool ok = queue.push(task);
        if (!ok) {
            std::cerr << "[ERROR] Task " << task.id << " dropped due to overflow\n";
        }
        return ok;
    }

private:
    // 工作线程主循环
    void workerLoop(std::size_t idx) {
        Task task;
        while (queue.pop(task)) {
            try {
                task.work();
            } catch (const std::exception &e) {
                std::cerr << "[Worker " << idx << "] task " << task.id
                        << " threw: " << e.what() << "\n";
            }
        }
    }

    // 老化监控循环
    void agingLoop() {
        while (!stop_monitor) {
            std::this_thread::sleep_for(promote_interval);
            queue.promoteWaitingTasks(promote_interval);
        }
    }

    ThreadSafePriorityQueue queue;
    const std::chrono::milliseconds promote_interval;
    std::vector<std::thread> workers;
    std::thread monitor;
    std::atomic<bool> stop_monitor{false};
};

//---------------------------------------------------------------------
// 示例入口（生产环境请替换）
//---------------------------------------------------------------------
int main() {
    std::cout << "[Main] Scheduler starting with 4 worker threads\n";
    Scheduler sched(4, 1024, std::chrono::seconds(5)); // 4 个工作线程
    std::cout << "[Main] Submitting 20 tasks...\n";

    // 提交 20 个示例任务，优先级从 0-4 轮流分配
    for (int i = 0; i < 20; ++i) {
        int each_working_time = 1;
        Task t{
            .id = i,
            .priority = i % 5, // 示例优先级
            .arrival = std::chrono::steady_clock::now(),
            .work = [i,each_working_time] {
                std::cout << "|----[Task] Running task " << i
                        << " on thread " << std::this_thread::get_id() << "\n";
                std::this_thread::sleep_for(std::chrono::seconds(each_working_time)); //休眠 （模拟工作）
                std::cout << "|----[Task] Finished task " << i << "\n";
            }
        };
        bool ok = sched.submit(t);
        if (ok) {
            std::cout << "[Main] Submitted task " << i
                    << " (priority=" << t.priority << ")\n";
        } else {
            std::cout << "[Main][ERROR] Failed to submit task " << i << "\n";
        }
        //主线程稍作休眠，以控制任务提交的速率，避免一次性提交全部任务。
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
    int wait_time = 30;
    std::cout << "[Main] All tasks submitted. Letting scheduler run for " << wait_time << "s...\n";
    // 让调度器运行一段时间
    std::this_thread::sleep_for(std::chrono::seconds(wait_time));
    std::cout << "[Main] Time up, exiting now.\n";
    return 0;
}
