import threading
import heapq
import time
from datetime import datetime, timedelta

class Task:
    """
    任务结构体：包含任务ID、优先级、到达时间、可抢占标志和执行函数。
    priority: 优先级，0 为最高；
    arrival: 到达时间；
    preemptible: 是否允许被高优先级任务抢占；
    work: 执行函数，需定期检查抢占事件。
    """
    def __init__(self, id, priority, work, preemptible=True):
        self.id = id
        self.priority = priority
        self.arrival = time.monotonic()
        self.preemptible = preemptible
        self.work = work

    def __lt__(self, other):
        if self.priority == other.priority:
            return self.arrival < other.arrival
        return self.priority < other.priority

class ThreadSafePriorityQueue:
    """
    线程安全最小堆队列：支持容量、阻塞pop、老化和停止。
    """
    def __init__(self, capacity):
        self.capacity = capacity
        self._heap = []
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._stopped = False

    def push(self, task):
        with self._lock:
            if len(self._heap) >= self.capacity:
                print(f"[WARN] 队列已满 (capacity={self.capacity})，丢弃任务 {task.id}")
                return False
            heapq.heappush(self._heap, task)
            self._not_empty.notify()
        return True

    def pop(self):
        with self._not_empty:
            while not self._heap and not self._stopped:
                self._not_empty.wait()
            if self._stopped and not self._heap:
                return None
            return heapq.heappop(self._heap)

    def promote_waiting_tasks(self, threshold_seconds):
        with self._lock:
            if not self._heap:
                return
            now = time.monotonic()
            temp = []
            while self._heap:
                t = heapq.heappop(self._heap)
                waited = now - t.arrival
                if waited >= threshold_seconds and t.priority > 0:
                    t.priority -= 1
                temp.append(t)
            for t in temp:
                heapq.heappush(self._heap, t)

    def stop(self):
        with self._lock:
            self._stopped = True
            self._not_empty.notify_all()

class Scheduler:
    """
    调度器：支持任务抢占，工作线程和老化监控线程。
    """
    def __init__(self, num_workers, capacity=1024, aging_interval=10):
        self.queue = ThreadSafePriorityQueue(capacity)
        self.aging_interval = aging_interval
        self.workers = []
        self._stop_monitor = threading.Event()
        # 抢占事件：有更高优先级任务到达时设置
        self.preempt_event = threading.Event()
        # 记录每个线程当前任务优先级
        self.current_priority = {}
        # 存放被抢占的任务ID
        self.preempted_tasks = []

        for i in range(num_workers):
            t = threading.Thread(target=self._worker_loop, args=(i,))
            t.start()
            self.workers.append(t)

        self.monitor = threading.Thread(target=self._aging_loop)
        self.monitor.start()

    def submit(self, task):
        ok = self.queue.push(task)
        if ok:
            for tid, prio in list(self.current_priority.items()):
                if prio is not None and task.priority < prio:
                    print(f"[Preempt] 任务 {task.id} 优先级高于线程 {tid} 的当前任务，触发抢占")
                    self.preempt_event.set()
                    break
        else:
            print(f"[ERROR] 任务 {task.id} 因队列溢出被丢弃")
        return ok

    def _worker_loop(self, idx):
        thread_id = threading.get_ident()
        while True:
            task = self.queue.pop()
            if task is None:
                break
            self.current_priority[thread_id] = task.priority
            self.preempt_event.clear()
            print(f"[Worker {idx}] 开始执行任务 {task.id} (优先级={task.priority})")
            try:
                task.work(self.preempt_event, task)
            except PreemptedException:
                print(f"[Worker {idx}] 任务 {task.id} 被抢占，重新入队")
                self.preempted_tasks.append(task.id)
                task.arrival = time.monotonic()
                self.queue.push(task)
            except Exception as e:
                print(f"[Worker {idx}] 任务 {task.id} 出错: {e}")
            finally:
                self.current_priority[thread_id] = None
        print(f"[Worker {idx}] 退出")

    def _aging_loop(self):
        while not self._stop_monitor.is_set():
            time.sleep(self.aging_interval)
            self.queue.promote_waiting_tasks(self.aging_interval)

    def shutdown(self):
        self.queue.stop()
        for t in self.workers:
            t.join()
        self._stop_monitor.set()
        self.monitor.join()

class PreemptedException(Exception):
    pass

# 示例用工作函数：支持抢占，定期检查事件

def make_preemptible_work(idx, total_steps=10, step_duration=0.2):
    def work(preempt_event, task):
        for step in range(total_steps):
            if preempt_event.is_set() and task.preemptible:
                raise PreemptedException()
            print(f"|----[Task] {idx} 执行第 {step+1}/{total_steps} 步")
            time.sleep(step_duration)
        print(f"|----[Task] {idx} 完成")
    return work

if __name__ == "__main__":
    print("[Main] 启动调度器 (4线程)，支持抢占")
    sched = Scheduler(num_workers=4, capacity=1024, aging_interval=5)
    print("[Main] 提交测试任务：先提交一个长任务，再提交高优先级任务进行抢占验证")

    # 提交一个长任务（低优先级，可被抢占）
    long_task = Task(id=100, priority=3, work=make_preemptible_work(100, total_steps=50, step_duration=0.1))
    sched.submit(long_task)
    time.sleep(0.5)

    # 提交高优先级任务（抢占）
    high_task = Task(id=200, priority=0, work=make_preemptible_work(200, total_steps=5, step_duration=0.1))
    sched.submit(high_task)

    # 等待所有任务执行完毕
    time.sleep(10)
    sched.shutdown()

    # 验证抢占结果
    if 100 in sched.preempted_tasks:
        print("[Test Result] 高优先级任务成功抢占低优先级任务 100！")
    else:
        print("[Test Result] 未检测到抢占，测试失败。")
