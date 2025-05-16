import threading
import heapq
import time
import random
from datetime import datetime, timedelta

class Task:
    """
    任务结构体：包含任务ID、优先级、到达时间、可抢占标志和执行函数。
    priority: 优先级，1 为最高，5 为最低；
    arrival: 到达时间；
    preemptible: 是否允许被紧急任务抢占；仅在任务支持抢占时才会中断；
    work: 执行函数，需定期检查抢占事件。
    """
    def __init__(self, id, priority, work, preemptible=True):
        self.id = id
        self.priority = priority
        self.arrival = time.monotonic()
        self.preemptible = preemptible
        self.work = work

    def __lt__(self, other):
        # 优先级相同时，按到达先后顺序处理
        if self.priority == other.priority:
            return self.arrival < other.arrival
        return self.priority < other.priority

class ThreadSafePriorityQueue:
    """
    线程安全最小堆队列：支持容量、阻塞 pop、老化和停止控制。
    """
    def __init__(self, capacity):
        self.capacity = capacity
        self._heap = []
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._stopped = False

    def push(self, task):
        """将任务推入队列；队列满则丢弃并返回 False"""
        with self._lock:
            if len(self._heap) >= self.capacity:
                print(f"[WARN] 队列已满 (capacity={self.capacity})，丢弃任务 {task.id}")
                return False
            heapq.heappush(self._heap, task)
            self._not_empty.notify()
        return True

    def pop(self):
        """阻塞弹出任务；队列停止且空时返回 None"""
        with self._not_empty:
            while not self._heap and not self._stopped:
                self._not_empty.wait()
            if self._stopped and not self._heap:
                return None
            return heapq.heappop(self._heap)

    def promote_waiting_tasks(self, threshold_seconds):
        """老化机制：等待超过阈值的任务提升优先级"""
        with self._lock:
            if not self._heap:
                return
            now = time.monotonic()
            temp = []
            while self._heap:
                t = heapq.heappop(self._heap)
                waited = now - t.arrival
                if waited >= threshold_seconds and t.priority > 1:
                    t.priority -= 1  # 提升优先级，但不超过 1
                temp.append(t)
            for t in temp:
                heapq.heappush(self._heap, t)

    def stop(self):
        """停止队列并唤醒所有等待线程"""
        with self._lock:
            self._stopped = True
            self._not_empty.notify_all()

class Scheduler:
    """
    调度器：支持仅 priority==1 的紧急任务抢占，工作线程和老化监控线程。
    抢占逻辑：只有提交的任务优先级为 1 时，且比当前执行任务优先级更高（数字更小），才触发抢占。
    """
    def __init__(self, num_workers, capacity=1024, aging_interval=10):
        self.queue = ThreadSafePriorityQueue(capacity)
        self.aging_interval = aging_interval
        self.workers = []
        self._stop_monitor = threading.Event()
        self.preempt_event = threading.Event()
        self.current_priority = {}      # 记录线程当前任务的优先级
        self.preempted_tasks = []       # 存放被抢占的任务ID
        self.completed_tasks = []       # 存放正常完成的任务ID

        for i in range(num_workers):
            t = threading.Thread(target=self._worker_loop, args=(i,))
            t.start()
            self.workers.append(t)
        self.monitor = threading.Thread(target=self._aging_loop)
        self.monitor.start()

    def submit(self, task):
        """提交任务：仅当 task.priority==1 时才考虑触发抢占"""
        ok = self.queue.push(task)
        if ok and task.priority == 1:
            for tid, prio in list(self.current_priority.items()):
                if prio is not None and task.priority < prio:
                    print(f"[Preempt] 紧急任务 {task.id} (优先级=1) 高于线程 {tid} 当前任务，触发抢占")
                    self.preempt_event.set()
                    break
        elif not ok:
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
                # 正常执行完毕
                print(f"[Complete] 任务 {task.id} 正常完成")
                self.completed_tasks.append(task.id)
            except PreemptedException:
                print(f"[Worker {idx}] 任务 {task.id} 被抢占，重新入队")
                self.preempted_tasks.append(task.id)
                task.arrival = time.monotonic()
                self.queue.push(task)
            except Exception as e:
                print(f"[Worker {idx}] 任务 {task.id} 执行出错: {e}")
            finally:
                self.current_priority[thread_id] = None
        print(f"[Worker {idx}] 退出")

    def _aging_loop(self):
        """周期性老化提升"""
        while not self._stop_monitor.is_set():
            time.sleep(self.aging_interval)
            self.queue.promote_waiting_tasks(self.aging_interval)

    def shutdown(self):
        """优雅关闭调度器"""
        self.queue.stop()
        for t in self.workers:
            t.join()
        self._stop_monitor.set()
        self.monitor.join()

class PreemptedException(Exception):
    pass

# 示例用工作函数：支持抢占，定期检查抢占事件

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
    print("[Main] 启动调度器 (4线程)，仅 priority==1 的紧急任务可抢占")
    sched = Scheduler(num_workers=4, capacity=1024, aging_interval=5)
    print("[Main] 提交 20 个任务，ID=1~20，优先级随机(1~5)，一旦被抢占会记录并输出当前完成情况")

    # 存储任务优先级对照
    task_priorities = {}
    # 提交 20 个测试任务
    for i in range(1, 21):
        prio = random.randint(1, 5)
        task_priorities[i] = prio
        t = Task(id=i, priority=prio, work=make_preemptible_work(i, total_steps=10, step_duration=0.1))
        sched.submit(t)
        print(f"[Main] 提交任务 {i} (优先级={prio})")
        time.sleep(0.05)

    # 等待任务执行完毕
    time.sleep(20)
    sched.shutdown()

    # 输出被抢占和完成的任务情况
    if sched.preempted_tasks:
        print(f"[Test Result] 以下任务被抢占过: {sched.preempted_tasks}")
    else:
        print("[Test Result] 未检测到任何抢占。")
    print(f"[Test Result] 以下任务正常完成: {sched.completed_tasks}")
    # 输出任务优先级对照表
    print("[Task Priorities] 任务ID -> 优先级")
    for tid, prio in task_priorities.items():
        print(f"    任务 {tid}: 优先级 {prio}")