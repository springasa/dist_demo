#!/usr/bin/env python
#  -*- mode: python; -*-

# 1. run all tasks
# 2. log
# 3. parallel run
# 4. parallel run job number
# 5. depend

import subprocess
import os
import time
import copy
	
class Runner(object):
	def __init__(self, task):
		self.task = task
		self.returncode = None
		
	def cmd(self):
		return self.task.split()
	
	@staticmethod
	def outlog(task):
		return os.path.join('log', '%s.log' % task.replace('/', '_'))
		
	@staticmethod
	def errlog(task):
		return os.path.join('log', '[ERROR]%s.log' % task.replace('/', '_'))
		
	def start(self):
		out = open(self.outlog(self.task), 'w')
		err = open(self.errlog(self.task), 'w')
		self.process = subprocess.Popen(self.cmd(), stdout = out, stderr = err)
		out.close()
		err.close()
		self.on_start()
		return self
		
	def on_start(self):
		pass
		
	def on_stop(self, returncode):
		pass
		
	def poll(self):
		returncode = self.process.poll()
		if returncode != self.returncode:
			self.on_stop(returncode)
		self.returncode = returncode
		return returncode

	def aborted(self):
		return False
		
class RunnerPool(object):
	def __init__(self, runner = Runner, max_runner = 2):
		self.idle = [runner] * max_runner
		self.used = {}
	
	def empty(self):
		return len(self.idle) == 0
		
	def alloc_runner(self, task):
		runner = self.idle.pop()
		r = runner(task)
		self.used[r] = runner
		return r
		
	def free_runner(self, r):
		self.idle.append(self.used[r])
		del self.used[r]
		
class BatchRun(object):
	def __init__(self, runner_poll = RunnerPool()):
		self.draw = None
		self.runner_poll = runner_poll
		self.tasks = []
		self.dep = {}
		
	def set_drawer(self, drawer):
		self.draw = drawer
		
	def add(self, tasks):
		self.tasks += tasks
		
	def depend(self, task, depend):
		self.dep[task] = self.dep.get(task, []) + depend

	def idle_runner_count(self):
		return self.max_runner - len(self.running)
		
	def is_waiting(self, task):
		return 0 == len(filter(
			lambda task:
				task in self.todo
				or task in [task.task for task in self.running],
			self.dep.get(task, [])))
		
	def waiting_task_count(self):
		todo = filter(lambda task: self.is_waiting(task), self.todo)
		return len(todo)
			
	def next_task(self):
		todo = filter(lambda task: self.is_waiting(task), self.todo)
		return todo[0]

	def elapse(self):
		return time.time() - self.start_time
			
	def run(self):
		self.start_time = time.time()
		self.success = []
		self.failed = []
		self.running = []
		self.todo = copy.copy(self.tasks)
		while len(self.todo) > 0 or len(self.running) > 0:
			# dispatch task
			while not self.runner_poll.empty() and self.waiting_task_count() > 0:
				task = self.next_task()
				self.todo.remove(task)
				self.running.append(self.runner_poll.alloc_runner(task))
				self.running[-1].start()
				
			# check result
			for task in self.running:
				returncode = task.poll()
				if returncode != None:
					if returncode == 0:
						self.success.append(task.task)
					else:
						self.failed.append(task.task)
					self.running.remove(task)
					self.runner_poll.free_runner(task)
				elif task.aborted():
					self.todo.insert(0, task.task)
					self.running.remove(task)
					self.runner_poll.free_runner(task)
					
			if self.runner_poll.empty() or self.waiting_task_count() == 0:
				if self.draw:
					self.draw()
				time.sleep(1)
			
		return len(self.failed)

if __name__ == '__main__':
	if not os.path.exists('log'):
		os.mkdir('log')
	br = BatchRun()
	br.add(['sleep 1', 'sleep 2', 'sleep 3', 'sleep 4'])
	br.depend('sleep 1', ['sleep 3', 'sleep 4'])
	br.depend('sleep 2', ['sleep 3'])
	br.depend('sleep 2', ['sleep 4'])
	br.run()
