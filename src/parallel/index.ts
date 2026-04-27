/**
 * ABOUTME: ParallelExecutor — top-level coordinator for parallel task execution.
 * Analyzes task dependencies, groups independent tasks, executes them in parallel
 * git worktrees, and merges results back sequentially with conflict resolution.
 */

import { readFile, writeFile, appendFile, access, constants } from 'node:fs/promises';
import { join } from 'node:path';
import type { RalphConfig } from '../config/types.js';
import type { TrackerPlugin, TrackerTask } from '../plugins/trackers/types.js';
import type { EngineEventListener } from '../engine/types.js';
import { analyzeTaskGraph, shouldRunParallel } from './task-graph.js';
import { WorktreeManager } from './worktree-manager.js';
import { MergeEngine } from './merge-engine.js';
import { ConflictResolver, type AiResolverCallback } from './conflict-resolver.js';
import { Worker } from './worker.js';
import type {
  MergeOperation,
  ParallelExecutorConfig,
  ParallelExecutorState,
  ParallelExecutorStatus,
  TaskGraphAnalysis,
  WorktreeInfo,
  WorkerDisplayState,
  WorkerResult,
} from './types.js';
import type {
  ParallelEvent,
  ParallelEventListener,
} from './events.js';

/** Default parallel executor configuration */
const DEFAULT_PARALLEL_CONFIG: ParallelExecutorConfig = {
  maxWorkers: 3,
  worktreeDir: '.ralph-tui/worktrees',
  cwd: process.cwd(),
  maxIterationsPerWorker: 10,
  iterationDelay: 1000,
  aiConflictResolution: true,
  maxRequeueCount: 1,
};

interface PendingConflictEntry {
  operation: MergeOperation;
  workerResult: WorkerResult;
}

/**
 * Coordinates parallel execution of independent tasks using git worktrees.
 *
 * Execution flow:
 * 1. Fetch all tasks from the tracker
 * 2. Run TaskGraphAnalysis to find parallel groups
 * 3. For each group (in topological order):
 *    a. Start up to maxWorkers workers from a FIFO pending queue
 *    b. When a worker finishes, run merge/conflict post-processing immediately
 *    c. Refill the freed worker slot with the next pending task
 *    d. Continue until both pending and in-flight workers are empty
 * 4. After all groups: cleanup all worktrees, emit completion
 */
export class ParallelExecutor {
  private readonly config: ParallelExecutorConfig;
  private readonly baseConfig: RalphConfig;
  private readonly tracker: TrackerPlugin;

  private readonly worktreeManager: WorktreeManager;
  private readonly mergeEngine: MergeEngine;
  private readonly conflictResolver: ConflictResolver;

  private status: ParallelExecutorStatus = 'idle';
  private taskGraph: TaskGraphAnalysis | null = null;
  private currentGroupIndex = 0;
  private activeWorkers: Worker[] = [];
  private completedResults: WorkerResult[] = [];
  private totalTasksCompleted = 0;
  private totalTasksFailed = 0;
  private totalMergesCompleted = 0;
  private totalConflictsResolved = 0;
  private startedAt: string | null = null;
  private sessionId: string;
  private shouldStop = false;
  private paused = false;
  private statusBeforePause: ParallelExecutorStatus | null = null;
  private pauseWaiters: Array<() => void> = [];
  private returnToOriginalBranchError: string | null = null;
  private workerLaunchCount = 0;

  private readonly parallelListeners: ParallelEventListener[] = [];
  private readonly engineListeners: EngineEventListener[] = [];

  /** Track re-queue counts per task to prevent infinite loops */
  private requeueCounts = new Map<string, number>();

  /** Pending conflicts that need user-driven retry/skip actions */
  private pendingConflicts: PendingConflictEntry[] = [];

  /**
   * Worktrees intentionally preserved on cleanup for manual recovery.
   * These correspond to branches with failed or unmerged results.
   */
  private preservedRecoveryWorktrees: WorktreeInfo[] = [];

  constructor(
    baseConfig: RalphConfig,
    tracker: TrackerPlugin,
    parallelConfig?: Partial<ParallelExecutorConfig>
  ) {
    this.baseConfig = baseConfig;
    this.tracker = tracker;
    this.sessionId = baseConfig.sessionId ?? `parallel-${Date.now()}`;

    this.config = {
      ...DEFAULT_PARALLEL_CONFIG,
      cwd: baseConfig.cwd,
      maxIterationsPerWorker: baseConfig.maxIterations,
      iterationDelay: baseConfig.iterationDelay,
      ...parallelConfig,
    };

    this.worktreeManager = new WorktreeManager({
      cwd: this.config.cwd,
      worktreeDir: this.config.worktreeDir,
      maxWorktrees: this.config.maxWorkers * 2, // Buffer for re-queued tasks
    });

    this.mergeEngine = new MergeEngine(this.config.cwd);
    this.conflictResolver = new ConflictResolver(this.config.cwd);

    // Wire up merge and conflict events
    this.mergeEngine.on((event) => this.emitParallel(event));
    this.conflictResolver.on((event) => this.emitParallel(event));
  }

  /**
   * Register a parallel event listener.
   * @returns Unsubscribe function
   */
  on(listener: ParallelEventListener): () => void {
    this.parallelListeners.push(listener);
    return () => {
      const idx = this.parallelListeners.indexOf(listener);
      if (idx >= 0) this.parallelListeners.splice(idx, 1);
    };
  }

  /**
   * Register an engine event listener for forwarded worker events.
   * @returns Unsubscribe function
   */
  onEngineEvent(listener: EngineEventListener): () => void {
    this.engineListeners.push(listener);
    return () => {
      const idx = this.engineListeners.indexOf(listener);
      if (idx >= 0) this.engineListeners.splice(idx, 1);
    };
  }

  /**
   * Set the AI conflict resolver callback.
   */
  setAiResolver(resolver: AiResolverCallback): void {
    this.conflictResolver.setAiResolver(resolver);
  }

  /**
   * Retry conflict resolution for the pending failed operation.
   * Returns true if retry was initiated, false if no pending conflict.
   */
  async retryConflictResolution(): Promise<boolean> {
    const pending = this.pendingConflicts[0];
    const operation = pending?.operation;
    const workerResult = pending?.workerResult;

    if (!operation || !workerResult) {
      return false;
    }

    // Save tracker state before resolution to prevent stale worktree state from overwriting
    const savedState = await this.saveTrackerState();

    try {
      // Re-attempt resolution
      const resolutions = await this.conflictResolver.resolveConflicts(operation);
      const allResolved = resolutions.every((r) => r.success);

      if (allResolved) {
        // Success! Remove the resolved pending entry and mark task as complete.
        this.removePendingConflictByOperationId(operation.id);

        try {
          await this.tracker.completeTask(workerResult.task.id);
        } catch {
          // Log but don't fail after successful resolution
        }

        await this.mergeProgressFile(workerResult);
        this.totalConflictsResolved += resolutions.length;
        this.totalMergesCompleted++;
        this.emitNextPendingConflictIfAny();
        return true;
      }

      // Still failed - keep pending state for another retry
      return false;
    } finally {
      // Always restore tracker state to prevent stale worktree data from persisting
      await this.restoreTrackerState(savedState);
    }
  }

  /**
   * Skip the pending failed conflict and continue execution.
   * The task's merge will be abandoned (task remains incomplete).
   */
  skipFailedConflict(): void {
    const pending = this.pendingConflicts.shift();
    if (!pending) {
      return;
    }

    this.markConflictOperationRolledBack(
      pending.operation.id,
      'Skipped by user after failed conflict resolution'
    );

    // Emit an event so the TUI knows to close the conflict panel.
    this.emitParallel({
      type: 'conflict:resolved',
      timestamp: new Date().toISOString(),
      operationId: pending.operation.id,
      taskId: pending.workerResult.task.id,
      results: [],
    });

    this.emitNextPendingConflictIfAny();
  }

  /**
   * Check if there's a pending conflict operation.
   */
  hasPendingConflict(): boolean {
    return this.pendingConflicts.length > 0;
  }

  /**
   * Reset internal state so the executor can run again.
   * Call this before `execute()` when restarting after completion or stop.
   */
  reset(): void {
    this.shouldStop = false;
    this.status = 'idle';
    this.taskGraph = null;
    this.currentGroupIndex = 0;
    this.activeWorkers = [];
    this.completedResults = [];
    this.totalTasksCompleted = 0;
    this.totalTasksFailed = 0;
    this.totalMergesCompleted = 0;
    this.totalConflictsResolved = 0;
    this.startedAt = null;
    this.requeueCounts.clear();
    this.sessionId = `parallel-${Date.now()}`;
    this.paused = false;
    this.statusBeforePause = null;
    this.pauseWaiters = [];
    this.pendingConflicts = [];
    this.preservedRecoveryWorktrees = [];
    this.returnToOriginalBranchError = null;
    this.workerLaunchCount = 0;
  }

  /**
   * Analyze tasks and run parallel execution.
   * Main entry point for the parallel execution flow.
   */
  async execute(): Promise<void> {
    this.startedAt = new Date().toISOString();
    this.status = 'analyzing';

    try {
      // Fetch all tasks from the tracker
      let tasks = await this.tracker.getTasks({
        status: ['open', 'in_progress'],
      });

      // Apply task ID filter if provided (for --task-range support)
      if (this.config.filteredTaskIds && this.config.filteredTaskIds.length > 0) {
        const filteredIdSet = new Set(this.config.filteredTaskIds);
        tasks = tasks.filter((t) => filteredIdSet.has(t.id));
      }

      if (tasks.length === 0) {
        this.status = 'completed';
        return;
      }

      // Analyze task graph
      this.taskGraph = analyzeTaskGraph(tasks);

      if (!shouldRunParallel(this.taskGraph)) {
        // Fall back — this shouldn't happen if the caller checked first
        this.status = 'completed';
        return;
      }

      // Initialize session branch unless directMerge is enabled.
      // The session branch holds all worker merges, keeping the original branch clean.
      if (!this.config.directMerge) {
        const { branch, original } = this.mergeEngine.initializeSessionBranch(
          this.sessionId,
          this.config.sessionBranchName
        );

        this.emitParallel({
          type: 'parallel:session-branch-created',
          timestamp: new Date().toISOString(),
          sessionId: this.sessionId,
          sessionBranch: branch,
          originalBranch: original,
        });
      }

      // Create session backup (on the session branch if one was created)
      this.mergeEngine.createSessionBackup(this.sessionId);

      this.emitParallel({
        type: 'parallel:started',
        timestamp: this.startedAt,
        sessionId: this.sessionId,
        analysis: this.taskGraph,
        totalGroups: this.taskGraph.groups.length,
        totalTasks: this.taskGraph.actionableTaskCount,
        maxWorkers: this.config.maxWorkers,
      });

      // Execute groups in topological order
      for (let i = 0; i < this.taskGraph.groups.length; i++) {
        if (this.shouldStop) break;
        await this.waitWhilePaused();
        if (this.shouldStop) break;

        this.currentGroupIndex = i;
        const group = this.taskGraph.groups[i];

        await this.executeGroup(group, i);
      }

      const allActionableTasksCompleted =
        this.totalTasksCompleted >= this.taskGraph.actionableTaskCount &&
        this.totalTasksFailed === 0;
      this.status = this.shouldStop || !allActionableTasksCompleted
        ? 'interrupted'
        : 'completed';

      this.emitParallel({
        type: 'parallel:completed',
        timestamp: new Date().toISOString(),
        sessionId: this.sessionId,
        totalTasksCompleted: this.totalTasksCompleted,
        totalTasksFailed: this.totalTasksFailed,
        totalMergesCompleted: this.totalMergesCompleted,
        totalConflictsResolved: this.totalConflictsResolved,
        durationMs: Date.now() - new Date(this.startedAt).getTime(),
      });
    } catch (err) {
      this.status = 'failed';
      const error = err instanceof Error ? err.message : String(err);

      this.emitParallel({
        type: 'parallel:failed',
        timestamp: new Date().toISOString(),
        sessionId: this.sessionId,
        error,
        tasksCompletedBeforeFailure: this.totalTasksCompleted,
      });

      throw err;
    } finally {
      // Always cleanup
      await this.cleanup();
    }
  }

  /**
   * Stop parallel execution gracefully.
   * Stops all active workers and waits for them to finish.
   */
  async stop(): Promise<void> {
    this.shouldStop = true;
    this.paused = false;
    this.statusBeforePause = null;
    this.releasePauseWaiters();

    // Stop all active workers
    const stopPromises = this.activeWorkers.map((w) => w.stop());
    await Promise.allSettled(stopPromises);

    this.status = 'interrupted';
  }

  /**
   * Pause all active workers after their current iterations complete.
   */
  pause(): void {
    if (this.paused || this.status === 'completed' || this.status === 'failed') {
      return;
    }

    this.paused = true;
    this.statusBeforePause = this.status;
    this.status = 'paused';

    for (const worker of this.activeWorkers) {
      worker.pause();
    }
  }

  /**
   * Resume all active workers from paused state.
   */
  resume(): void {
    if (!this.paused) {
      return;
    }

    this.paused = false;
    this.status = this.statusBeforePause ?? 'executing';
    this.statusBeforePause = null;
    this.releasePauseWaiters();

    for (const worker of this.activeWorkers) {
      worker.resume();
    }
  }

  /**
   * Get the current executor state for TUI rendering.
   */
  getState(): ParallelExecutorState {
    return {
      status: this.status,
      taskGraph: this.taskGraph,
      currentGroupIndex: this.currentGroupIndex,
      totalGroups: this.taskGraph?.groups.length ?? 0,
      workers: this.activeWorkers.map((w) => w.getDisplayState()),
      mergeQueue: [...this.mergeEngine.getQueue()],
      completedMerges: [],
      activeConflicts: [],
      totalTasksCompleted: this.totalTasksCompleted,
      totalTasks: this.taskGraph?.actionableTaskCount ?? 0,
      startedAt: this.startedAt,
      elapsedMs: this.startedAt
        ? Date.now() - new Date(this.startedAt).getTime()
        : 0,
    };
  }

  /**
   * Get the session branch name (e.g., "ralph-session/a4d1aae7").
   * @returns Session branch name, or null if using directMerge mode
   */
  getSessionBranch(): string | null {
    return this.mergeEngine.getSessionBranch();
  }

  /**
   * Get the original branch name before session branch was created.
   * @returns Original branch name, or null if using directMerge mode
   */
  getOriginalBranch(): string | null {
    return this.mergeEngine.getOriginalBranch();
  }

  /**
   * Get any error encountered when trying to return to the original branch.
   */
  getReturnToOriginalBranchError(): string | null {
    return this.returnToOriginalBranchError;
  }

  /**
   * Get worktrees that were intentionally preserved for manual recovery.
   */
  getPreservedRecoveryWorktrees(): WorktreeInfo[] {
    return [...this.preservedRecoveryWorktrees];
  }

  /**
   * Get display states for all active workers.
   */
  getWorkerStates(): WorkerDisplayState[] {
    return this.activeWorkers.map((w) => w.getDisplayState());
  }

  /**
   * Execute a single parallel group.
   */
  private async executeGroup(
    group: { index: number; tasks: TrackerTask[]; depth: number },
    groupIndex: number
  ): Promise<void> {
    this.status = 'executing';
    const totalGroups = this.taskGraph!.groups.length;

    this.emitParallel({
      type: 'parallel:group-started',
      timestamp: new Date().toISOString(),
      group: { ...group, maxPriority: group.tasks[0]?.priority ?? 2 },
      groupIndex,
      totalGroups,
      workerCount: Math.min(group.tasks.length, this.config.maxWorkers),
    });

    const pendingTasks = [...group.tasks];
    const inFlightWorkers = new Map<number, Promise<{ slotIndex: number; result: WorkerResult }>>();
    let groupTasksCompleted = 0;
    let groupTasksFailed = 0;
    let groupMergesCompleted = 0;
    let groupMergesFailed = 0;

    const launchWorkerInSlot = async (slotIndex: number): Promise<boolean> => {
      await this.waitWhilePaused();
      if (this.shouldStop) {
        return false;
      }

      const nextTask = pendingTasks.shift();
      if (!nextTask) {
        return false;
      }

      const startedWorker = await this.startWorkerForTask(nextTask, slotIndex);
      inFlightWorkers.set(slotIndex, startedWorker.resultPromise);
      return true;
    };

    const initialWorkerCount = Math.min(this.config.maxWorkers, pendingTasks.length);
    for (let slotIndex = 0; slotIndex < initialWorkerCount; slotIndex++) {
      const started = await launchWorkerInSlot(slotIndex);
      if (!started) {
        break;
      }
    }

    while (inFlightWorkers.size > 0) {
      const completed = await Promise.race(inFlightWorkers.values());
      inFlightWorkers.delete(completed.slotIndex);
      this.activeWorkers = this.activeWorkers.filter((w) => w.id !== completed.result.workerId);
      this.worktreeManager.release(`worker-${completed.result.workerId}`);
      this.completedResults.push(completed.result);

      await this.handleWorkerCompletion(completed.result, pendingTasks, {
        incrementTaskCompleted: () => {
          groupTasksCompleted++;
        },
        incrementTaskFailed: () => {
          groupTasksFailed++;
        },
        incrementMergeCompleted: () => {
          groupMergesCompleted++;
        },
        incrementMergeFailed: () => {
          groupMergesFailed++;
        },
      });

      await launchWorkerInSlot(completed.slotIndex);
    }

    this.emitParallel({
      type: 'parallel:group-completed',
      timestamp: new Date().toISOString(),
      groupIndex,
      totalGroups,
      tasksCompleted: groupTasksCompleted,
      tasksFailed: groupTasksFailed,
      mergesCompleted: groupMergesCompleted,
      mergesFailed: groupMergesFailed,
    });
  }

  /**
   * Start a worker for the provided task in a fixed slot.
   */
  private async startWorkerForTask(
    task: TrackerTask,
    slotIndex: number
  ): Promise<{ resultPromise: Promise<{ slotIndex: number; result: WorkerResult }> }> {
    const workerId = `w${this.currentGroupIndex}-${slotIndex}-${this.workerLaunchCount++}`;

    const worktreeInfo = await this.worktreeManager.acquire(workerId, task.id);
    const worker = new Worker(
      {
        id: workerId,
        task,
        worktreePath: worktreeInfo.path,
        branchName: worktreeInfo.branch,
        cwd: this.config.cwd,
      },
      this.config.maxIterationsPerWorker
    );

    worker.on((event) => this.emitParallel(event));
    worker.onEngineEvent((event) => {
      for (const listener of this.engineListeners) {
        try {
          listener(event);
        } catch {
          // Don't let listener errors propagate
        }
      }
    });

    await worker.initialize(this.baseConfig, this.tracker);
    this.activeWorkers.push(worker);

    try {
      await this.tracker.updateTaskStatus(task.id, 'in_progress');
    } catch {
      // Non-fatal — tracker update may fail for some trackers
    }

    const resultPromise = worker.start().then((result) => ({ slotIndex, result }));

    return { resultPromise };
  }

  /**
   * Handle completion pipeline for an individual worker.
   */
  private async handleWorkerCompletion(
    result: WorkerResult,
    pendingTasks: TrackerTask[],
    counters: {
      incrementTaskCompleted: () => void;
      incrementTaskFailed: () => void;
      incrementMergeCompleted: () => void;
      incrementMergeFailed: () => void;
    }
  ): Promise<void> {
    if (this.shouldStop) {
      counters.incrementTaskFailed();
      this.totalTasksFailed++;
      await this.resetTaskToOpen(result.task.id);
      return;
    }

    if (!(result.success && result.taskCompleted)) {
      counters.incrementTaskFailed();
      this.totalTasksFailed++;
      await this.resetTaskToOpen(result.task.id);
      return;
    }

    this.status = 'merging';

    const savedState = await this.saveTrackerState();
    let mergeResult: Awaited<ReturnType<typeof this.mergeEngine.processNext>>;
    this.mergeEngine.enqueue(result);
    try {
      mergeResult = await this.mergeEngine.processNext();
    } finally {
      await this.restoreTrackerState(savedState);
    }

    if (mergeResult?.success) {
      try {
        await this.tracker.completeTask(result.task.id);
      } catch {
        // Log but don't fail after successful merge
      }
      await this.mergeProgressFile(result);
      this.requeueCounts.delete(result.task.id);
      counters.incrementTaskCompleted();
      this.totalTasksCompleted++;
      counters.incrementMergeCompleted();
      this.totalMergesCompleted++;
      this.status = 'executing';
      return;
    }

    if (mergeResult?.hadConflicts) {
      const operation = this.mergeEngine
        .getQueue()
        .find((op) => op.id === mergeResult.operationId);

      if (operation && this.config.aiConflictResolution) {
        if (this.shouldStop) {
          counters.incrementTaskFailed();
          this.totalTasksFailed++;
          counters.incrementMergeFailed();
          this.markConflictOperationRolledBack(
            operation.id,
            'Parallel execution stopped before conflict resolution'
          );
          await this.resetTaskToOpen(result.task.id);
          return;
        }

        const savedConflictState = await this.saveTrackerState();
        let resolutions: Awaited<ReturnType<typeof this.conflictResolver.resolveConflicts>>;
        let allResolved = false;
        try {
          resolutions = await this.conflictResolver.resolveConflicts(operation);
          allResolved = resolutions.every((r) => r.success);
        } finally {
          await this.restoreTrackerState(savedConflictState);
        }

        if (allResolved) {
          try {
            await this.tracker.completeTask(result.task.id);
          } catch {
            // Log but don't fail after successful resolution
          }
          await this.mergeProgressFile(result);
          this.requeueCounts.delete(result.task.id);
          this.totalConflictsResolved += resolutions.length;
          counters.incrementTaskCompleted();
          this.totalTasksCompleted++;
          counters.incrementMergeCompleted();
          this.totalMergesCompleted++;
          this.status = 'executing';
          return;
        }

        const requeued = await this.handleMergeFailure(result, operation);
        if (requeued) {
          this.enqueueRetryTasks(pendingTasks, [result.task]);
          this.emitParallel({
            type: 'conflict:resolved',
            timestamp: new Date().toISOString(),
            operationId: operation.id,
            taskId: result.task.id,
            results: [],
          });
          this.status = 'executing';
          return;
        }

        this.enqueuePendingConflict(operation, result);
        counters.incrementTaskFailed();
        this.totalTasksFailed++;
        counters.incrementMergeFailed();
        this.status = 'executing';
        return;
      }

      const requeued = await this.handleMergeFailure(result, operation);
      if (requeued) {
        this.enqueueRetryTasks(pendingTasks, [result.task]);
      } else {
        counters.incrementTaskFailed();
        this.totalTasksFailed++;
        counters.incrementMergeFailed();
      }
      this.status = 'executing';
      return;
    }

    const requeued = await this.handleMergeFailure(result);
    if (requeued) {
      this.enqueueRetryTasks(pendingTasks, [result.task]);
    } else {
      counters.incrementTaskFailed();
      this.totalTasksFailed++;
      counters.incrementMergeFailed();
    }
    this.status = 'executing';
  }

  /**
   * Handle a merge failure by tracking retries and resetting the task to open.
   */
  private async handleMergeFailure(
    result: WorkerResult,
    operation?: MergeOperation
  ): Promise<boolean> {
    const taskId = result.task.id;
    const currentCount = this.requeueCounts.get(taskId) ?? 0;
    const shouldRequeue = currentCount < this.config.maxRequeueCount;

    if (shouldRequeue) {
      this.requeueCounts.set(taskId, currentCount + 1);
    }

    if (operation?.status === 'conflicted') {
      this.markConflictOperationRolledBack(
        operation.id,
        'Conflict resolution failed; task reset to open'
      );
    }

    await this.mergeProgressFile(result);
    await this.resetTaskToOpen(taskId);
    return shouldRequeue;
  }

  /**
   * Best-effort reset of a task status to open.
   * Prevents tasks from remaining stuck in in_progress after cancellation/failure.
   */
  private async resetTaskToOpen(taskId: string): Promise<void> {
    try {
      await this.tracker.updateTaskStatus(taskId, 'open');
    } catch {
      // Best effort
    }
  }

  /**
   * Clean up all resources.
   */
  private async cleanup(): Promise<void> {
    const branchesToPreserve = this.getBranchesToPreserveForRecovery();
    this.preservedRecoveryWorktrees = this.worktreeManager
      .getAllWorktrees()
      .filter((info) => branchesToPreserve.has(info.branch))
      .map((info) => ({ ...info }));
    try {
      const preserved = await this.worktreeManager.cleanupAll({
        preserveBranches: branchesToPreserve,
      });
      this.preservedRecoveryWorktrees = preserved.map((info) => ({ ...info }));
    } catch {
      // Best effort cleanup
    }

    try {
      this.mergeEngine.cleanupTags();
    } catch {
      // Best effort cleanup
    }

    // Return to original branch if a session branch was created.
    // This leaves the session branch with all merged changes, but the user
    // is back on their original branch ready for next steps.
    if (!this.config.directMerge) {
      try {
        this.mergeEngine.returnToOriginalBranch();
        this.returnToOriginalBranchError = null;
      } catch (error) {
        this.returnToOriginalBranchError = error instanceof Error
          ? error.message
          : String(error);
      }
    }
  }

  private async waitWhilePaused(): Promise<void> {
    while (this.paused && !this.shouldStop) {
      await new Promise<void>((resolve) => {
        this.pauseWaiters.push(resolve);
      });
    }
  }

  private releasePauseWaiters(): void {
    const waiters = this.pauseWaiters;
    this.pauseWaiters = [];
    for (const resolve of waiters) {
      resolve();
    }
  }

  private enqueuePendingConflict(
    operation: MergeOperation,
    workerResult: WorkerResult
  ): void {
    if (this.pendingConflicts.some((entry) => entry.operation.id === operation.id)) {
      return;
    }
    this.pendingConflicts.push({ operation, workerResult });
  }

  private removePendingConflictByOperationId(operationId: string): void {
    this.pendingConflicts = this.pendingConflicts.filter(
      (entry) => entry.operation.id !== operationId
    );
  }

  private emitNextPendingConflictIfAny(): void {
    const next = this.pendingConflicts[0];
    if (!next) {
      return;
    }

    const conflictedFiles = next.operation.conflictedFiles ?? [];
    this.emitParallel({
      type: 'conflict:detected',
      timestamp: new Date().toISOString(),
      operationId: next.operation.id,
      taskId: next.workerResult.task.id,
      conflicts: conflictedFiles.map((filePath) => ({
        filePath,
        oursContent: '',
        theirsContent: '',
        baseContent: '',
        conflictMarkers: '',
      })),
    });
  }

  private enqueueRetryTasks(
    pendingTasks: TrackerTask[],
    retryTasks: TrackerTask[]
  ): void {
    if (retryTasks.length === 0) {
      return;
    }

    const existingTaskIds = new Set(pendingTasks.map((task) => task.id));
    for (const task of retryTasks) {
      if (existingTaskIds.has(task.id)) {
        continue;
      }
      pendingTasks.push(task);
      existingTaskIds.add(task.id);
    }
  }

  private markConflictOperationRolledBack(
    operationId: string,
    reason: string
  ): void {
    this.mergeEngine.markOperationRolledBack(operationId, reason);
  }

  /**
   * Determine which worker branches should be preserved for manual recovery.
   * Keep any branch that did not merge successfully and contains potentially
   * useful work (failed execution or unmerged commits).
   */
  private getBranchesToPreserveForRecovery(): Set<string> {
    const mergedBranches = new Set(
      this.mergeEngine
        .getQueue()
        .filter((operation) => operation.status === 'completed')
        .map((operation) => operation.sourceBranch)
    );

    const preserveBranches = new Set(
      this.mergeEngine
        .getQueue()
        .filter((operation) => operation.status !== 'completed')
        .map((operation) => operation.sourceBranch)
    );

    for (const result of this.completedResults) {
      if (mergedBranches.has(result.branchName)) {
        continue;
      }

      if (!result.success || result.commitCount > 0) {
        preserveBranches.add(result.branchName);
      }
    }

    return preserveBranches;
  }

  /**
   * Merge a worker's progress.md into the main progress.md.
   * This allows learnings from completed tasks to be visible to subsequent workers.
   */
  private async mergeProgressFile(workerResult: WorkerResult): Promise<void> {
    if (!workerResult.worktreePath) return;

    const workerProgressPath = join(workerResult.worktreePath, '.ralph-tui', 'progress.md');
    const mainProgressPath = join(this.config.cwd, '.ralph-tui', 'progress.md');

    try {
      // Check if worker's progress file exists
      await access(workerProgressPath, constants.R_OK);

      // Read the worker's progress content
      const workerProgress = await readFile(workerProgressPath, 'utf-8');
      if (!workerProgress.trim()) return;

      // Append to main progress file with a separator
      const separator = `\n\n---\n\n## Parallel Task: ${workerResult.task.title} (${workerResult.task.id})\n\n`;
      await appendFile(mainProgressPath, separator + workerProgress);
    } catch {
      // Silently ignore if worker progress file doesn't exist or can't be read
    }
  }

  /**
   * Save tracker state files before a merge operation.
   * Returns a map of file paths to their contents for later restoration.
   *
   * This prevents git merge from overwriting tracker state (like task completion status)
   * with stale versions from worker worktrees.
   */
  private async saveTrackerState(): Promise<Map<string, string>> {
    const savedState = new Map<string, string>();

    if (typeof this.tracker.getStateFiles !== 'function') {
      return savedState;
    }

    const stateFiles = this.tracker.getStateFiles();
    for (const filePath of stateFiles) {
      try {
        const content = await readFile(filePath, 'utf-8');
        savedState.set(filePath, content);
      } catch {
        // File may not exist yet - that's fine
      }
    }

    return savedState;
  }

  /**
   * Restore tracker state files after a merge operation.
   * This ensures tracker state (task completion status) is not overwritten
   * by stale versions from worker worktrees during git merge.
   */
  private async restoreTrackerState(savedState: Map<string, string>): Promise<void> {
    for (const [filePath, content] of savedState) {
      try {
        await writeFile(filePath, content, 'utf-8');
        // Clear tracker's cache so it re-reads the restored content
        const tracker = this.tracker as unknown as { clearCache?: () => void };
        if (typeof tracker.clearCache === 'function') {
          tracker.clearCache();
        }
      } catch {
        // Best effort - log but don't fail
      }
    }
  }

  /**
   * Emit a parallel event to all listeners.
   */
  private emitParallel(event: ParallelEvent): void {
    for (const listener of this.parallelListeners) {
      try {
        listener(event);
      } catch {
        // Don't let listener errors break the executor
      }
    }
  }
}

// Re-export key types and functions for convenient imports
export { analyzeTaskGraph, shouldRunParallel, recommendParallelism } from './task-graph.js';
export { WorktreeManager } from './worktree-manager.js';
export { MergeEngine } from './merge-engine.js';
export { ConflictResolver } from './conflict-resolver.js';
export { Worker } from './worker.js';
export type {
  ParallelExecutorConfig,
  ParallelExecutorState,
  ParallelExecutorStatus,
  TaskGraphAnalysis,
  ParallelGroup,
  WorkerResult,
  WorkerDisplayState,
  MergeResult,
  MergeOperation,
  FileConflict,
  ConflictResolutionResult,
  ParallelismRecommendation,
  ParallelismConfidence,
} from './types.js';
export type {
  ParallelEvent,
  ParallelEventType,
  ParallelEventListener,
} from './events.js';
