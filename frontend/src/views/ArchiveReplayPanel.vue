<template>
  <section class="archive-panel" data-testid="archive-replay-panel">
    <header class="panel-head">
      <div>
        <h3>归档重放任务</h3>
        <p class="page-desc">
          展示从可重放归档包发起的重放任务：只有通过核验的正文和原始编号会进入重放，
          局部重放会明确标出未执行的批次与缺损的原文区间。
        </p>
      </div>
      <button class="btn" type="button" data-testid="replay-refresh" @click="refresh">刷新</button>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="replay-error">{{ errorMessage }}</p>

    <table class="data-table">
      <thead>
        <tr>
          <th>任务</th><th>归档包</th><th>请求批次</th><th>重放范围</th><th>执行批次</th>
          <th>产出</th><th>缺损区间</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="task in tasks"
          :key="task.id"
          :data-testid="`replay-task-${task.id}`"
          :data-scope="task.scope"
        >
          <td>#{{ task.id }}</td>
          <td>{{ task.archive_package_id }}</td>
          <td>{{ task.requested_batches.join(', ') }}</td>
          <td>
            <span class="badge" :class="task.complete_replay ? 'ok' : 'warn'">
              {{ task.scope }}
            </span>
          </td>
          <td>{{ task.executed_batches.join(', ') || '—' }}</td>
          <td>{{ task.used_line_count }} 行</td>
          <td>
            <span v-if="!Object.keys(task.missing_ranges).length">—</span>
            <span v-else class="bad-text">
              {{ Object.entries(task.missing_ranges).map(([k, v]) => `${k}: ${v.join('、')}`).join('；') }}
            </span>
          </td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`replay-sources-${task.id}`" @click="openSources(task)">
              追溯原文
            </button>
          </td>
        </tr>
        <tr v-if="!tasks.length">
          <td colspan="8" class="empty-state">还没有从归档包发起的重放任务</td>
        </tr>
      </tbody>
    </table>

    <section v-if="sources" class="source-panel" data-testid="replay-sources">
      <h4>
        任务 #{{ sources.replay_id }} 使用过的原文
        <span class="badge" :class="sources.complete_replay ? 'ok' : 'warn'">{{ sources.scope }}</span>
        <span class="badge">{{ sources.total }} 行</span>
      </h4>
      <p v-if="Object.keys(sources.missing_ranges).length" class="bad-text" data-testid="replay-sources-missing">
        缺损区间：{{ Object.entries(sources.missing_ranges).map(([k, v]) => `${k} ${v.join('、')}`).join('；') }}
      </p>
      <p v-if="sources.skipped_batches.length" class="bad-text" data-testid="replay-sources-skipped">
        未执行批次：{{ sources.skipped_batches.join(', ') }}
      </p>
      <table class="data-table">
        <thead>
          <tr>
            <th>批次</th><th>原始行号</th><th>原始记录ID</th><th>分片</th>
            <th>原始正文</th><th>内容校验</th><th>标准化结果</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in sources.items" :key="`${item.batch_no}-${item.line_no}`">
            <td>{{ item.batch_no }}</td>
            <td>{{ item.line_no }}</td>
            <td>{{ item.raw_record_id }}</td>
            <td class="mono">{{ item.shard_id }}</td>
            <td class="mono">{{ item['原始正文'] }}</td>
            <td class="mono">{{ item.content_checksum.slice(0, 16) }}</td>
            <td class="mono">{{ item['标准化字段集'] }}</td>
          </tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  listArchiveReplays,
  replaySources,
  type ReplaySource,
  type ReplayTask,
} from '@/api/archive-service'

const tasks = ref<ReplayTask[]>([])
const sources = ref<{
  replay_id: number
  scope: string
  complete_replay: boolean
  missing_ranges: Record<string, string[]>
  skipped_batches: string[]
  items: ReplaySource[]
  total: number
} | null>(null)
const errorMessage = ref('')

async function refresh() {
  errorMessage.value = ''
  try {
    tasks.value = (await listArchiveReplays()).items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重放任务读取失败'
  }
}

async function openSources(task: ReplayTask) {
  errorMessage.value = ''
  try {
    sources.value = await replaySources(task.id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '原文追溯读取失败'
  }
}

onMounted(refresh)
</script>
