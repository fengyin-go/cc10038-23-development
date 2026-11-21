<template>
  <section class="archive-panel" data-testid="archive-delivery">
    <header class="panel-head">
      <div>
        <h3>归档交付与重放接收</h3>
        <p class="page-desc">
          把原始日志分片打成可重放归档包，逐片带批次、原始行号和内容校验；接收端核验后写入可用文件清单。
        </p>
      </div>
      <button class="btn" type="button" data-testid="archive-refresh" @click="refreshAll">刷新</button>
    </header>

    <p v-if="message" class="panel-message" data-testid="archive-message">{{ message }}</p>
    <p v-if="errorMessage" class="error-text" data-testid="archive-error">{{ errorMessage }}</p>

    <div class="archive-grid">
      <article class="archive-card">
        <h4>1. 生成可重放归档包</h4>
        <label class="field">
          <span>采集批次（可多选，逗号分隔）</span>
          <input v-model="buildBatches" data-testid="archive-build-batches" placeholder="INGE-0001, INGE-0002" />
        </label>
        <label class="field">
          <span>归档包名称</span>
          <input v-model="buildName" data-testid="archive-build-name" placeholder="留空则自动命名" />
        </label>
        <div class="row-actions">
          <button class="btn primary" type="button" data-testid="archive-build" :disabled="building" @click="build">
            {{ building ? '生成中…' : '生成归档包' }}
          </button>
        </div>
        <p class="hint">
          包内每个分片都记录批次、原始行号和正文摘要，下载文件可直接交给重放引擎接收。
        </p>
      </article>

      <article class="archive-card">
        <h4>2. 接收归档包</h4>
        <label class="field">
          <span>归档包编号</span>
          <input v-model="receiveId" data-testid="archive-receive-id" placeholder="ARC-20260101-0001" />
        </label>
        <label class="field">
          <span>上传归档文件（.ndjson）</span>
          <input type="file" accept=".ndjson,.json,.txt" data-testid="archive-upload" @change="onFileChange" />
        </label>
        <label class="field checkbox">
          <input v-model="receiveIsNew" type="checkbox" data-testid="archive-receive-new" />
          <span>作为新包接收（否则按编号补传）</span>
        </label>
        <button class="btn primary" type="button" data-testid="archive-receive" :disabled="receiving || !receiveText" @click="receive">
          {{ receiving ? '核验中…' : '上传并核验' }}
        </button>
        <p class="hint" v-if="uploadedName">已读取文件：{{ uploadedName }}（{{ uploadedLines }} 行）</p>
      </article>
    </div>

    <section v-if="current" class="archive-card" data-testid="archive-verification">
      <h4>3. 核验结果：{{ current.package_id }}</h4>
      <div class="scope-row">
        <span class="badge" :class="current.complete_package ? 'ok' : 'warn'" data-testid="archive-package-scope">
          {{ current.complete_package ? '整包核验通过' : '仅局部可用' }}
        </span>
        <span class="badge" data-testid="archive-verified-count">
          通过分片 {{ current.verification.verified_shards }} / {{ current.verification.total_shards }}
        </span>
        <span class="badge" :class="current.verification.package_integrity === '通过' ? 'ok' : 'warn'">
          文件完整性：{{ current.verification.package_integrity }}
        </span>
      </div>

      <table class="data-table">
        <thead>
          <tr>
            <th>批次</th><th>数据源</th><th>核验状态</th><th>可核验行</th><th>缺损原文区间</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="batch in current.verification.batches"
            :key="batch.batch_no"
            :data-testid="`archive-batch-${batch.batch_no}`"
            :data-status="batch.status"
          >
            <td>{{ batch.batch_no }}</td>
            <td>{{ batch.source_id }}</td>
            <td>
              <span class="badge" :class="batch.status === '通过' ? 'ok' : batch.readable ? 'warn' : 'bad'">
                {{ batch.status }}
              </span>
            </td>
            <td>{{ batch.verified_line_count }} / {{ batch.expected_line_count }}</td>
            <td>
              <span v-if="!batch.missing_ranges.length">—</span>
              <span v-else class="bad-text" data-testid="archive-missing-ranges">
                原始行 {{ batch.missing_ranges.join('、') }}
              </span>
            </td>
          </tr>
        </tbody>
      </table>

      <p v-if="current.verification.invalid.length" class="bad-text" data-testid="archive-invalid">
        校验失败分片：
        <span v-for="item in current.verification.invalid" :key="`${item.ordinal}-${item.reason}`">
          #{{ item.ordinal }}（{{ item.batch_no || item.expected_batch || '未知批次' }}
          行 {{ item.line_no || item.expected_line || '?' }}）{{ item.reason }}；
        </span>
      </p>

      <div class="row-actions">
        <button class="btn" type="button" data-testid="archive-download" @click="download(current)">
          下载归档包
        </button>
        <button class="btn" type="button" data-testid="archive-verify-again" @click="verifyAgain">
          用当前已接收内容重新核验
        </button>
      </div>
    </section>

    <section class="archive-card">
      <h4>4. 可用文件清单</h4>
      <label class="field checkbox">
        <input v-model="readableOnly" type="checkbox" data-testid="archive-readonly" @change="refreshFiles" />
        <span>只看可读批次</span>
      </label>
      <table class="data-table">
        <thead>
          <tr>
            <th>归档包</th><th>批次</th><th>文件状态</th><th>已登记行</th><th>已登记原始记录</th>
            <th>缺损区间</th><th>重放任务</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="file in files" :key="`${file.package_id}-${file.batch_no}`" :data-testid="`archive-file-${file.package_id}-${file.batch_no}`">
            <td>{{ file.package_id }}</td>
            <td>{{ file.batch_no }}</td>
            <td>
              <span class="badge" :class="file.file_status === '可用' ? 'ok' : file.readable ? 'warn' : 'bad'">
                {{ file.file_status }}
              </span>
            </td>
            <td>{{ file.registered_lines }} / {{ file.expected_line_count }}</td>
            <td class="mono">{{ file.raw_record_ids.join(', ') || '—' }}</td>
            <td>{{ file.missing_ranges.join('、') || '—' }}</td>
            <td>{{ file.replay_task_ids.join(', ') || '—' }}</td>
          </tr>
          <tr v-if="!files.length">
            <td colspan="7" class="empty-state">还没有接收过归档包</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="archive-card">
      <h4>5. 归档包列表与重放</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>归档包</th><th>批次</th><th>分片</th><th>接收状态</th><th>核验</th>
            <th>可读批次</th><th>缺损区间</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in packages" :key="item.package_id" :data-testid="`archive-package-${item.package_id}`">
            <td>{{ item.package_id }}</td>
            <td>{{ item.batch_nos.join(', ') }}</td>
            <td>{{ item.shard_count }}</td>
            <td>{{ item.status }}</td>
            <td>
              <span class="badge" :class="item.complete_package ? 'ok' : 'warn'">
                {{ item.complete_package ? '整包' : '局部' }}
              </span>
            </td>
            <td>{{ item.readable_batches.join(', ') || '—' }}</td>
            <td>
              {{ Object.entries(item.missing_ranges).map(([k, v]) => `${k}: ${v.join('、')}`).join('；') || '—' }}
            </td>
            <td class="row-actions">
              <button class="link" type="button" :data-testid="`archive-select-${item.package_id}`" @click="selectPackage(item)">
                选择
              </button>
              <button class="link" type="button" :data-testid="`archive-replay-${item.package_id}`" @click="replay(item)">
                重放
              </button>
            </td>
          </tr>
          <tr v-if="!packages.length">
            <td colspan="8" class="empty-state">还没有归档包</td>
          </tr>
        </tbody>
      </table>

      <div class="replay-form">
        <label class="field">
          <span>重放规则版本</span>
          <input v-model="replayRule" data-testid="archive-replay-rule" placeholder="例如 field-rule-v2" />
        </label>
        <label class="field">
          <span>只重放批次（可留空 = 整包）</span>
          <input v-model="replayBatches" data-testid="archive-replay-batches" placeholder="留空表示全部可读批次" />
        </label>
        <button class="btn primary" type="button" data-testid="archive-replay-run" :disabled="replaying" @click="replay(lastSelected)">
          {{ replaying ? '重放中…' : '执行重放' }}
        </button>
      </div>

      <div v-if="lastReplay" class="scope-row" data-testid="archive-replay-result">
        <span class="badge" :class="lastReplay.complete_replay ? 'ok' : 'warn'">{{ lastReplay.scope }}</span>
        <span class="badge">任务 #{{ lastReplay.replay.id }}</span>
        <span class="badge">执行批次：{{ lastReplay.executed_batches.join(', ') || '—' }}</span>
        <span class="badge" v-if="lastReplay.skipped_batches.length">跳过：{{ lastReplay.skipped_batches.join(', ') }}</span>
        <span class="badge warn" v-if="Object.keys(lastReplay.missing_ranges).length">
          缺损：{{ Object.entries(lastReplay.missing_ranges).map(([k, v]) => `${k} ${v.join('、')}`).join('；') }}
        </span>
        <span class="badge">{{ lastReplay.replayed_count }} 行</span>
      </div>
      <p v-if="lastReplay" class="hint">{{ lastReplay.message }}</p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  buildArchivePackage,
  downloadArchivePackage,
  getArchivePackage,
  listArchivePackages,
  listUsableFiles,
  receiveArchivePackage,
  replayArchivePackage,
  type ArchivePackage,
  type FileEntry,
} from '@/api/archive-service'

const packages = ref<ArchivePackage[]>([])
const files = ref<FileEntry[]>([])
const current = ref<(ArchivePackage & Record<string, unknown>) | null>(null)
const selected = ref<ArchivePackage | null>(null)
const message = ref('')
const errorMessage = ref('')
const building = ref(false)
const receiving = ref(false)
const replaying = ref(false)
const buildBatches = ref('')
const buildName = ref('')
const receiveId = ref('')
const receiveText = ref('')
const receiveIsNew = ref(false)
const uploadedName = ref('')
const uploadedLines = ref(0)
const readableOnly = ref(false)
const replayRule = ref('replay-rule-v2')
const replayBatches = ref('')
const lastReplay = ref<Awaited<ReturnType<typeof replayArchivePackage>> | null>(null)
const lastSelected = ref<ArchivePackage | null>(null)

function splitList(text: string): string[] {
  return text
    .split(/[,，\s]+/)
    .map((item) => item.trim())
    .filter(Boolean)
}

async function refreshAll() {
  errorMessage.value = ''
  try {
    const [packagePayload, filePayload] = await Promise.all([
      listArchivePackages(),
      listUsableFiles(readableOnly.value),
    ])
    packages.value = packagePayload.items
    files.value = filePayload.items
    if (selected.value) {
      const match = packages.value.find((item) => item.package_id === selected.value?.package_id)
      if (match) selected.value = match
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '归档数据读取失败'
  }
}

async function refreshFiles() {
  try {
    files.value = (await listUsableFiles(readableOnly.value)).items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '可用文件清单读取失败'
  }
}

async function build() {
  building.value = true
  errorMessage.value = ''
  message.value = ''
  try {
    const result = await buildArchivePackage(splitList(buildBatches.value), buildName.value || undefined)
    if (!result.ok) throw new Error(result.message)
    message.value = result.message
    if (result.package) {
      selected.value = result.package
      lastSelected.value = result.package
      current.value = await getArchivePackage(result.package.package_id)
    }
    await refreshAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '归档包生成失败'
  } finally {
    building.value = false
  }
}

function onFileChange(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file) return
  const reader = new FileReader()
  reader.onload = () => {
    receiveText.value = String(reader.result || '')
    uploadedName.value = file.name
    uploadedLines.value = receiveText.value.split('\n').filter((line) => line.trim()).length
    if (!receiveId.value) {
      try {
        const first = JSON.parse(receiveText.value.split('\n')[0])
        if (first?.package_id) receiveId.value = String(first.package_id)
      } catch {
        // 第一行坏了就由用户手填编号
      }
    }
  }
  reader.readAsText(file)
}

async function receive() {
  if (!receiveId.value.trim()) {
    errorMessage.value = '请填写归档包编号'
    return
  }
  receiving.value = true
  errorMessage.value = ''
  message.value = ''
  try {
    const result = await receiveArchivePackage(
      receiveId.value.trim(),
      receiveText.value,
      uploadedName.value || undefined,
    )
    message.value = result.message
    current.value = await getArchivePackage(result.package.package_id)
    selected.value = result.package
    lastSelected.value = result.package
    await refreshAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '归档包接收失败'
  } finally {
    receiving.value = false
  }
}

async function verifyAgain() {
  if (!current.value) return
  try {
    current.value = await getArchivePackage(current.value.package_id)
    message.value = '已刷新核验结果'
    await refreshAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '核验失败'
  }
}

async function selectPackage(item: ArchivePackage) {
  selected.value = item
  lastSelected.value = item
  current.value = await getArchivePackage(item.package_id)
  message.value = `已选择归档包 ${item.package_id}`
}

async function download(item: ArchivePackage) {
  errorMessage.value = ''
  try {
    await downloadArchivePackage(item.package_id, item.archive_name)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '下载失败'
  }
}

async function replay(item: ArchivePackage | null) {
  const target = item ?? selected.value
  if (!target) {
    errorMessage.value = '请先在上表选择一个归档包'
    return
  }
  if (!replayRule.value.trim()) {
    errorMessage.value = '请填写重放规则版本'
    return
  }
  replaying.value = true
  errorMessage.value = ''
  message.value = ''
  try {
    const batches = splitList(replayBatches.value)
    lastReplay.value = await replayArchivePackage(
      target.package_id,
      replayRule.value.trim(),
      batches.length ? batches : undefined,
    )
    message.value = lastReplay.value.message
    await refreshAll()
    current.value = await getArchivePackage(target.package_id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重放失败'
  } finally {
    replaying.value = false
  }
}

onMounted(refreshAll)
</script>
