import { request } from './client'

export type VerificationBatch = {
  batch_no: string
  source_id: string
  status: string
  readable: boolean
  expected_line_count: number
  verified_line_count: number
  missing_line_nos: string[]
  missing_ranges: string[]
  verified_lines: string[]
  shards: {
    shard_id: string
    line_no: string
    raw_record_id?: number | null
    body_checksum: string
    content_checksum: string
  }[]
}

export type Verification = {
  state: string
  package_integrity: string
  manifest_integrity?: string
  complete_package: boolean
  verified_shards: number
  total_shards: number
  valid_shards: number
  invalid_shards: number
  processed_shards: number
  readable_batches: string[]
  passed_batches: string[]
  missing_ranges: Record<string, string[]>
  missing_lines?: Record<string, string[]>
  batches: VerificationBatch[]
  invalid: {
    ordinal: number
    shard_id?: string
    batch_no?: string
    line_no?: string
    expected_batch?: string
    expected_line?: string
    reason: string
  }[]
  malformed_lines: { line: number; reason: string }[]
  verified_at: string
}

export type ArchivePackage = {
  package_id: string
  archive_name: string
  created_at: string
  origin: string
  status: string
  shard_count: number
  record_count: number
  batch_nos: string[]
  complete_package: boolean
  verification: Verification
  readable_batches: string[]
  missing_ranges: Record<string, string[]>
  replay_task_ids: string[]
  receipts: Record<string, unknown>[]
  download_url?: string
}

export type FileEntry = {
  package_id: string
  archive_name: string
  batch_no: string
  source_id: string
  file_status: string
  readable: boolean
  expected_line_count: number
  verified_line_count: number
  missing_ranges: string[]
  raw_record_ids: number[]
  registered_lines: number
  complete_package: boolean
  replay_task_ids: string[]
  package_status?: string
}

export type ReplayTask = {
  id: number
  '历史批次范围': string
  '新解析规则版本': string
  '重放状态': string
  '产出记录数': string
  archive_package_id: string
  scope: string
  complete_replay: boolean
  executed_batches: string[]
  skipped_batches: string[]
  missing_ranges: Record<string, string[]>
  scope_note: string
  used_line_count: number
  created_entities: number
  updated_entities: number
  requested_batches: string[]
  package_revision: string
  status: string
}

export type ReplaySource = {
  batch_no: string
  line_no: string
  raw_record_id: number
  shard_id: string
  body_checksum: string
  content_checksum: string
  body_preview: string
  parsed_entity_id: number
  '原始正文': string
  '标准化字段集': string
  '校验状态': string
}

export type PackageBuildResult = {
  ok: boolean
  message: string
  package?: ArchivePackage
  download_url?: string
}

async function jsonBody<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(path, init)
  const payload = await response.json().catch(() => ({}))
  if (!response.ok) {
    const detail = typeof payload?.detail === 'string' ? payload.detail : `接口返回 ${response.status}`
    throw new Error(detail)
  }
  return payload as T
}

export async function listArchivePackages(): Promise<{ items: ArchivePackage[]; total: number }> {
  return jsonBody('/api/archive/packages')
}

export async function buildArchivePackage(
  batchNos: string[],
  archiveName?: string,
  strategyId?: number,
): Promise<PackageBuildResult> {
  return jsonBody('/api/archive/packages', {
    method: 'POST',
    body: JSON.stringify({ batch_nos: batchNos, archive_name: archiveName, strategy_id: strategyId }),
  })
}

export async function getArchivePackage(packageId: string): Promise<ArchivePackage & Record<string, unknown>> {
  return jsonBody(`/api/archive/packages/${encodeURIComponent(packageId)}`)
}

export async function downloadArchivePackage(packageId: string, archiveName: string): Promise<void> {
  const response = await request(`/api/archive/packages/${encodeURIComponent(packageId)}/download`)
  if (!response.ok) throw new Error('归档包下载失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${archiveName || packageId}.ndjson`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function receiveArchivePackage(
  packageId: string,
  archiveText: string,
  archiveName?: string,
): Promise<{ ok: boolean; message: string; package: ArchivePackage }> {
  return jsonBody(`/api/archive/packages/${encodeURIComponent(packageId)}/receive`, {
    method: 'POST',
    body: JSON.stringify({ archive_text: archiveText, archive_name: archiveName }),
  })
}

export async function listUsableFiles(readableOnly = false): Promise<{ items: FileEntry[]; total: number }> {
  return jsonBody(`/api/archive/files?readable_only=${readableOnly ? 'true' : 'false'}`)
}

export async function replayArchivePackage(
  packageId: string,
  newParserRule: string,
  batchNos?: string[],
  validateEntities = false,
): Promise<{
  ok: boolean
  message: string
  replay: ReplayTask
  replayed_count: number
  scope: string
  complete_replay: boolean
  executed_batches: string[]
  skipped_batches: string[]
  missing_ranges: Record<string, string[]>
}> {
  return jsonBody(`/api/archive/packages/${encodeURIComponent(packageId)}/replay`, {
    method: 'POST',
    body: JSON.stringify({
      new_parser_rule: newParserRule,
      batch_nos: batchNos ?? null,
      validate_entities: validateEntities,
    }),
  })
}

export async function listArchiveReplays(): Promise<{ items: ReplayTask[]; total: number }> {
  return jsonBody('/api/archive/replays')
}

export async function replaySources(taskId: number): Promise<{
  replay_id: number
  scope: string
  complete_replay: boolean
  missing_ranges: Record<string, string[]>
  skipped_batches: string[]
  items: ReplaySource[]
  total: number
}> {
  return jsonBody(`/api/archive/replays/${taskId}/sources`)
}
