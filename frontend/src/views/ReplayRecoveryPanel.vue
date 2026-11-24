<template>
  <section class="replay-panel" data-testid="replay-panel">
    <header class="replay-head">
      <div>
        <h3>版本缺口检索与增量重放恢复</h3>
        <p class="replay-tip">
          按湖表与版本范围定位「已提交 / 已撤销 / 还缺」版本，生成增量重放区间；
          恢复时重新核对目标湖表真实提交，检查点确认到哪就从下一位重读，其他湖表同版本号不会填补本表缺口。
        </p>
      </div>
      <button class="btn ghost" type="button" data-testid="seed-scenario" @click="seedScenario">
        填入题面示例（检查点20 / 本表21、23 / 他表22）
      </button>
    </header>

    <form class="filter-bar replay-form" @submit.prevent="queryRange">
      <label class="filter-item">
        <span>目标湖表</span>
        <input v-model="table" data-testid="replay-table" placeholder="例如 target_table" />
      </label>
      <label class="filter-item narrow">
        <span>起始版本</span>
        <input v-model.number="startVersion" type="number" data-testid="replay-start" />
      </label>
      <label class="filter-item narrow">
        <span>结束版本</span>
        <input v-model.number="endVersion" type="number" data-testid="replay-end" />
      </label>
      <label class="filter-item narrow">
        <span>检查点（可选）</span>
        <input v-model.number="checkpoint" type="number" data-testid="replay-checkpoint" />
      </label>
      <button class="btn primary" type="submit" data-testid="replay-query" :disabled="loading === 'query'">
        {{ loading === 'query' ? '检索中…' : '检索重放区间' }}
      </button>
      <button class="btn" type="button" data-testid="next-offset-btn" @click="readNext">
        读检查点下一段偏移
      </button>
    </form>

    <p v-if="errorMessage" class="error-text" data-testid="replay-error">{{ errorMessage }}</p>

    <div v-if="nextInfo" class="replay-next" data-testid="next-offset-panel">
      检查点实际确认到
      <strong data-testid="next-checkpoint">{{ nextInfo.checkpoint }}</strong>
      ，下一段真正需重读的偏移为
      <strong class="accent" data-testid="next-offset">{{ nextInfo.next_offset }}</strong>
    </div>

    <div v-if="survey" class="replay-result" data-testid="survey-result">
      <div class="version-groups">
        <div class="version-group committed">
          <h4>已提交</h4>
          <p data-testid="replay-committed">{{ formatList(survey.committed) }}</p>
        </div>
        <div class="version-group revoked">
          <h4>已撤销</h4>
          <p data-testid="replay-revoked">{{ formatList(survey.revoked) }}</p>
        </div>
        <div class="version-group missing">
          <h4>还缺版本（本表）</h4>
          <p data-testid="replay-missing">{{ formatList(survey.missing) }}</p>
        </div>
        <div class="version-group range">
          <h4>查询产生的增量重放区间</h4>
          <p data-testid="replay-range">
            <template v-if="rangeVersions.length">
              {{ rangeVersions[0] }} – {{ rangeVersions[rangeVersions.length - 1] }}
              （版本 {{ formatList(rangeVersions) }}）
            </template>
            <template v-else>检查点后暂无可连续重放的区间</template>
          </p>
        </div>
      </div>

      <div class="version-chips" data-testid="version-chips">
        <span
          v-for="item in survey.versions"
          :key="item.version"
          class="chip"
          :data-state="item.state"
          :data-testid="`version-chip-${item.version}`"
        >
          v{{ item.version }} · {{ stateLabel(item.state) }}
        </span>
      </div>

      <div class="recover-actions">
        <button
          class="btn primary"
          type="button"
          data-testid="recover-button"
          :disabled="loading === 'recover'"
          @click="recover"
        >
          {{ loading === 'recover' ? '恢复核对中…' : '消费该区间并恢复' }}
        </button>
        <span class="hint">恢复会重新核对目标湖表当前真实提交，并发重复请求只确认一次。</span>
      </div>
    </div>

    <div v-if="recovery" class="recovery-result" data-testid="recovery-result">
      <h4>第一次恢复的实际结果</h4>
      <p>
        实际重放范围：
        <strong data-testid="recovery-actual-range">{{ actualRangeText }}</strong>
        <span class="hint">（请求区间 {{ recovery.requested_range?.start_version }}–{{ recovery.requested_range?.end_version }}）</span>
      </p>
      <p>
        纳入的提交数据：
        <span data-testid="recovery-included">{{ includedText }}</span>
      </p>
      <p v-if="recovery.conflicts?.length" class="conflict" data-testid="recovery-conflicts">
        冲突说明：
        <span v-for="(c, i) in recovery.conflicts" :key="i">
          版本 {{ c.version }} 查询时缺失、恢复前已补交（{{ c.commit_id }}），未纳入本次冻结区间，需重新查询后恢复；
        </span>
      </p>
      <p>
        检查点确认：<strong>{{ recovery.checkpoint }}</strong>
        ｜下一次读取位置：<strong class="accent" data-testid="recovery-next-offset">{{ recovery.next_offset }}</strong>
        ｜状态：<span data-testid="recovery-status">{{ recovery.status }}</span>
        <span v-if="recovery.already_confirmed" class="hint">（该恢复已确认过，重复请求不再确认）</span>
      </p>
      <p v-if="recovery.message" class="recovery-message" data-testid="recovery-message">{{ recovery.message }}</p>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'

import { request } from '@/api/client'

type AnyRecord = Record<string, any>

const table = ref('target_table')
const startVersion = ref<number>(21)
const endVersion = ref<number>(23)
const checkpoint = ref<number | undefined>(20)
const loading = ref<'' | 'query' | 'recover'>('')
const errorMessage = ref('')
const survey = ref<AnyRecord | null>(null)
const recovery = ref<AnyRecord | null>(null)
const nextInfo = ref<AnyRecord | null>(null)

const rangeVersions = computed<number[]>(() => survey.value?.replay_range?.versions ?? [])
const actualRangeText = computed(() => {
  const range = recovery.value?.actual_range
  const versions: number[] = range?.versions ?? []
  if (!versions.length) return '无有效版本'
  return `${range.start_version} – ${range.end_version}（版本 ${versions.join(', ')}）`
})
const includedText = computed(() => {
  const commits: AnyRecord[] = recovery.value?.included_commits ?? []
  if (!commits.length) return '无'
  return commits
    .map((item) => `${item.commit_id} 的 v${item.version} 数据（${item.row_count ?? (item.rows?.length ?? 0)} 行）`)
    .join('；')
})

function formatList(values: unknown): string {
  return Array.isArray(values) && values.length ? values.join(', ') : '无'
}

function stateLabel(state: string): string {
  if (state === 'committed') return '已提交'
  if (state === 'revoked') return '已撤销'
  if (state === 'conflict') return '冲突'
  return '还缺'
}

async function parseOrThrow(response: Response): Promise<AnyRecord> {
  const data = (await response.json()) as AnyRecord
  if (!response.ok || data.ok === false) {
    throw new Error(String(data.message ?? data.detail ?? `接口返回 ${response.status}`))
  }
  return data
}

async function seedScenario(): Promise<void> {
  errorMessage.value = ''
  try {
    const response = await request('/api/delta_commit/seed-scenario', {
      method: 'POST',
      body: JSON.stringify({
        target_table: table.value || 'target_table',
        checkpoint: checkpoint.value ?? 20,
      }),
    })
    const data = await parseOrThrow(response)
    table.value = String(data.target_table ?? table.value)
    checkpoint.value = Number(data.checkpoint ?? 20)
    startVersion.value = 21
    endVersion.value = 23
    survey.value = null
    recovery.value = null
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '构造示例失败'
  }
}

function queryString(): string {
  const params = new URLSearchParams({ table: table.value })
  if (startVersion.value !== undefined && startVersion.value !== null) {
    params.set('start_version', String(startVersion.value))
  }
  if (endVersion.value !== undefined && endVersion.value !== null) {
    params.set('end_version', String(endVersion.value))
  }
  if (checkpoint.value !== undefined && checkpoint.value !== null) {
    params.set('checkpoint', String(checkpoint.value))
  }
  return params.toString()
}

async function queryRange(): Promise<void> {
  loading.value = 'query'
  errorMessage.value = ''
  recovery.value = null
  try {
    const response = await request(`/api/delta_commit/replay-range?${queryString()}`)
    survey.value = await parseOrThrow(response)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重放区间查询失败'
  } finally {
    loading.value = ''
  }
}

async function recover(): Promise<void> {
  if (!survey.value) return
  loading.value = 'recover'
  errorMessage.value = ''
  try {
    const response = await request('/api/delta_commit/recover', {
      method: 'POST',
      body: JSON.stringify({ query_id: survey.value.query_id }),
    })
    recovery.value = await parseOrThrow(response)
    nextInfo.value = {
      checkpoint: recovery.value.checkpoint,
      next_offset: recovery.value.next_offset,
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复失败'
  } finally {
    loading.value = ''
  }
}

async function readNext(): Promise<void> {
  errorMessage.value = ''
  try {
    const params = new URLSearchParams({ table: table.value })
    const response = await request(`/api/delta_commit/next-offset?${params.toString()}`)
    nextInfo.value = await parseOrThrow(response)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '读取检查点偏移失败'
  }
}
</script>

<style scoped>
.replay-panel {
  margin: 16px 0;
  padding: 16px;
  border: 1px solid var(--border-color, #e3e8ef);
  border-radius: 12px;
  background: var(--card-bg, #fff);
}
.replay-head {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: flex-start;
  flex-wrap: wrap;
}
.replay-head h3 {
  margin: 0 0 6px;
}
.replay-tip {
  margin: 0;
  color: #5b6675;
  font-size: 13px;
  line-height: 1.7;
  max-width: 760px;
}
.replay-form {
  margin-top: 12px;
  align-items: flex-end;
  flex-wrap: wrap;
}
.filter-item.narrow {
  max-width: 150px;
}
.replay-next {
  margin-top: 12px;
  padding: 10px 12px;
  border-radius: 8px;
  background: #eef6ff;
  font-size: 14px;
}
.accent {
  color: #1a66ff;
  font-size: 16px;
}
.version-groups {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 10px;
  margin-top: 12px;
}
.version-group {
  padding: 10px 12px;
  border-radius: 8px;
  background: #f6f8fb;
}
.version-group h4 {
  margin: 0 0 6px;
  font-size: 13px;
  color: #5b6675;
}
.version-group p {
  margin: 0;
  font-weight: 600;
}
.version-group.committed p {
  color: #1a8f4c;
}
.version-group.revoked p {
  color: #b06a00;
}
.version-group.missing p {
  color: #d3382f;
}
.version-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 12px;
}
.chip {
  padding: 3px 10px;
  border-radius: 999px;
  font-size: 12px;
  border: 1px solid #d5dce6;
}
.chip[data-state='committed'] {
  background: #e8f7ee;
  border-color: #9bd8b4;
  color: #1a8f4c;
}
.chip[data-state='revoked'] {
  background: #fdf3e2;
  border-color: #e6c489;
  color: #b06a00;
}
.chip[data-state='missing'] {
  background: #fdeceb;
  border-color: #efa7a2;
  color: #d3382f;
}
.recover-actions {
  margin-top: 14px;
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}
.hint {
  color: #8a94a6;
  font-size: 12px;
  font-weight: 400;
}
.recovery-result {
  margin-top: 14px;
  padding: 12px 14px;
  border-left: 3px solid #1a66ff;
  background: #f5f9ff;
  border-radius: 0 8px 8px 0;
  font-size: 14px;
  line-height: 1.8;
}
.recovery-result h4 {
  margin: 0 0 6px;
}
.recovery-result p {
  margin: 4px 0;
}
.conflict {
  color: #b06a00;
}
.recovery-message {
  color: #44506a;
}
.error-text {
  color: #d3382f;
}
</style>
