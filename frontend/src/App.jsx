import { useEffect, useMemo, useRef, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import './App.css'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'

const PROVIDERS = [
  { value: 'postgresql', label: 'PostgreSQL', defaultPort: '5432' },
  { value: 'mysql', label: 'MySQL', defaultPort: '3306' },
  { value: 'sqlserver', label: 'SQL Server', defaultPort: '1433' },
]

const STEP_CONFIG = [
  {
    key: 'connection',
    title: '1. Database Connection',
    subtitle: 'Secure handshake established with production instance.',
  },
  {
    key: 'schema',
    title: '2. Schema Extraction',
    subtitle: 'Retrieving table metadata and relational constraints.',
  },
  {
    key: 'profiling',
    title: '3. Data Profiling',
    subtitle: 'Analyzing column quality and freshness patterns.',
  },
  {
    key: 'doc',
    title: '4. Business Document Generation',
    subtitle: 'Synthesizing technical metadata into natural language context.',
  },
]

const NAV_ITEMS = [
  { key: 'overview', label: 'Overview' },
  { key: 'dictionary', label: 'Data Dictionary' },
  { key: 'profiling', label: 'Data Profiling' },
  { key: 'ai_analysis', label: 'DB AI Analysis' },
  { key: 'schema_viz', label: 'Schema Visualization' },
]

const SENSITIVE_KEYWORDS = [
  'email',
  'phone',
  'mobile',
  'address',
  'password',
  'secret',
  'token',
  'ssn',
  'tax',
  'birth',
  'dob',
  'card',
  'iban',
  'upi',
]

const EXPORT_UNAVAILABLE_MESSAGE =
  'Export unavailable until schema, profiling, and document are loaded.'

function IconDatabase() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <ellipse cx="12" cy="5" rx="7.5" ry="3.2" />
      <path d="M4.5 5v6c0 1.8 3.4 3.2 7.5 3.2s7.5-1.4 7.5-3.2V5" />
      <path d="M4.5 11v6c0 1.8 3.4 3.2 7.5 3.2s7.5-1.4 7.5-3.2v-6" />
    </svg>
  )
}

function IconServer() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <rect x="4" y="4" width="16" height="7" rx="1.6" />
      <rect x="4" y="13" width="16" height="7" rx="1.6" />
      <circle cx="8" cy="7.6" r="1" />
      <circle cx="8" cy="16.6" r="1" />
    </svg>
  )
}

function IconUser() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="12" cy="8" r="3.2" />
      <path d="M5.2 19c1.5-2.9 3.8-4.3 6.8-4.3s5.3 1.4 6.8 4.3" />
    </svg>
  )
}

function IconLock() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <rect x="5" y="10" width="14" height="10" rx="2" />
      <path d="M8 10V8a4 4 0 1 1 8 0v2" />
    </svg>
  )
}

function IconRadar() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="12" cy="12" r="8" />
      <circle cx="12" cy="12" r="4" />
      <circle cx="12" cy="12" r="1.2" />
      <path d="M12 12l6 6" />
    </svg>
  )
}

function IconShield() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 3l7 3v6c0 4.5-2.8 7.5-7 9-4.2-1.5-7-4.5-7-9V6l7-3z" />
      <path d="M9 12l2 2 4-4" />
    </svg>
  )
}

function IconGrid() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <rect x="4" y="4" width="6" height="6" rx="1" />
      <rect x="14" y="4" width="6" height="6" rx="1" />
      <rect x="4" y="14" width="6" height="6" rx="1" />
      <rect x="14" y="14" width="6" height="6" rx="1" />
    </svg>
  )
}

function IconBranch() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M8 5a2 2 0 1 0 0.001 0z" />
      <path d="M16 7a2 2 0 1 0 0.001 0z" />
      <path d="M16 17a2 2 0 1 0 0.001 0z" />
      <path d="M8 7v10a2 2 0 0 0 2 2h4" />
      <path d="M10 7h4" />
    </svg>
  )
}

function IconTeam() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="8" cy="9" r="2.5" />
      <circle cx="16" cy="9" r="2.5" />
      <path d="M3.5 19c1.1-2.4 2.8-3.7 4.5-3.7S11.4 16.6 12.5 19" />
      <path d="M11.5 19c1.1-2.4 2.8-3.7 4.5-3.7s3.4 1.3 4.5 3.7" />
    </svg>
  )
}

function IconCog() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="12" cy="12" r="3" />
      <path d="M19.4 15a1 1 0 0 0 .2 1.1l.1.1a1 1 0 0 1 0 1.4l-1.1 1.1a1 1 0 0 1-1.4 0l-.1-.1a1 1 0 0 0-1.1-.2 1 1 0 0 0-.6.9v.2a1 1 0 0 1-1 1h-1.6a1 1 0 0 1-1-1V20a1 1 0 0 0-.6-.9 1 1 0 0 0-1.1.2l-.1.1a1 1 0 0 1-1.4 0l-1.1-1.1a1 1 0 0 1 0-1.4l.1-.1a1 1 0 0 0 .2-1.1 1 1 0 0 0-.9-.6H4.4a1 1 0 0 1-1-1v-1.6a1 1 0 0 1 1-1H4.6a1 1 0 0 0 .9-.6 1 1 0 0 0-.2-1.1l-.1-.1a1 1 0 0 1 0-1.4l1.1-1.1a1 1 0 0 1 1.4 0l.1.1a1 1 0 0 0 1.1.2h.1a1 1 0 0 0 .5-.9V4.4a1 1 0 0 1 1-1h1.6a1 1 0 0 1 1 1v.2a1 1 0 0 0 .6.9 1 1 0 0 0 1.1-.2l.1-.1a1 1 0 0 1 1.4 0l1.1 1.1a1 1 0 0 1 0 1.4l-.1.1a1 1 0 0 0-.2 1.1v.1a1 1 0 0 0 .9.5h.2a1 1 0 0 1 1 1v1.6a1 1 0 0 1-1 1h-.2a1 1 0 0 0-.9.6z" />
    </svg>
  )
}

function IconBell() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 4a5 5 0 0 0-5 5v2.8L5 15.2h14l-2-3.4V9a5 5 0 0 0-5-5z" />
      <path d="M10 18a2 2 0 0 0 4 0" />
    </svg>
  )
}

function IconSearch() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="11" cy="11" r="6" />
      <path d="M20 20l-4.2-4.2" />
    </svg>
  )
}

function IconDownload() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 5v10" />
      <path d="M8.5 11.5L12 15l3.5-3.5" />
      <path d="M5 19h14" />
    </svg>
  )
}

function getNavIcon(key) {
  if (key === 'overview') return <IconGrid />
  if (key === 'dictionary') return <IconDatabase />
  if (key === 'profiling') return <IconRadar />
  if (key === 'ai_analysis') return <IconShield />
  if (key === 'schema_viz') return <IconBranch />
  return <IconTeam />
}

function asNumber(value, fallback = 0) {
  const num = Number(value)
  return Number.isFinite(num) ? num : fallback
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value))
}

function slugifyDatabaseName(name) {
  const normalized = String(name || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/-{2,}/g, '-')
    .replace(/^-|-$/g, '')

  return normalized || 'database'
}

function buildTimestampForFilename(date = new Date()) {
  const pad = (value) => String(value).padStart(2, '0')
  const year = date.getFullYear()
  const month = pad(date.getMonth() + 1)
  const day = pad(date.getDate())
  const hours = pad(date.getHours())
  const minutes = pad(date.getMinutes())
  const seconds = pad(date.getSeconds())
  return `${year}-${month}-${day}_${hours}${minutes}${seconds}`
}

function downloadBlob(filename, mimeType, content) {
  if (typeof Blob === 'undefined' || typeof URL === 'undefined' || !URL.createObjectURL) {
    throw new Error('This browser cannot generate downloads for exported reports.')
  }

  const blob = new Blob([content], { type: mimeType })
  const objectUrl = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = objectUrl
  anchor.download = filename
  try {
    document.body.appendChild(anchor)
    anchor.click()
    anchor.remove()
  } finally {
    URL.revokeObjectURL(objectUrl)
  }
}

function escapeMarkdownCell(value) {
  return String(value ?? '')
    .replace(/\|/g, '\\|')
    .replace(/\n/g, ' ')
}

function createChatMessage(role, text) {
  const id =
    typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
      ? crypto.randomUUID()
      : `${Date.now()}-${Math.random().toString(16).slice(2)}`

  return {
    id,
    role,
    text: String(text || ''),
    createdAt: new Date().toISOString(),
  }
}

function formatCompactNumber(value) {
  const number = asNumber(value)
  return new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 }).format(
    number
  )
}

function formatPercent(value) {
  const number = asNumber(value)
  return `${number.toFixed(1)}%`
}

function formatRelativeTime(isoValue) {
  if (!isoValue) return 'No timestamp'

  const date = new Date(isoValue)
  if (Number.isNaN(date.getTime())) return 'Unknown'

  const diffMs = date.getTime() - Date.now()
  const isFuture = diffMs > 0
  const absMs = Math.abs(diffMs)

  const minutes = Math.floor(absMs / 60000)
  const hours = Math.floor(absMs / 3600000)
  const days = Math.floor(absMs / 86400000)

  if (minutes < 1) return 'just now'
  if (minutes < 60) return `${minutes}m ${isFuture ? 'from now' : 'ago'}`
  if (hours < 24) return `${hours}h ${isFuture ? 'from now' : 'ago'}`
  return `${days}d ${isFuture ? 'from now' : 'ago'}`
}

function computeTableStatus(quality) {
  if (quality >= 90) return 'Active'
  if (quality >= 70) return 'Review'
  return 'Deprecated'
}

function computeAvailabilityStatus(item) {
  if (item?.has_schema && item?.has_profiling && item?.has_doc) return 'ready'
  if (item?.has_schema || item?.has_profiling || item?.has_doc) return 'partial'
  return 'pending'
}

function buildDashboardData(database, schemaData, profilingData, docData) {
  const schemaTables = Array.isArray(schemaData?.schema) ? schemaData.schema : []
  const profileTables = Array.isArray(profilingData?.profile) ? profilingData.profile : []
  const docTables = Array.isArray(docData?.tables) ? docData.tables : []

  const profileMap = new Map(
    profileTables
      .filter((item) => item?.table_name)
      .map((item) => [String(item.table_name).toLowerCase(), item])
  )

  const totalTables = schemaTables.length
  const totalColumns = schemaTables.reduce(
    (total, table) => total + (Array.isArray(table?.columns) ? table.columns.length : 0),
    0
  )

  const completenessScores = profileTables
    .map((table) => asNumber(table?.completeness?.table_completeness_pct, NaN))
    .filter((value) => Number.isFinite(value))

  const avgCompleteness = completenessScores.length
    ? completenessScores.reduce((sum, value) => sum + value, 0) / completenessScores.length
    : 0

  const unhealthyTables = profileTables.reduce(
    (count, table) => (table?.key_health?.status === 'healthy' ? count : count + 1),
    0
  )

  const qualityScore = Math.max(0, Math.min(100, avgCompleteness - unhealthyTables * 4))

  const sensitiveFields = schemaTables.reduce((count, table) => {
    if (!Array.isArray(table?.columns)) return count
    return (
      count +
      table.columns.filter((column) => {
        const name = String(column?.name || '').toLowerCase()
        return SENSITIVE_KEYWORDS.some((keyword) => name.includes(keyword))
      }).length
    )
  }, 0)

  const tableExplorer = schemaTables
    .map((table) => {
      const tableName = String(table?.table_name || 'unknown')
      const profiling = profileMap.get(tableName.toLowerCase())
      const rowCount = asNumber(profiling?.completeness?.row_count)
      const completeness = asNumber(profiling?.completeness?.table_completeness_pct)
      const healthPenalty = profiling?.key_health?.status === 'healthy' ? 0 : 15
      const quality = Math.max(0, Math.min(100, completeness - healthPenalty))

      return {
        name: tableName,
        schema: database.toLowerCase(),
        rows: rowCount,
        lastUpdated: formatRelativeTime(profiling?.freshness?.latest_timestamp),
        quality,
        status: computeTableStatus(quality),
      }
    })
    .sort((left, right) => right.rows - left.rows)

  const topPriorities = docTables.filter((table) => table?.priority === 'high').length

  return {
    database,
    totalTables,
    totalColumns,
    qualityScore,
    sensitiveFields,
    welcomeSummary:
      docData?.overview?.summary ||
      'Analysis is complete. Use this dashboard to inspect quality and table readiness.',
    recommendations: Array.isArray(docData?.overview?.global_recommendations)
      ? docData.overview.global_recommendations.slice(0, 2)
      : [],
    tableExplorer,
    topPriorities,
  }
}

function buildProfilingInsights(profileRows) {
  const rows = Array.isArray(profileRows) ? profileRows : []
  const totalTables = rows.length

  const completenessValues = rows
    .map((item) => asNumber(item?.completeness?.table_completeness_pct, NaN))
    .filter((value) => Number.isFinite(value))

  const averageCompleteness = completenessValues.length
    ? completenessValues.reduce((sum, value) => sum + value, 0) / completenessValues.length
    : 0

  let healthyTables = 0
  const freshnessBuckets = {
    fresh: 0,
    monitor: 0,
    stale: 0,
    noTimestamp: 0,
  }

  const tableQuality = []
  const nullRiskColumns = []

  rows.forEach((item) => {
    const tableName = String(item?.table_name || 'unknown')
    const completeness = asNumber(item?.completeness?.table_completeness_pct)
    const keyHealthy = item?.key_health?.status === 'healthy'
    if (keyHealthy) healthyTables += 1

    const stalenessDays = item?.freshness?.staleness_days
    if (stalenessDays === null || stalenessDays === undefined) {
      freshnessBuckets.noTimestamp += 1
    } else if (asNumber(stalenessDays) <= 7) {
      freshnessBuckets.fresh += 1
    } else if (asNumber(stalenessDays) <= 30) {
      freshnessBuckets.monitor += 1
    } else {
      freshnessBuckets.stale += 1
    }

    const penalty = keyHealthy ? 0 : 12
    tableQuality.push({
      table_name: tableName,
      score: Math.max(0, Math.min(100, completeness - penalty)),
      completeness,
      keyHealthy,
    })

    const columns = Array.isArray(item?.completeness?.columns) ? item.completeness.columns : []
    columns.forEach((column) => {
      const nullCount = asNumber(column?.null_count)
      const completenessPct = asNumber(column?.completeness_pct, NaN)
      if (!nullCount || !Number.isFinite(completenessPct) || completenessPct >= 100) return

      nullRiskColumns.push({
        table_name: tableName,
        column_name: String(column?.column || 'unknown'),
        null_pct: Math.max(0, 100 - completenessPct),
      })
    })
  })

  const healthiestPct = totalTables ? (healthyTables / totalTables) * 100 : 0
  const qualityLeaders = tableQuality.sort((a, b) => b.score - a.score).slice(0, 8)
  const nullRiskLeaders = nullRiskColumns.sort((a, b) => b.null_pct - a.null_pct).slice(0, 8)

  return {
    totalTables,
    averageCompleteness,
    healthyTables,
    warningTables: Math.max(0, totalTables - healthyTables),
    healthiestPct,
    freshnessBuckets,
    qualityLeaders,
    nullRiskLeaders,
  }
}

function buildDocInsights(docPayload) {
  const doc = docPayload && typeof docPayload === 'object' ? docPayload : {}
  const tables = Array.isArray(doc.tables) ? doc.tables : []
  const recommendations = Array.isArray(doc?.overview?.global_recommendations)
    ? doc.overview.global_recommendations
    : []

  const priorityCount = {
    high: 0,
    medium: 0,
    low: 0,
    unknown: 0,
  }

  tables.forEach((table) => {
    const value = String(table?.priority || '').toLowerCase()
    if (value === 'high' || value === 'medium' || value === 'low') {
      priorityCount[value] += 1
    } else {
      priorityCount.unknown += 1
    }
  })

  return {
    summary: String(doc?.overview?.summary || ''),
    generatedAt: String(doc?.generated_at || ''),
    model: String(doc?.model || ''),
    recommendations,
    tables,
    priorityCount,
  }
}

function buildSchemaVisualization(schemaRows) {
  const rows = Array.isArray(schemaRows) ? schemaRows : []
  const tableMap = new Map(
    rows.map((table) => [String(table?.table_name || '').toLowerCase(), table])
  )

  const nodes = rows.map((table) => ({
    id: String(table?.table_name || ''),
    columnCount: Array.isArray(table?.columns) ? table.columns.length : 0,
    pkCount: Array.isArray(table?.primary_keys) ? table.primary_keys.length : 0,
    fkCount: Array.isArray(table?.foreign_keys) ? table.foreign_keys.length : 0,
  }))

  const edges = []
  const seen = new Set()

  rows.forEach((table) => {
    const source = String(table?.table_name || '')
    const foreignKeys = Array.isArray(table?.foreign_keys) ? table.foreign_keys : []

    foreignKeys.forEach((foreignKey) => {
      const referredRaw = String(foreignKey?.referred_table || '')
      const targetTable =
        tableMap.get(referredRaw.toLowerCase())?.table_name || referredRaw
      if (!targetTable) return

      const localColumns = Array.isArray(foreignKey?.column) ? foreignKey.column : []
      const referredColumns = Array.isArray(foreignKey?.referred_columns)
        ? foreignKey.referred_columns
        : []
      const signature = `${source}|${targetTable}|${localColumns.join(',')}|${referredColumns.join(',')}`
      if (seen.has(signature)) return
      seen.add(signature)

      edges.push({
        source,
        target: targetTable,
        localColumns,
        referredColumns,
      })
    })
  })

  const width = 1080
  const height = 680
  const centerX = width / 2
  const centerY = height / 2
  const total = Math.max(nodes.length, 1)
  const radius = Math.min(280, 160 + total * 7)

  const positionedNodes = nodes.map((node, index) => {
    const angle = (Math.PI * 2 * index) / total - Math.PI / 2
    const wave = Math.sin(index * 1.7) * 26
    return {
      ...node,
      x: centerX + (radius + wave) * Math.cos(angle),
      y: centerY + (radius + wave) * Math.sin(angle),
    }
  })

  const nodeMap = new Map(positionedNodes.map((node) => [node.id, node]))
  const positionedEdges = edges
    .map((edge) => {
      const sourceNode = nodeMap.get(edge.source)
      const targetNode = nodeMap.get(edge.target)
      if (!sourceNode || !targetNode) return null
      return { ...edge, sourceNode, targetNode }
    })
    .filter(Boolean)

  const relationStrength = new Map()
  positionedEdges.forEach((edge) => {
    relationStrength.set(
      edge.source,
      (relationStrength.get(edge.source) || 0) + 1
    )
    relationStrength.set(
      edge.target,
      (relationStrength.get(edge.target) || 0) + 1
    )
  })

  const leaders = [...positionedNodes]
    .map((node) => ({
      ...node,
      relationCount: relationStrength.get(node.id) || 0,
    }))
    .sort((a, b) => b.relationCount - a.relationCount)
    .slice(0, 8)

  return {
    width,
    height,
    nodes: positionedNodes,
    edges: positionedEdges,
    leaders,
  }
}

function App() {
  const [screen, setScreen] = useState('databases')
  const [form, setForm] = useState({
    db_type: '',
    host: '',
    port: '',
    database: '',
    username: '',
    password: '',
  })
  const [showPassword, setShowPassword] = useState(false)
  const [status, setStatus] = useState({ type: '', message: '' })
  const [loadingAction, setLoadingAction] = useState('')
  const [stepState, setStepState] = useState({
    connection: { status: 'pending', message: STEP_CONFIG[0].subtitle },
    schema: { status: 'pending', message: STEP_CONFIG[1].subtitle },
    profiling: { status: 'pending', message: STEP_CONFIG[2].subtitle },
    doc: { status: 'pending', message: STEP_CONFIG[3].subtitle },
  })
  const [analysisStats, setAnalysisStats] = useState({
    tables: 0,
    columns: 0,
    relations: 0,
  })
  const [activeDatabase, setActiveDatabase] = useState('')
  const [dashboardData, setDashboardData] = useState({
    database: '',
    totalTables: 0,
    totalColumns: 0,
    qualityScore: 0,
    sensitiveFields: 0,
    welcomeSummary: '',
    recommendations: [],
    tableExplorer: [],
    topPriorities: 0,
  })
  const [savedDatabases, setSavedDatabases] = useState([])
  const [savedDatabasesLoading, setSavedDatabasesLoading] = useState(false)
  const [savedDatabasesError, setSavedDatabasesError] = useState('')
  const [savedDatabasesNotice, setSavedDatabasesNotice] = useState({
    type: '',
    message: '',
  })
  const [activeTab, setActiveTab] = useState('overview')
  const [schemaDictionary, setSchemaDictionary] = useState([])
  const [dictionaryQuery, setDictionaryQuery] = useState('')
  const [selectedTableName, setSelectedTableName] = useState('')
  const [profilingSnapshot, setProfilingSnapshot] = useState([])
  const [docSnapshot, setDocSnapshot] = useState({})
  const [docSearch, setDocSearch] = useState('')
  const [selectedVizTable, setSelectedVizTable] = useState('')
  const [vizViewport, setVizViewport] = useState({ scale: 1, tx: 0, ty: 0 })
  const [isVizDragging, setIsVizDragging] = useState(false)
  const [showExportMenu, setShowExportMenu] = useState(false)
  const [isExporting, setIsExporting] = useState(false)
  const [exportError, setExportError] = useState('')
  const [chatSessionId, setChatSessionId] = useState('')
  const [chatInput, setChatInput] = useState('')
  const [chatMessages, setChatMessages] = useState([])
  const [chatLoading, setChatLoading] = useState(false)
  const [chatError, setChatError] = useState('')
  const vizDragRef = useRef({
    active: false,
    startX: 0,
    startY: 0,
    startTx: 0,
    startTy: 0,
  })
  const exportMenuRef = useRef(null)
  const chatThreadRef = useRef(null)

  const providerMap = useMemo(
    () =>
      PROVIDERS.reduce((acc, provider) => {
        acc[provider.value] = provider
        return acc
      }, {}),
    []
  )

  const activeTabLabel = useMemo(
    () => NAV_ITEMS.find((item) => item.key === activeTab)?.label || 'Overview',
    [activeTab]
  )

  const dictionaryTables = useMemo(() => {
    const normalizedQuery = dictionaryQuery.trim().toLowerCase()
    if (!normalizedQuery) return schemaDictionary

    return schemaDictionary.filter((table) =>
      String(table?.table_name || '').toLowerCase().includes(normalizedQuery)
    )
  }, [schemaDictionary, dictionaryQuery])

  const selectedDictionaryTable = useMemo(() => {
    if (!dictionaryTables.length) return null
    const selectedTable = dictionaryTables.find(
      (table) => table?.table_name === selectedTableName
    )
    return selectedTable || dictionaryTables[0]
  }, [dictionaryTables, selectedTableName])

  const profilingInsights = useMemo(
    () => buildProfilingInsights(profilingSnapshot),
    [profilingSnapshot]
  )

  const docInsights = useMemo(() => buildDocInsights(docSnapshot), [docSnapshot])

  const filteredDocTables = useMemo(() => {
    const query = docSearch.trim().toLowerCase()
    if (!query) return docInsights.tables

    return docInsights.tables.filter((table) =>
      String(table?.table_name || '').toLowerCase().includes(query)
    )
  }, [docInsights.tables, docSearch])

  const schemaViz = useMemo(
    () => buildSchemaVisualization(schemaDictionary),
    [schemaDictionary]
  )

  const selectedVizNode = useMemo(() => {
    if (!schemaViz.nodes.length) return null
    return (
      schemaViz.nodes.find((node) => node.id === selectedVizTable) ||
      schemaViz.nodes[0]
    )
  }, [schemaViz.nodes, selectedVizTable])

  const selectedVizOutgoing = useMemo(() => {
    if (!selectedVizNode) return []
    return schemaViz.edges.filter((edge) => edge.source === selectedVizNode.id)
  }, [schemaViz.edges, selectedVizNode])

  const selectedVizIncoming = useMemo(() => {
    if (!selectedVizNode) return []
    return schemaViz.edges.filter((edge) => edge.target === selectedVizNode.id)
  }, [schemaViz.edges, selectedVizNode])

  const isExportReady = useMemo(() => {
    const hasDocSnapshot =
      docSnapshot && typeof docSnapshot === 'object' && Object.keys(docSnapshot).length > 0
    return (
      Boolean(String(activeDatabase || '').trim()) &&
      schemaDictionary.length > 0 &&
      profilingSnapshot.length > 0 &&
      hasDocSnapshot
    )
  }, [activeDatabase, schemaDictionary, profilingSnapshot, docSnapshot])

  const loadSavedDatabases = async () => {
    setSavedDatabasesLoading(true)
    setSavedDatabasesError('')
    try {
      const response = await fetch(`${API_BASE_URL}/databases`)
      const data = await response.json().catch(() => ({}))
      if (!response.ok) {
        throw new Error(
          typeof data.detail === 'string' ? data.detail : 'Failed to load databases.'
        )
      }
      setSavedDatabases(Array.isArray(data.databases) ? data.databases : [])
    } catch (error) {
      setSavedDatabasesError(
        error instanceof Error
          ? error.message
          : 'Failed to load saved databases from API.'
      )
    } finally {
      setSavedDatabasesLoading(false)
    }
  }

  useEffect(() => {
    loadSavedDatabases()
  }, [])

  useEffect(() => {
    if (!dictionaryTables.length) {
      if (selectedTableName) setSelectedTableName('')
      return
    }

    const tableStillExists = dictionaryTables.some(
      (table) => table?.table_name === selectedTableName
    )
    if (!tableStillExists) {
      setSelectedTableName(dictionaryTables[0]?.table_name || '')
    }
  }, [dictionaryTables, selectedTableName])

  useEffect(() => {
    if (!schemaViz.nodes.length) {
      if (selectedVizTable) setSelectedVizTable('')
      return
    }

    const tableStillExists = schemaViz.nodes.some(
      (node) => node.id === selectedVizTable
    )
    if (!tableStillExists) {
      setSelectedVizTable(schemaViz.nodes[0]?.id || '')
    }
  }, [schemaViz.nodes, selectedVizTable])

  useEffect(() => {
    setVizViewport({ scale: 1, tx: 0, ty: 0 })
    setIsVizDragging(false)
    vizDragRef.current.active = false
  }, [schemaViz.nodes.length, activeDatabase])

  useEffect(() => {
    if (screen !== 'dashboard') {
      setShowExportMenu(false)
    }
  }, [screen])

  useEffect(() => {
    if (!showExportMenu) return undefined

    const handleOutsideClick = (event) => {
      if (exportMenuRef.current && !exportMenuRef.current.contains(event.target)) {
        setShowExportMenu(false)
      }
    }

    const handleEscape = (event) => {
      if (event.key === 'Escape') {
        setShowExportMenu(false)
      }
    }

    document.addEventListener('mousedown', handleOutsideClick)
    document.addEventListener('keydown', handleEscape)

    return () => {
      document.removeEventListener('mousedown', handleOutsideClick)
      document.removeEventListener('keydown', handleEscape)
    }
  }, [showExportMenu])

  useEffect(() => {
    if (!isExportReady) {
      setShowExportMenu(false)
    }
  }, [isExportReady])

  useEffect(() => {
    if (screen !== 'dashboard' || !activeDatabase) return

    setChatSessionId('')
    setChatInput('')
    setChatError('')
    setChatMessages([
      createChatMessage(
        'assistant',
        `Connected to ${activeDatabase}. I answer database-related questions only and can use read-only SQL when needed.`
      ),
    ])
  }, [screen, activeDatabase])

  useEffect(() => {
    if (!chatThreadRef.current) return
    chatThreadRef.current.scrollTop = chatThreadRef.current.scrollHeight
  }, [chatMessages, chatLoading])

  const buildExportJsonPayload = () => {
    const database = String(activeDatabase || dashboardData.database || '').trim()
    const databaseSlug = slugifyDatabaseName(database)

    return {
      metadata: {
        project_name: 'DataLens',
        database,
        database_slug: databaseSlug,
        exported_at: new Date().toISOString(),
        report_version: '1.0.0',
        sources_included: ['schema', 'profiling', 'doc'],
      },
      schema: schemaDictionary,
      profiling: profilingSnapshot,
      doc: docSnapshot,
    }
  }

  const buildExportMarkdown = (payload) => {
    const metadata = payload?.metadata || {}
    const schemaRows = Array.isArray(payload?.schema) ? payload.schema : []
    const profilingRows = Array.isArray(payload?.profiling) ? payload.profiling : []
    const doc = payload?.doc && typeof payload.doc === 'object' ? payload.doc : {}
    const docTables = Array.isArray(doc.tables) ? doc.tables : []
    const docOverview = doc?.overview && typeof doc.overview === 'object' ? doc.overview : {}
    const recommendations = Array.isArray(docOverview.global_recommendations)
      ? docOverview.global_recommendations
      : []

    const totalTables = schemaRows.length
    const totalColumns = schemaRows.reduce(
      (sum, table) => sum + (Array.isArray(table?.columns) ? table.columns.length : 0),
      0
    )
    const totalRelations = schemaRows.reduce(
      (sum, table) => sum + (Array.isArray(table?.foreign_keys) ? table.foreign_keys.length : 0),
      0
    )

    const schemaSummaryRows = schemaRows
      .map((table) => ({
        table_name: String(table?.table_name || 'unknown'),
        column_count: Array.isArray(table?.columns) ? table.columns.length : 0,
        pk_count: Array.isArray(table?.primary_keys) ? table.primary_keys.length : 0,
        fk_count: Array.isArray(table?.foreign_keys) ? table.foreign_keys.length : 0,
      }))
      .sort((left, right) => left.table_name.localeCompare(right.table_name))

    const completenessValues = profilingRows
      .map((row) => asNumber(row?.completeness?.table_completeness_pct, NaN))
      .filter((value) => Number.isFinite(value))
    const averageCompleteness = completenessValues.length
      ? completenessValues.reduce((sum, value) => sum + value, 0) / completenessValues.length
      : 0

    let healthyKeys = 0
    const freshness = {
      fresh: 0,
      monitor: 0,
      stale: 0,
      noTimestamp: 0,
    }
    const nullRiskColumns = []

    profilingRows.forEach((row) => {
      if (row?.key_health?.status === 'healthy') {
        healthyKeys += 1
      }

      const stalenessDays = row?.freshness?.staleness_days
      const stalenessNumber = asNumber(stalenessDays, NaN)
      if (stalenessDays === null || stalenessDays === undefined || Number.isNaN(stalenessNumber)) {
        freshness.noTimestamp += 1
      } else if (stalenessNumber <= 7) {
        freshness.fresh += 1
      } else if (stalenessNumber <= 30) {
        freshness.monitor += 1
      } else {
        freshness.stale += 1
      }

      const columns = Array.isArray(row?.completeness?.columns) ? row.completeness.columns : []
      columns.forEach((column) => {
        const nullCount = asNumber(column?.null_count)
        const completenessPct = asNumber(column?.completeness_pct, NaN)
        if (!nullCount || !Number.isFinite(completenessPct) || completenessPct >= 100) return

        nullRiskColumns.push({
          table_name: String(row?.table_name || 'unknown'),
          column_name: String(column?.column || 'unknown'),
          null_count: nullCount,
          null_pct: Math.max(0, 100 - completenessPct),
        })
      })
    })

    const reviewKeys = Math.max(0, profilingRows.length - healthyKeys)
    const topNullRiskColumns = nullRiskColumns
      .sort((left, right) => right.null_pct - left.null_pct)
      .slice(0, 10)

    const maxTableInsights = 100
    const includedTableInsights = docTables.slice(0, maxTableInsights)
    const omittedTableCount = Math.max(0, docTables.length - includedTableInsights.length)

    const lines = []
    lines.push(`# DataLens Database Report: ${metadata.database || 'Unknown Database'}`)
    lines.push('')
    lines.push('## 1. Metadata')
    lines.push(`- Project: ${metadata.project_name || 'DataLens'}`)
    lines.push(`- Database: ${metadata.database || 'Unknown'}`)
    lines.push(`- Database Slug: ${metadata.database_slug || 'database'}`)
    lines.push(`- Exported At (ISO): ${metadata.exported_at || new Date().toISOString()}`)
    lines.push(`- Report Version: ${metadata.report_version || '1.0.0'}`)
    lines.push(
      `- Sources Included: ${(Array.isArray(metadata.sources_included) ? metadata.sources_included : []).join(', ')}`
    )
    lines.push('')
    lines.push('## 2. Executive Summary')
    lines.push(String(docOverview.summary || 'No executive summary available.'))
    lines.push('')
    lines.push('## 3. Dataset Overview')
    lines.push(`- Total Tables: ${totalTables.toLocaleString()}`)
    lines.push(`- Total Columns: ${totalColumns.toLocaleString()}`)
    lines.push(`- Total Relations: ${totalRelations.toLocaleString()}`)
    lines.push('')
    lines.push('## 4. Schema Summary')
    lines.push('| Table | Columns | PK Count | FK Count |')
    lines.push('| --- | ---: | ---: | ---: |')
    if (schemaSummaryRows.length) {
      schemaSummaryRows.forEach((row) => {
        lines.push(
          `| ${escapeMarkdownCell(row.table_name)} | ${row.column_count} | ${row.pk_count} | ${row.fk_count} |`
        )
      })
    } else {
      lines.push('| _No schema tables available_ | 0 | 0 | 0 |')
    }
    lines.push('')
    lines.push('## 5. Profiling Highlights')
    lines.push(`- Average Completeness: ${formatPercent(averageCompleteness)}`)
    lines.push(
      `- Key Health Split: Healthy ${healthyKeys.toLocaleString()} | Needs Review ${reviewKeys.toLocaleString()}`
    )
    lines.push(
      `- Freshness Split: Fresh ${freshness.fresh.toLocaleString()} | Monitor ${freshness.monitor.toLocaleString()} | Stale ${freshness.stale.toLocaleString()} | No Timestamp ${freshness.noTimestamp.toLocaleString()}`
    )
    lines.push('')
    lines.push('### Top Null-Risk Columns')
    lines.push('| Table | Column | Null % | Null Count |')
    lines.push('| --- | --- | ---: | ---: |')
    if (topNullRiskColumns.length) {
      topNullRiskColumns.forEach((column) => {
        lines.push(
          `| ${escapeMarkdownCell(column.table_name)} | ${escapeMarkdownCell(column.column_name)} | ${column.null_pct.toFixed(2)}% | ${column.null_count.toLocaleString()} |`
        )
      })
    } else {
      lines.push('| _No null-risk columns identified_ | - | 0% | 0 |')
    }
    lines.push('')
    lines.push('## 6. AI Recommendations')
    if (recommendations.length) {
      recommendations.forEach((recommendation) => {
        lines.push(`- ${String(recommendation)}`)
      })
    } else {
      lines.push('- No recommendations available.')
    }
    lines.push('')
    lines.push('## 7. Table AI Insights')
    if (includedTableInsights.length) {
      includedTableInsights.forEach((table, index) => {
        const usage = Array.isArray(table?.usage_recommendations)
          ? table.usage_recommendations
          : []
        const quality = Array.isArray(table?.data_quality_observations)
          ? table.data_quality_observations
          : []
        const kpis = Array.isArray(table?.suggested_kpis) ? table.suggested_kpis : []

        lines.push(`### ${index + 1}. ${String(table?.table_name || 'Unknown Table')}`)
        lines.push(`- Business Summary: ${String(table?.business_summary || 'No summary available.')}`)
        lines.push('- Usage Recommendations:')
        if (usage.length) {
          usage.forEach((item) => lines.push(`  - ${String(item)}`))
        } else {
          lines.push('  - None provided.')
        }
        lines.push('- Quality Observations:')
        if (quality.length) {
          quality.forEach((item) => lines.push(`  - ${String(item)}`))
        } else {
          lines.push('  - None provided.')
        }
        lines.push('- Suggested KPIs:')
        if (kpis.length) {
          kpis.forEach((item) => lines.push(`  - ${String(item)}`))
        } else {
          lines.push('  - None provided.')
        }
        lines.push('')
      })
    } else {
      lines.push('No table-level AI insights available.')
      lines.push('')
    }
    if (omittedTableCount > 0) {
      lines.push(`Note: additional tables omitted (${omittedTableCount.toLocaleString()}).`)
      lines.push('')
    }

    return lines.join('\n')
  }

  const handleExport = async (format) => {
    setExportError('')
    setShowExportMenu(false)

    if (!isExportReady) {
      setExportError(EXPORT_UNAVAILABLE_MESSAGE)
      return
    }

    setIsExporting(true)

    try {
      const payload = buildExportJsonPayload()
      const databaseSlug = payload?.metadata?.database_slug || 'database'
      const timestamp = buildTimestampForFilename()
      const extension = format === 'markdown' ? 'md' : 'json'
      const filename = `${databaseSlug}_report_${timestamp}.${extension}`

      if (format === 'json') {
        downloadBlob(filename, 'application/json;charset=utf-8', JSON.stringify(payload, null, 2))
      } else {
        const markdown = buildExportMarkdown(payload)
        downloadBlob(filename, 'text/markdown;charset=utf-8', markdown)
      }
    } catch (error) {
      setExportError(
        error instanceof Error
          ? error.message
          : 'Failed to export report. Please try again.'
      )
    } finally {
      setIsExporting(false)
    }
  }

  const sendChatMessage = async () => {
    const message = chatInput.trim()
    if (!message || chatLoading) return

    if (!activeDatabase) {
      setChatError('No active database selected for chat.')
      return
    }

    setChatError('')
    setChatInput('')
    setChatMessages((current) => [...current, createChatMessage('user', message)])
    setChatLoading(true)

    try {
      const response = await fetch(`${API_BASE_URL}/chat/message`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          database: activeDatabase,
          message,
          session_id: chatSessionId || undefined,
          user_id: 'datalens-ui',
        }),
      })
      const data = await response.json().catch(() => ({}))
      if (!response.ok) {
        throw new Error(
          typeof data.detail === 'string' ? data.detail : 'Failed to get agent response.'
        )
      }

      if (typeof data.session_id === 'string' && data.session_id.trim()) {
        setChatSessionId(data.session_id)
      }

      const reply =
        typeof data.reply === 'string' && data.reply.trim()
          ? data.reply.trim()
          : 'I could not generate a response. Please try rephrasing your question.'

      setChatMessages((current) => [...current, createChatMessage('assistant', reply)])
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : 'Failed to connect to chat service.'
      setChatError(errorMessage)
      setChatMessages((current) => [
        ...current,
        createChatMessage(
          'assistant',
          `I could not process that request. ${errorMessage}`
        ),
      ])
    } finally {
      setChatLoading(false)
    }
  }

  const zoomSchemaViz = (factor, point = null) => {
    setVizViewport((current) => {
      const nextScale = clamp(current.scale * factor, 0.45, 2.8)
      if (nextScale === current.scale) return current

      const pivot = point || {
        x: schemaViz.width / 2,
        y: schemaViz.height / 2,
      }

      return {
        scale: nextScale,
        tx: pivot.x - ((pivot.x - current.tx) / current.scale) * nextScale,
        ty: pivot.y - ((pivot.y - current.ty) / current.scale) * nextScale,
      }
    })
  }

  const resetSchemaVizView = () => {
    setVizViewport({ scale: 1, tx: 0, ty: 0 })
  }

  const handleSchemaVizWheel = (event) => {
    event.preventDefault()
    const bounds = event.currentTarget.getBoundingClientRect()
    const pivot = {
      x: ((event.clientX - bounds.left) / bounds.width) * schemaViz.width,
      y: ((event.clientY - bounds.top) / bounds.height) * schemaViz.height,
    }
    zoomSchemaViz(event.deltaY < 0 ? 1.12 : 0.9, pivot)
  }

  const handleSchemaVizPointerDown = (event) => {
    if (event.pointerType === 'mouse' && event.button !== 0) return
    if (event.target.closest('.schema-node')) return

    vizDragRef.current = {
      active: true,
      startX: event.clientX,
      startY: event.clientY,
      startTx: vizViewport.tx,
      startTy: vizViewport.ty,
    }
    setIsVizDragging(true)
    event.currentTarget.setPointerCapture(event.pointerId)
  }

  const handleSchemaVizPointerMove = (event) => {
    if (!vizDragRef.current.active) return
    const bounds = event.currentTarget.getBoundingClientRect()
    const dx = ((event.clientX - vizDragRef.current.startX) / bounds.width) * schemaViz.width
    const dy =
      ((event.clientY - vizDragRef.current.startY) / bounds.height) * schemaViz.height

    setVizViewport((current) => ({
      ...current,
      tx: vizDragRef.current.startTx + dx,
      ty: vizDragRef.current.startTy + dy,
    }))
  }

  const stopSchemaVizDrag = (event) => {
    if (!vizDragRef.current.active) return
    vizDragRef.current.active = false
    setIsVizDragging(false)
    if (event?.currentTarget?.releasePointerCapture) {
      try {
        event.currentTarget.releasePointerCapture(event.pointerId)
      } catch {
        // Ignore release errors from stale pointer ids.
      }
    }
  }

  const openSavedDashboard = async (databaseName) => {
    setLoadingAction(`open:${databaseName}`)
    setSavedDatabasesError('')
    setSavedDatabasesNotice({ type: '', message: '' })
    setExportError('')
    setShowExportMenu(false)

    try {
      const response = await fetch(`${API_BASE_URL}/databases/overview`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ database: databaseName }),
      })
      const data = await response.json().catch(() => ({}))
      if (!response.ok) {
        throw new Error(
          typeof data.detail === 'string'
            ? data.detail
            : 'Failed to load saved database overview.'
        )
      }

      const schemaData = { schema: Array.isArray(data.schema) ? data.schema : [] }
      const profilingData = { profile: Array.isArray(data.profile) ? data.profile : [] }
      const docData = data.doc || {}

      const tables = schemaData.schema
      const columns = tables.reduce(
        (total, table) => total + (Array.isArray(table?.columns) ? table.columns.length : 0),
        0
      )
      const relations = tables.reduce(
        (total, table) => total + (Array.isArray(table?.foreign_keys) ? table.foreign_keys.length : 0),
        0
      )

      setActiveDatabase(databaseName)
      setAnalysisStats({ tables: tables.length, columns, relations })
      setSchemaDictionary(tables)
      setProfilingSnapshot(profilingData.profile)
      setDocSnapshot(docData)
      setSelectedTableName(tables[0]?.table_name || '')
      setSelectedVizTable(tables[0]?.table_name || '')
      setDictionaryQuery('')
      setDocSearch('')
      setActiveTab('overview')
      setDashboardData(buildDashboardData(databaseName, schemaData, profilingData, docData))
      setScreen('dashboard')
    } catch (error) {
      setSavedDatabasesError(
        error instanceof Error ? error.message : 'Unable to open saved dashboard.'
      )
    } finally {
      setLoadingAction('')
    }
  }

  const deleteSavedDatabase = async (databaseName) => {
    const confirmed = window.confirm(
      `Delete "${databaseName}" and all files in its data folder? This cannot be undone.`
    )
    if (!confirmed) return

    setLoadingAction(`delete:${databaseName}`)
    setSavedDatabasesError('')
    setSavedDatabasesNotice({ type: '', message: '' })

    try {
      const response = await fetch(
        `${API_BASE_URL}/databases/${encodeURIComponent(databaseName)}`,
        {
          method: 'DELETE',
        }
      )
      const data = await response.json().catch(() => ({}))
      if (!response.ok) {
        throw new Error(
          typeof data.detail === 'string'
            ? data.detail
            : 'Failed to delete database files.'
        )
      }

      setSavedDatabasesNotice({
        type: 'success',
        message: `Deleted database folder: ${data.database_slug || databaseName}`,
      })
      await loadSavedDatabases()
    } catch (error) {
      setSavedDatabasesNotice({
        type: 'error',
        message:
          error instanceof Error
            ? error.message
            : 'Unable to delete database files.',
      })
    } finally {
      setLoadingAction('')
    }
  }

  const updateField = (field, value) => {
    setForm((current) => ({ ...current, [field]: value }))
  }

  const handleProviderChange = (value) => {
    const defaultPort = providerMap[value]?.defaultPort || ''
    setForm((current) => ({
      ...current,
      db_type: value,
      port: current.port || defaultPort,
    }))
  }

  const updateStep = (key, nextStatus, message) => {
    setStepState((current) => ({
      ...current,
      [key]: {
        ...current[key],
        status: nextStatus,
        message: message || current[key].message,
      },
    }))
  }

  const resetProgress = () => {
    setStepState({
      connection: { status: 'pending', message: STEP_CONFIG[0].subtitle },
      schema: { status: 'pending', message: STEP_CONFIG[1].subtitle },
      profiling: { status: 'pending', message: STEP_CONFIG[2].subtitle },
      doc: { status: 'pending', message: STEP_CONFIG[3].subtitle },
    })
    setAnalysisStats({ tables: 0, columns: 0, relations: 0 })
  }

  const validateForm = () => {
    const missingField = Object.entries(form).find(([, value]) => !String(value).trim())

    if (missingField) {
      setStatus({
        type: 'error',
        message: 'Please fill all fields before continuing.',
      })
      return null
    }

    const parsedPort = Number(form.port)
    if (!Number.isInteger(parsedPort) || parsedPort <= 0) {
      setStatus({
        type: 'error',
        message: 'Port must be a valid positive number.',
      })
      return null
    }

    return {
      db_type: form.db_type.trim(),
      host: form.host.trim(),
      port: parsedPort,
      database: form.database.trim(),
      username: form.username.trim(),
      password: form.password,
    }
  }

  const saveCredentials = async (payload) => {
    const response = await fetch(`${API_BASE_URL}/databases/credentials`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    })

    const data = await response.json().catch(() => ({}))
    if (!response.ok) {
      const detail =
        typeof data.detail === 'string' ? data.detail : 'Failed to save database credentials.'
      throw new Error(detail)
    }

    return data
  }

  const submitCredentials = async (action) => {
    const payload = validateForm()
    if (!payload) return

    setLoadingAction(action)
    setStatus({ type: '', message: '' })

    try {
      const data = await saveCredentials(payload)

      const actionText =
        action === 'test'
          ? 'Connection details validated and saved.'
          : 'Database credentials saved. Ready for analysis.'

      setStatus({
        type: 'success',
        message: `${actionText} Folder: ${data.database_slug}`,
      })
    } catch (error) {
      setStatus({
        type: 'error',
        message:
          error instanceof Error ? error.message : 'Failed to connect to API. Check backend URL.',
      })
    } finally {
      setLoadingAction('')
    }
  }

  const runAnalysis = async () => {
    const payload = validateForm()
    if (!payload) return

    setLoadingAction('start')
    setStatus({ type: '', message: '' })
    setExportError('')
    setShowExportMenu(false)
    resetProgress()
    setActiveDatabase(payload.database)
    setScreen('analysis')

    let currentStep = 'connection'

    try {
      updateStep('connection', 'active')
      await saveCredentials(payload)
      updateStep('connection', 'success', 'Secure handshake established and credentials stored.')

      currentStep = 'schema'
      updateStep('schema', 'active')
      const schemaResponse = await fetch(`${API_BASE_URL}/databases/schema/extract`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ database: payload.database }),
      })
      const schemaData = await schemaResponse.json().catch(() => ({}))
      if (!schemaResponse.ok) {
        throw new Error(
          typeof schemaData.detail === 'string' ? schemaData.detail : 'Schema extraction failed.'
        )
      }

      const tables = Array.isArray(schemaData.schema) ? schemaData.schema : []
      const columns = tables.reduce(
        (total, table) => total + (Array.isArray(table.columns) ? table.columns.length : 0),
        0
      )
      const relations = tables.reduce(
        (total, table) => total + (Array.isArray(table.foreign_keys) ? table.foreign_keys.length : 0),
        0
      )
      setAnalysisStats({ tables: tables.length, columns, relations })
      updateStep(
        'schema',
        'success',
        `Retrieved metadata for ${tables.length} tables and relational constraints.`
      )

      currentStep = 'profiling'
      updateStep('profiling', 'active')
      const profilingResponse = await fetch(`${API_BASE_URL}/databases/profiling/extract`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ database: payload.database }),
      })
      const profilingData = await profilingResponse.json().catch(() => ({}))
      if (!profilingResponse.ok) {
        throw new Error(
          typeof profilingData.detail === 'string' ? profilingData.detail : 'Data profiling failed.'
        )
      }
      updateStep(
        'profiling',
        'success',
        `Profiled ${profilingData.tables_profiled || 0} tables for quality and freshness.`
      )

      currentStep = 'doc'
      updateStep('doc', 'active')
      const docResponse = await fetch(`${API_BASE_URL}/databases/doc/generate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ database: payload.database }),
      })
      const docData = await docResponse.json().catch(() => ({}))
      if (!docResponse.ok) {
        throw new Error(
          typeof docData.detail === 'string'
            ? docData.detail
            : 'Business document generation failed.'
        )
      }

      updateStep(
        'doc',
        'success',
        `Generated business document for ${docData.tables?.length || 0} documented tables.`
      )
      setStatus({
        type: 'success',
        message: 'Analysis pipeline completed successfully.',
      })

      setSchemaDictionary(tables)
      setProfilingSnapshot(
        Array.isArray(profilingData.profile) ? profilingData.profile : []
      )
      setDocSnapshot(docData)
      setSelectedTableName(tables[0]?.table_name || '')
      setSelectedVizTable(tables[0]?.table_name || '')
      setDictionaryQuery('')
      setDocSearch('')
      setActiveTab('overview')
      setDashboardData(buildDashboardData(payload.database, schemaData, profilingData, docData))
      loadSavedDatabases()
      setScreen('dashboard')
    } catch (error) {
      updateStep(
        currentStep,
        'error',
        error instanceof Error ? error.message : 'Analysis step failed.'
      )
      setStatus({
        type: 'error',
        message: error instanceof Error ? error.message : 'Analysis failed. Please try again.',
      })
    } finally {
      setLoadingAction('')
    }
  }

  const stepBadgeLabel = (value) => {
    if (value === 'success') return 'SUCCESS'
    if (value === 'active') return 'ACTIVE'
    if (value === 'error') return 'FAILED'
    return 'PENDING'
  }

  const renderStepIcon = (value) => {
    if (value === 'success') return <span className="timeline-icon-glyph">✓</span>
    if (value === 'active') return <span className="timeline-spinner" aria-hidden="true" />
    if (value === 'error') return <span className="timeline-icon-glyph">!</span>
    return <span className="timeline-icon-glyph">·</span>
  }

  if (screen === 'databases') {
    return (
      <main className="db-list-page">
        <div className="ambient ambient-top" />
        <div className="ambient ambient-bottom" />

        <section className="db-list-shell">
          <header className="db-list-header">
            <div>
              <p className="db-list-kicker">DataLens Workspace</p>
              <h1>Select A Database</h1>
              <p>
                Choose an existing database workspace to open the overview dashboard,
                or add a new database connection.
              </p>
            </div>
            <button
              type="button"
              className="primary-btn"
              onClick={() => {
                setStatus({ type: '', message: '' })
                setScreen('connect')
              }}
            >
              Add New Database
              <span className="btn-arrow">→</span>
            </button>
          </header>

          {savedDatabasesError ? <p className="db-list-error">{savedDatabasesError}</p> : null}
          {savedDatabasesNotice.message ? (
            <p className={`db-list-notice ${savedDatabasesNotice.type}`}>
              {savedDatabasesNotice.message}
            </p>
          ) : null}

          <div className="db-list-grid">
            {savedDatabasesLoading ? (
              <p className="db-list-empty">Loading databases...</p>
            ) : savedDatabases.length ? (
              savedDatabases.map((item) => {
                const availability = computeAvailabilityStatus(item)
                const isOpening = loadingAction === `open:${item.database}`
                const isDeleting = loadingAction === `delete:${item.database}`
                const readyToOpen = availability === 'ready'
                return (
                  <article key={item.database_slug} className="db-card">
                    <header>
                      <div className="db-card-title">
                        <span className="db-card-icon">
                          <IconDatabase />
                        </span>
                        <div>
                          <h2>{item.database}</h2>
                          <p>
                            {item.db_type || 'unknown'} • {item.host || 'host not set'}
                          </p>
                        </div>
                      </div>
                      <span className={`availability-pill ${availability}`}>
                        {availability.toUpperCase()}
                      </span>
                    </header>

                    <div className="db-card-stats">
                      <div>
                        <span>Tables</span>
                        <strong>{(item.tables_found || 0).toLocaleString()}</strong>
                      </div>
                      <div>
                        <span>Profiled</span>
                        <strong>{(item.tables_profiled || 0).toLocaleString()}</strong>
                      </div>
                    </div>

                    <div className="db-card-flags">
                      <span className={item.has_schema ? 'yes' : 'no'}>Schema</span>
                      <span className={item.has_profiling ? 'yes' : 'no'}>Profiling</span>
                      <span className={item.has_doc ? 'yes' : 'no'}>Doc</span>
                    </div>

                    <div className="db-card-actions">
                      <button
                        type="button"
                        className="secondary-btn db-open-btn"
                        onClick={() => openSavedDashboard(item.database)}
                        disabled={!readyToOpen || Boolean(loadingAction)}
                      >
                        {isOpening
                          ? 'Opening...'
                          : readyToOpen
                            ? 'Open Dashboard'
                            : 'Run Analysis First'}
                      </button>
                      <button
                        type="button"
                        className="secondary-btn danger-btn"
                        onClick={() => deleteSavedDatabase(item.database)}
                        disabled={Boolean(loadingAction)}
                      >
                        {isDeleting ? 'Deleting...' : 'Delete'}
                      </button>
                    </div>
                  </article>
                )
              })
            ) : (
              <p className="db-list-empty">
                No saved databases found. Add your first database connection.
              </p>
            )}
          </div>
        </section>
      </main>
    )
  }

  if (screen === 'dashboard') {
    return (
      <main className="dashboard-page">
        <aside className="dashboard-sidebar">
          <div className="sidebar-brand">
            <div className="sidebar-logo">
              <IconGrid />
            </div>
            <p className="sidebar-title">DataLens</p>
          </div>

          <nav className="sidebar-nav" aria-label="Dashboard Navigation">
            {NAV_ITEMS.map((item) => (
              <button
                type="button"
                key={item.key}
                className={`nav-item ${item.key === activeTab ? 'active' : ''}`}
                onClick={() => {
                  if (
                    item.key === 'overview' ||
                    item.key === 'dictionary' ||
                    item.key === 'profiling' ||
                    item.key === 'ai_analysis' ||
                    item.key === 'schema_viz'
                  ) {
                    setActiveTab(item.key)
                  }
                }}
                disabled={
                  item.key !== 'overview' &&
                  item.key !== 'dictionary' &&
                  item.key !== 'profiling' &&
                  item.key !== 'ai_analysis' &&
                  item.key !== 'schema_viz'
                }
              >
                <span className="nav-icon">{getNavIcon(item.key)}</span>
                {item.label}
              </button>
            ))}
          </nav>

          <div className="sidebar-footer">
            <div className="sidebar-user">
              <span className="user-avatar">DB</span>
              <div>
                <p>{dashboardData.database || activeDatabase}</p>
                <span>Read-only Mode</span>
              </div>
            </div>
            <button
              type="button"
              className="secondary-btn reset-btn"
              onClick={() => {
                loadSavedDatabases()
                setActiveTab('overview')
                setScreen('databases')
              }}
            >
              Switch Database
            </button>
          </div>
        </aside>

        <section className="dashboard-main">
          <header className="dashboard-topbar">
            <div className="topbar-crumbs">
              <span>Home</span>
              <span>/</span>
              <strong>Dashboard</strong>
              <span>/</span>
              <strong>{activeTabLabel}</strong>
            </div>
            <div className="topbar-right">
              <div className="export-shell" ref={exportMenuRef}>
                <button
                  type="button"
                  className="topbar-export-btn"
                  onClick={() => {
                    setExportError('')
                    setShowExportMenu((current) => !current)
                  }}
                  disabled={isExporting || !isExportReady}
                  title={isExportReady ? 'Export report' : EXPORT_UNAVAILABLE_MESSAGE}
                >
                  <span className="nav-icon">
                    <IconDownload />
                  </span>
                  {isExporting ? 'Exporting...' : 'Export'}
                </button>

                {showExportMenu && isExportReady ? (
                  <div className="export-menu" role="menu" aria-label="Export format">
                    <button
                      type="button"
                      role="menuitem"
                      className="export-menu-item"
                      onClick={() => handleExport('json')}
                    >
                      Export as JSON
                    </button>
                    <button
                      type="button"
                      role="menuitem"
                      className="export-menu-item"
                      onClick={() => handleExport('markdown')}
                    >
                      Export as Markdown
                    </button>
                  </div>
                ) : null}
              </div>
              {exportError ? (
                <p className="export-error">{exportError}</p>
              ) : !isExportReady ? (
                <p className="export-error">{EXPORT_UNAVAILABLE_MESSAGE}</p>
              ) : null}
            </div>
          </header>

          <div className="dashboard-content">
            {activeTab === 'overview' ? (
              <>
                <section className="welcome-panel">
                  <h1>Welcome back, Analyst</h1>
                  <p>{dashboardData.welcomeSummary}</p>
                  {dashboardData.recommendations.length ? (
                    <ul className="recommendation-list">
                      {dashboardData.recommendations.map((recommendation) => (
                        <li key={recommendation}>{recommendation}</li>
                      ))}
                    </ul>
                  ) : null}
                </section>

                <section className="metrics-grid" aria-label="Overview metrics">
                  <article className="metric-card">
                    <p>Total Tables</p>
                    <strong>{dashboardData.totalTables.toLocaleString()}</strong>
                    <span>{analysisStats.tables ? `+${analysisStats.tables}` : 'Schema indexed'}</span>
                  </article>
                  <article className="metric-card">
                    <p>Total Columns</p>
                    <strong>{dashboardData.totalColumns.toLocaleString()}</strong>
                    <span>{analysisStats.columns ? `+${analysisStats.columns}` : 'Columns mapped'}</span>
                  </article>
                  <article className="metric-card">
                    <p>Quality Score</p>
                    <strong>{formatPercent(dashboardData.qualityScore)}</strong>
                    <span>{dashboardData.topPriorities} high-priority table(s)</span>
                  </article>
                  <article className="metric-card warning">
                    <p>Sensitive Fields</p>
                    <strong>{dashboardData.sensitiveFields.toLocaleString()}</strong>
                    <span>Review masking policy</span>
                  </article>
                </section>

                <section className="table-panel">
                  <header className="table-panel-head">
                    <h2>Table Explorer</h2>
                  </header>

                  <div className="table-shell">
                    <table>
                      <thead>
                        <tr>
                          <th>Name</th>
                          <th>Schema</th>
                          <th>Rows</th>
                          <th>Last Updated</th>
                          <th>Quality</th>
                          <th>Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {dashboardData.tableExplorer.length ? (
                          dashboardData.tableExplorer.map((table) => (
                            <tr key={table.name}>
                              <td>
                                <span className="row-name">
                                  <span className="row-dot" />
                                  {table.name}
                                </span>
                              </td>
                              <td>{table.schema}</td>
                              <td>{formatCompactNumber(table.rows)}</td>
                              <td>{table.lastUpdated}</td>
                              <td>
                                <div className="quality-track" aria-label={`Quality ${formatPercent(table.quality)}`}>
                                  <span
                                    className={`quality-fill ${table.status.toLowerCase()}`}
                                    style={{ width: `${Math.max(8, Math.round(table.quality))}%` }}
                                  />
                                </div>
                              </td>
                              <td>
                                <span className={`status-pill ${table.status.toLowerCase()}`}>{table.status}</span>
                              </td>
                            </tr>
                          ))
                        ) : (
                          <tr>
                            <td colSpan="6" className="empty-table-row">
                              No table insights are available yet.
                            </td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  </div>

                  <footer className="table-footer">
                    <span>
                      Showing all {dashboardData.tableExplorer.length.toLocaleString()} table(s)
                    </span>
                  </footer>
                </section>
              </>
            ) : activeTab === 'dictionary' ? (
              <>
                <section className="dictionary-overview">
                  <article className="dictionary-stat">
                    <span>Total Tables</span>
                    <strong>{schemaDictionary.length.toLocaleString()}</strong>
                  </article>
                  <article className="dictionary-stat">
                    <span>Total Columns</span>
                    <strong>
                      {schemaDictionary
                        .reduce(
                          (sum, table) => sum + (Array.isArray(table?.columns) ? table.columns.length : 0),
                          0
                        )
                        .toLocaleString()}
                    </strong>
                  </article>
                  <article className="dictionary-stat">
                    <span>Relations</span>
                    <strong>
                      {schemaDictionary
                        .reduce(
                          (sum, table) => sum + (Array.isArray(table?.foreign_keys) ? table.foreign_keys.length : 0),
                          0
                        )
                        .toLocaleString()}
                    </strong>
                  </article>
                </section>

                <section className="dictionary-layout">
                  <aside className="dictionary-list-panel">
                    <div className="dictionary-search">
                      <span className="search-icon">
                        <IconSearch />
                      </span>
                      <input
                        type="text"
                        placeholder="Search table..."
                        value={dictionaryQuery}
                        onChange={(event) => setDictionaryQuery(event.target.value)}
                      />
                    </div>
                    <p className="dictionary-count">
                      {dictionaryTables.length.toLocaleString()} table(s)
                    </p>

                    <div className="dictionary-list">
                      {dictionaryTables.length ? (
                        dictionaryTables.map((table) => (
                          <button
                            type="button"
                            key={table.table_name}
                            className={`dictionary-table-item ${
                              selectedDictionaryTable?.table_name === table.table_name ? 'active' : ''
                            }`}
                            onClick={() => setSelectedTableName(table.table_name)}
                          >
                            <span>{table.table_name}</span>
                            <small>{Array.isArray(table?.columns) ? table.columns.length : 0} cols</small>
                          </button>
                        ))
                      ) : (
                        <p className="dictionary-empty">No tables match your search.</p>
                      )}
                    </div>
                  </aside>

                  <article className="dictionary-detail-panel">
                    {selectedDictionaryTable ? (
                      <>
                        <header className="dictionary-detail-head">
                          <div>
                            <h2>{selectedDictionaryTable.table_name}</h2>
                            <p>
                              {Array.isArray(selectedDictionaryTable?.columns)
                                ? selectedDictionaryTable.columns.length
                                : 0}{' '}
                              columns
                            </p>
                          </div>
                          <div className="dictionary-tags">
                            <span>
                              PK: {Array.isArray(selectedDictionaryTable?.primary_keys) ? selectedDictionaryTable.primary_keys.length : 0}
                            </span>
                            <span>
                              FK: {Array.isArray(selectedDictionaryTable?.foreign_keys) ? selectedDictionaryTable.foreign_keys.length : 0}
                            </span>
                          </div>
                        </header>

                        <div className="dictionary-columns-shell">
                          <table>
                            <thead>
                              <tr>
                                <th>Column</th>
                                <th>Type</th>
                                <th>Nullable</th>
                                <th>Default</th>
                              </tr>
                            </thead>
                            <tbody>
                              {Array.isArray(selectedDictionaryTable?.columns) &&
                              selectedDictionaryTable.columns.length ? (
                                selectedDictionaryTable.columns.map((column) => (
                                  <tr key={column.name}>
                                    <td>{column.name}</td>
                                    <td>{column.type}</td>
                                    <td>{column.nullable ? 'Yes' : 'No'}</td>
                                    <td>{column.default && column.default !== 'None' ? column.default : '-'}</td>
                                  </tr>
                                ))
                              ) : (
                                <tr>
                                  <td colSpan="4" className="empty-table-row">
                                    No columns found for this table.
                                  </td>
                                </tr>
                              )}
                            </tbody>
                          </table>
                        </div>

                        <div className="dictionary-relations">
                          <h3>Foreign Key Relations</h3>
                          {Array.isArray(selectedDictionaryTable?.foreign_keys) &&
                          selectedDictionaryTable.foreign_keys.length ? (
                            <ul>
                              {selectedDictionaryTable.foreign_keys.map((foreignKey, index) => (
                                <li key={`${selectedDictionaryTable.table_name}-fk-${index}`}>
                                  <strong>{Array.isArray(foreignKey?.column) ? foreignKey.column.join(', ') : '-'}</strong>
                                  {' → '}
                                  {foreignKey?.referred_table || '-'}
                                  {Array.isArray(foreignKey?.referred_columns) &&
                                  foreignKey.referred_columns.length
                                    ? ` (${foreignKey.referred_columns.join(', ')})`
                                    : ''}
                                </li>
                              ))}
                            </ul>
                          ) : (
                            <p className="dictionary-no-relations">
                              No foreign key relationships defined for this table.
                            </p>
                          )}
                        </div>
                      </>
                    ) : (
                      <p className="dictionary-empty">No schema data available.</p>
                    )}
                  </article>
                </section>
              </>
            ) : activeTab === 'profiling' ? (
              <>
                <section className="profiling-hero">
                  <article className="profiling-card profiling-ring-card">
                    <p className="profiling-card-kicker">Average Completeness</p>
                    <div
                      className="profiling-ring"
                      style={{
                        background: `conic-gradient(#49d4a1 ${
                          Math.max(0, Math.min(100, profilingInsights.averageCompleteness))
                        }%, rgba(100, 122, 160, 0.2) 0)`,
                      }}
                    >
                      <div className="profiling-ring-inner">
                        <strong>{formatPercent(profilingInsights.averageCompleteness)}</strong>
                        <span>across all tables</span>
                      </div>
                    </div>
                  </article>

                  <article className="profiling-card">
                    <header className="profiling-card-head">
                      <h3>Key Health</h3>
                      <span>{profilingInsights.totalTables.toLocaleString()} tables</span>
                    </header>
                    <div className="profiling-stack">
                      <span
                        className="profiling-stack-fill healthy"
                        style={{ width: `${Math.max(4, profilingInsights.healthiestPct)}%` }}
                      />
                      <span
                        className="profiling-stack-fill warning"
                        style={{
                          width: `${Math.max(0, 100 - Math.max(4, profilingInsights.healthiestPct))}%`,
                        }}
                      />
                    </div>
                    <div className="profiling-legend">
                      <p>
                        <span className="dot healthy" />
                        Healthy: {profilingInsights.healthyTables.toLocaleString()}
                      </p>
                      <p>
                        <span className="dot warning" />
                        Needs review: {profilingInsights.warningTables.toLocaleString()}
                      </p>
                    </div>
                  </article>

                  <article className="profiling-card">
                    <header className="profiling-card-head">
                      <h3>Freshness Distribution</h3>
                      <span>staleness by table</span>
                    </header>
                    <div className="freshness-list">
                      {[
                        { key: 'fresh', label: '0-7 days', color: 'fresh' },
                        { key: 'monitor', label: '8-30 days', color: 'monitor' },
                        { key: 'stale', label: '31+ days', color: 'stale' },
                        { key: 'noTimestamp', label: 'No timestamp', color: 'none' },
                      ].map((bucket) => {
                        const count = profilingInsights.freshnessBuckets[bucket.key] || 0
                        const width = profilingInsights.totalTables
                          ? (count / profilingInsights.totalTables) * 100
                          : 0
                        return (
                          <div className="freshness-row" key={bucket.key}>
                            <div className="freshness-meta">
                              <span>{bucket.label}</span>
                              <strong>{count.toLocaleString()}</strong>
                            </div>
                            <div className="freshness-track">
                              <span
                                className={`freshness-fill ${bucket.color}`}
                                style={{ width: `${Math.max(count ? 8 : 0, width)}%` }}
                              />
                            </div>
                          </div>
                        )
                      })}
                    </div>
                  </article>
                </section>

                <section className="profiling-grid">
                  <article className="profiling-card">
                    <header className="profiling-card-head">
                      <h3>Table Quality Heat</h3>
                      <span>completeness + key integrity</span>
                    </header>
                    <div className="quality-bars">
                      {profilingInsights.qualityLeaders.length ? (
                        profilingInsights.qualityLeaders.map((table) => (
                          <div className="quality-bar-row" key={table.table_name}>
                            <div className="quality-bar-meta">
                              <span>{table.table_name}</span>
                              <strong>{formatPercent(table.score)}</strong>
                            </div>
                            <div className="quality-track wide">
                              <span
                                className={`quality-fill ${computeTableStatus(table.score).toLowerCase()}`}
                                style={{ width: `${Math.max(6, Math.round(table.score))}%` }}
                              />
                            </div>
                          </div>
                        ))
                      ) : (
                        <p className="dictionary-empty">No profiling rows found.</p>
                      )}
                    </div>
                  </article>

                  <article className="profiling-card">
                    <header className="profiling-card-head">
                      <h3>Null Risk Columns</h3>
                      <span>highest missing-value concentration</span>
                    </header>
                    <div className="null-risk-list">
                      {profilingInsights.nullRiskLeaders.length ? (
                        profilingInsights.nullRiskLeaders.map((column) => (
                          <div
                            className="null-risk-row"
                            key={`${column.table_name}-${column.column_name}`}
                          >
                            <div className="null-risk-meta">
                              <p>
                                {column.table_name}.{column.column_name}
                              </p>
                              <strong>{formatPercent(column.null_pct)} null</strong>
                            </div>
                            <div className="freshness-track">
                              <span
                                className="freshness-fill stale"
                                style={{ width: `${Math.max(7, Math.round(column.null_pct))}%` }}
                              />
                            </div>
                          </div>
                        ))
                      ) : (
                        <p className="dictionary-empty">No null-heavy columns detected.</p>
                      )}
                    </div>
                  </article>
                </section>
              </>
            ) : activeTab === 'ai_analysis' ? (
              <>
                <section className="ai-overview-grid">
                  <article className="ai-panel ai-summary">
                    <header className="ai-panel-head">
                      <h2>AI Business Summary</h2>
                      <span>
                        {docInsights.generatedAt
                          ? new Date(docInsights.generatedAt).toLocaleString()
                          : 'Unknown generated time'}
                      </span>
                    </header>
                    <p>
                      {docInsights.summary ||
                        'No AI summary found. Generate document data first.'}
                    </p>
                    <div className="ai-model-badge">
                      Model: {docInsights.model || 'unknown'}
                    </div>
                  </article>

                  <article className="ai-panel">
                    <header className="ai-panel-head">
                      <h3>Global Recommendations</h3>
                      <span>{docInsights.recommendations.length} item(s)</span>
                    </header>
                    {docInsights.recommendations.length ? (
                      <ul className="ai-recommendations">
                        {docInsights.recommendations.slice(0, 5).map((recommendation) => (
                          <li key={recommendation}>{recommendation}</li>
                        ))}
                      </ul>
                    ) : (
                      <p className="dictionary-empty">No recommendations available.</p>
                    )}
                  </article>
                </section>

                <section className="ai-overview-grid">
                  <article className="ai-panel">
                    <header className="ai-panel-head">
                      <h3>Priority Distribution</h3>
                      <span>{docInsights.tables.length.toLocaleString()} tables analyzed</span>
                    </header>
                    {[
                      { key: 'high', label: 'High Priority', color: 'high' },
                      { key: 'medium', label: 'Medium Priority', color: 'medium' },
                      { key: 'low', label: 'Low Priority', color: 'low' },
                      { key: 'unknown', label: 'Unclassified', color: 'unknown' },
                    ].map((item) => {
                      const count = docInsights.priorityCount[item.key] || 0
                      const width = docInsights.tables.length
                        ? (count / docInsights.tables.length) * 100
                        : 0
                      return (
                        <div className="ai-priority-row" key={item.key}>
                          <div className="ai-priority-meta">
                            <span>{item.label}</span>
                            <strong>{count.toLocaleString()}</strong>
                          </div>
                          <div className="ai-priority-track">
                            <span
                              className={`ai-priority-fill ${item.color}`}
                              style={{ width: `${Math.max(count ? 8 : 0, width)}%` }}
                            />
                          </div>
                        </div>
                      )
                    })}
                  </article>

                  <article className="ai-panel">
                    <header className="ai-panel-head">
                      <h3>Table Insights</h3>
                      <span>{filteredDocTables.length.toLocaleString()} visible</span>
                    </header>
                    <div className="dictionary-search ai-table-search">
                      <span className="search-icon">
                        <IconSearch />
                      </span>
                      <input
                        type="text"
                        placeholder="Search table insights..."
                        value={docSearch}
                        onChange={(event) => setDocSearch(event.target.value)}
                      />
                    </div>
                  </article>
                </section>

                <section className="ai-table-grid">
                  {filteredDocTables.length ? (
                    filteredDocTables.map((table) => (
                      <article className="ai-table-card" key={table.table_name}>
                        <header className="ai-table-head">
                          <h4>{table.table_name}</h4>
                          <span className={`ai-priority-pill ${String(table.priority || 'unknown').toLowerCase()}`}>
                            {String(table.priority || 'unknown').toUpperCase()}
                          </span>
                        </header>

                        <p className="ai-table-summary">
                          {table.business_summary || 'No business summary available.'}
                        </p>

                        <div className="ai-table-section">
                          <h5>Usage Recommendations</h5>
                          {Array.isArray(table.usage_recommendations) &&
                          table.usage_recommendations.length ? (
                            <ul>
                              {table.usage_recommendations.slice(0, 3).map((item) => (
                                <li key={`${table.table_name}-usage-${item}`}>{item}</li>
                              ))}
                            </ul>
                          ) : (
                            <p className="dictionary-empty">No usage notes.</p>
                          )}
                        </div>

                        <div className="ai-table-section">
                          <h5>Data Quality Observations</h5>
                          {Array.isArray(table.data_quality_observations) &&
                          table.data_quality_observations.length ? (
                            <ul>
                              {table.data_quality_observations.slice(0, 3).map((item) => (
                                <li key={`${table.table_name}-dq-${item}`}>{item}</li>
                              ))}
                            </ul>
                          ) : (
                            <p className="dictionary-empty">No quality observations.</p>
                          )}
                        </div>

                        {Array.isArray(table.suggested_kpis) && table.suggested_kpis.length ? (
                          <div className="ai-kpi-wrap">
                            {table.suggested_kpis.slice(0, 4).map((kpi) => (
                              <span key={`${table.table_name}-kpi-${kpi}`}>{kpi}</span>
                            ))}
                          </div>
                        ) : null}
                      </article>
                    ))
                  ) : (
                    <p className="dictionary-empty">No AI table insights match your search.</p>
                  )}
                </section>
              </>
            ) : (
              <>
                <section className="schema-viz-layout">
                  <article className="schema-viz-canvas-panel">
                    <header className="schema-viz-head">
                      <h2>Relationship Graph</h2>
                      <div className="schema-viz-head-right">
                        <span>
                          {schemaViz.nodes.length.toLocaleString()} tables •{' '}
                          {schemaViz.edges.length.toLocaleString()} links
                        </span>
                        <div className="schema-viz-controls">
                          <button
                            type="button"
                            onClick={() => zoomSchemaViz(0.9)}
                            aria-label="Zoom out"
                          >
                            −
                          </button>
                          <strong>{Math.round(vizViewport.scale * 100)}%</strong>
                          <button
                            type="button"
                            onClick={() => zoomSchemaViz(1.12)}
                            aria-label="Zoom in"
                          >
                            +
                          </button>
                          <button
                            type="button"
                            className="reset"
                            onClick={resetSchemaVizView}
                          >
                            Reset
                          </button>
                        </div>
                      </div>
                    </header>

                    <div
                      className={`schema-viz-canvas-wrap ${
                        isVizDragging ? 'dragging' : 'draggable'
                      }`}
                    >
                      {schemaViz.nodes.length ? (
                        <svg
                          className="schema-viz-canvas"
                          viewBox={`0 0 ${schemaViz.width} ${schemaViz.height}`}
                          role="img"
                          aria-label="Schema relationship graph"
                          onWheel={handleSchemaVizWheel}
                          onPointerDown={handleSchemaVizPointerDown}
                          onPointerMove={handleSchemaVizPointerMove}
                          onPointerUp={stopSchemaVizDrag}
                          onPointerLeave={stopSchemaVizDrag}
                          onPointerCancel={stopSchemaVizDrag}
                        >
                          <defs>
                            <linearGradient
                              id="schemaEdgeGradient"
                              x1="0%"
                              y1="0%"
                              x2="100%"
                              y2="100%"
                            >
                              <stop offset="0%" stopColor="#4f7cff" />
                              <stop offset="100%" stopColor="#52d1ff" />
                            </linearGradient>
                          </defs>

                          <g transform={`translate(${vizViewport.tx} ${vizViewport.ty})`}>
                            <g transform={`scale(${vizViewport.scale})`}>
                              {schemaViz.edges.map((edge, index) => {
                                const source = edge.sourceNode
                                const target = edge.targetNode
                                const controlX = (source.x + target.x) / 2
                                const controlY =
                                  (source.y + target.y) / 2 -
                                  Math.min(120, Math.abs(source.x - target.x) * 0.2 + 36)
                                const active =
                                  selectedVizNode &&
                                  (edge.source === selectedVizNode.id ||
                                    edge.target === selectedVizNode.id)
                                return (
                                  <path
                                    key={`${edge.source}-${edge.target}-${index}`}
                                    d={`M ${source.x} ${source.y} Q ${controlX} ${controlY} ${target.x} ${target.y}`}
                                    className={`schema-edge ${active ? 'active' : ''}`}
                                  />
                                )
                              })}

                              {schemaViz.nodes.map((node) => {
                                const selected = selectedVizNode?.id === node.id
                                return (
                                  <g
                                    key={node.id}
                                    className={`schema-node ${selected ? 'active' : ''}`}
                                    onClick={() => setSelectedVizTable(node.id)}
                                  >
                                    <circle cx={node.x} cy={node.y} r={selected ? 29 : 22} />
                                    <text x={node.x} y={node.y - 2} textAnchor="middle">
                                      {node.id.length > 16
                                        ? `${node.id.slice(0, 14)}..`
                                        : node.id}
                                    </text>
                                    <text
                                      x={node.x}
                                      y={node.y + 12}
                                      textAnchor="middle"
                                      className="schema-node-meta"
                                    >
                                      {node.columnCount} cols
                                    </text>
                                  </g>
                                )
                              })}
                            </g>
                          </g>
                        </svg>
                      ) : (
                        <p className="dictionary-empty">No schema tables available.</p>
                      )}
                    </div>
                  </article>

                  <aside className="schema-viz-detail-panel">
                    <header className="schema-viz-head">
                      <h3>
                        {selectedVizNode?.id || 'Select a table'}
                      </h3>
                      <span>Table Details</span>
                    </header>

                    {selectedVizNode ? (
                      <>
                        <div className="schema-viz-metrics">
                          <article>
                            <span>Columns</span>
                            <strong>{selectedVizNode.columnCount}</strong>
                          </article>
                          <article>
                            <span>Outgoing FK</span>
                            <strong>{selectedVizOutgoing.length}</strong>
                          </article>
                          <article>
                            <span>Incoming FK</span>
                            <strong>{selectedVizIncoming.length}</strong>
                          </article>
                        </div>

                        <div className="schema-viz-relations">
                          <h4>Outgoing Relationships</h4>
                          {selectedVizOutgoing.length ? (
                            <ul>
                              {selectedVizOutgoing.map((edge, index) => (
                                <li key={`out-${edge.source}-${edge.target}-${index}`}>
                                  <strong>{edge.source}</strong>
                                  {' → '}
                                  {edge.target}
                                  {edge.localColumns.length
                                    ? ` (${edge.localColumns.join(', ')})`
                                    : ''}
                                </li>
                              ))}
                            </ul>
                          ) : (
                            <p className="dictionary-empty">No outgoing relationships.</p>
                          )}
                        </div>

                        <div className="schema-viz-relations">
                          <h4>Incoming Relationships</h4>
                          {selectedVizIncoming.length ? (
                            <ul>
                              {selectedVizIncoming.map((edge, index) => (
                                <li key={`in-${edge.source}-${edge.target}-${index}`}>
                                  <strong>{edge.source}</strong>
                                  {' → '}
                                  {edge.target}
                                  {edge.referredColumns.length
                                    ? ` (${edge.referredColumns.join(', ')})`
                                    : ''}
                                </li>
                              ))}
                            </ul>
                          ) : (
                            <p className="dictionary-empty">No incoming relationships.</p>
                          )}
                        </div>

                        <div className="schema-viz-relations">
                          <h4>Most Connected Tables</h4>
                          <div className="schema-viz-leaders">
                            {schemaViz.leaders.length ? (
                              schemaViz.leaders.map((node) => (
                                <button
                                  type="button"
                                  key={`leader-${node.id}`}
                                  className={`schema-viz-leader ${
                                    selectedVizNode.id === node.id ? 'active' : ''
                                  }`}
                                  onClick={() => setSelectedVizTable(node.id)}
                                >
                                  <span>{node.id}</span>
                                  <div className="schema-viz-leader-bar">
                                    <span
                                      style={{
                                        width: `${
                                          schemaViz.leaders[0]?.relationCount
                                            ? Math.max(
                                                8,
                                                (node.relationCount /
                                                  schemaViz.leaders[0].relationCount) *
                                                  100
                                              )
                                            : 0
                                        }%`,
                                      }}
                                    />
                                  </div>
                                  <strong>{node.relationCount}</strong>
                                </button>
                              ))
                            ) : (
                              <p className="dictionary-empty">
                                No relationship leaders found.
                              </p>
                            )}
                          </div>
                        </div>
                      </>
                    ) : (
                      <p className="dictionary-empty">
                        No table selected for visualization.
                      </p>
                    )}
                  </aside>
                </section>
              </>
            )}
          </div>
        </section>

        <aside className="dashboard-chat-sidebar" aria-label="AI chat sidebar">
          <header className="chat-sidebar-head">
            <div>
              <p className="chat-kicker">DB AI Agent</p>
              <h3>Ask DataLens</h3>
            </div>
            <span className="chat-db-pill">{activeDatabase || 'No DB'}</span>
          </header>

          <div className="chat-thread" ref={chatThreadRef}>
            {chatMessages.map((item) => (
              <article key={item.id} className={`chat-bubble ${item.role}`}>
                {item.role === 'assistant' ? (
                  <div className="chat-markdown">
                    <ReactMarkdown remarkPlugins={[remarkGfm]}>{item.text}</ReactMarkdown>
                  </div>
                ) : (
                  <p>{item.text}</p>
                )}
                <span>{item.role === 'assistant' ? 'Agent' : 'You'}</span>
              </article>
            ))}

            {chatLoading ? (
              <article className="chat-bubble assistant loading">
                <p>Thinking...</p>
                <span>Agent</span>
              </article>
            ) : null}
          </div>

          {chatError ? <p className="chat-error">{chatError}</p> : null}

          <form
            className="chat-composer"
            onSubmit={(event) => {
              event.preventDefault()
              sendChatMessage()
            }}
          >
            <textarea
              value={chatInput}
              onChange={(event) => setChatInput(event.target.value)}
              placeholder="Ask about tables, columns, profiling, or read-only SQL..."
              rows={3}
              disabled={chatLoading || !activeDatabase}
            />
            <button
              type="submit"
              className="primary-btn chat-send-btn"
              disabled={chatLoading || !chatInput.trim() || !activeDatabase}
            >
              {chatLoading ? 'Sending...' : 'Send'}
            </button>
          </form>
        </aside>
      </main>
    )
  }

  if (screen === 'analysis') {
    return (
      <main className="connection-page">
        <div className="ambient ambient-top" />
        <div className="ambient ambient-bottom" />

        <section className="analysis-shell">
          <header className="analysis-header">
            <h1>Analyzing Your Database</h1>
            <p>
              Building a comprehensive intelligence map from your data assets. This may take a few
              moments.
            </p>
            <span className="analysis-db-tag">{activeDatabase}</span>
          </header>

          <div className="analysis-card">
            <div className="timeline">
              {STEP_CONFIG.map((step, index) => {
                const item = stepState[step.key]
                const statusValue = item?.status || 'pending'
                return (
                  <div className="timeline-row" key={step.key}>
                    <div className={`timeline-icon ${statusValue}`}>{renderStepIcon(statusValue)}</div>
                    <div className="timeline-content">
                      <p className="timeline-title">{step.title}</p>
                      <p className="timeline-subtitle">{item?.message || step.subtitle}</p>
                    </div>
                    <p className={`timeline-state ${statusValue}`}>{stepBadgeLabel(statusValue)}</p>
                    {index < STEP_CONFIG.length - 1 ? <span className="timeline-connector" /> : null}
                  </div>
                )
              })}
            </div>
          </div>

          <footer className="analysis-footer">
            <div className="analysis-stat">
              <span>Tables</span>
              <strong>{analysisStats.tables.toLocaleString()}</strong>
            </div>
            <div className="analysis-stat">
              <span>Columns</span>
              <strong>{analysisStats.columns.toLocaleString()}</strong>
            </div>
            <div className="analysis-stat">
              <span>Relations</span>
              <strong>{analysisStats.relations.toLocaleString()}</strong>
            </div>
          </footer>

          {status.message ? <p className={`form-status ${status.type} analysis-status`}>{status.message}</p> : null}

          <button
            type="button"
            className="secondary-btn back-btn"
            onClick={() => setScreen('connect')}
            disabled={Boolean(loadingAction)}
          >
            Back To Connection
          </button>
        </section>
      </main>
    )
  }

  return (
    <main className="connection-page">
      <div className="ambient ambient-top" />
      <div className="ambient ambient-bottom" />

      <section className="connection-card">
        <div className="connection-header">
          <div className="title-icon-wrap">
            <IconDatabase />
          </div>
          <h1>Connect Your Database</h1>
          <p>
            Securely link your data source to generate the AI dictionary. We&apos;ll analyze your
            schema to build intelligence.
          </p>
        </div>

        <div className="connection-form">
          <label className="field">
            <span className="field-label">Database Type</span>
            <div className="field-input select-wrap">
              <select value={form.db_type} onChange={(event) => handleProviderChange(event.target.value)}>
                <option value="">Select a provider...</option>
                {PROVIDERS.map((provider) => (
                  <option key={provider.value} value={provider.value}>
                    {provider.label}
                  </option>
                ))}
              </select>
            </div>
          </label>

          <div className="field-grid two">
            <label className="field">
              <span className="field-label">Host</span>
              <div className="field-input">
                <span className="input-icon">
                  <IconServer />
                </span>
                <input
                  value={form.host}
                  onChange={(event) => updateField('host', event.target.value)}
                  type="text"
                  placeholder="e.g., aws.amazon.com"
                />
              </div>
            </label>

            <label className="field">
              <span className="field-label">Port</span>
              <div className="field-input">
                <input
                  value={form.port}
                  onChange={(event) => updateField('port', event.target.value)}
                  type="text"
                  placeholder="5432"
                />
              </div>
            </label>
          </div>

          <label className="field">
            <span className="field-label">Database Name</span>
            <div className="field-input">
              <span className="input-icon">
                <IconDatabase />
              </span>
              <input
                value={form.database}
                onChange={(event) => updateField('database', event.target.value)}
                type="text"
                placeholder="production_db"
              />
            </div>
          </label>

          <div className="field-grid two">
            <label className="field">
              <span className="field-label">Username</span>
              <div className="field-input">
                <span className="input-icon">
                  <IconUser />
                </span>
                <input
                  value={form.username}
                  onChange={(event) => updateField('username', event.target.value)}
                  type="text"
                  placeholder="read_only_user"
                />
              </div>
            </label>

            <label className="field">
              <span className="field-label">Password</span>
              <div className="field-input">
                <span className="input-icon">
                  <IconLock />
                </span>
                <input
                  value={form.password}
                  onChange={(event) => updateField('password', event.target.value)}
                  type={showPassword ? 'text' : 'password'}
                  placeholder="••••••••••••"
                />
                <button
                  type="button"
                  className="ghost-eye"
                  onClick={() => setShowPassword((current) => !current)}
                  aria-label={showPassword ? 'Hide password' : 'Show password'}
                >
                  {showPassword ? 'Hide' : 'Show'}
                </button>
              </div>
            </label>
          </div>
        </div>

        <div className="connection-actions">
          <button
            type="button"
            className="secondary-btn"
            onClick={() => {
              loadSavedDatabases()
              setScreen('databases')
            }}
            disabled={Boolean(loadingAction)}
          >
            Saved Databases
          </button>
          <button
            type="button"
            className="secondary-btn"
            onClick={() => submitCredentials('test')}
            disabled={Boolean(loadingAction)}
          >
            <span className="btn-icon">
              <IconRadar />
            </span>
            {loadingAction === 'test' ? 'Testing...' : 'Test Connection'}
          </button>
          <button
            type="button"
            className="primary-btn"
            onClick={runAnalysis}
            disabled={Boolean(loadingAction)}
          >
            {loadingAction === 'start' ? 'Starting...' : 'Start Analysis'}
            <span className="btn-arrow">→</span>
          </button>
        </div>

        {status.message ? <p className={`form-status ${status.type}`}>{status.message}</p> : null}

        <div className="connection-footnote">
          <span className="footnote-icon">
            <IconShield />
          </span>
          Your credentials are encrypted using AES-256 standards. Read-only access recommended.
        </div>
      </section>
    </main>
  )
}

export default App
