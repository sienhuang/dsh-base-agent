import crypto from 'node:crypto'

export const name = 'dsh-base-agent-memory-context'
export const inject = ['agents']

function deepFreeze(value) {
  if (value !== null && typeof value === 'object' && !Object.isFrozen(value)) {
    Object.freeze(value)
    for (const child of Object.values(value)) deepFreeze(child)
  }
  return value
}

function userMessage(text) {
  return deepFreeze({
    id: crypto.randomUUID(),
    role: 'user',
    content: [{ type: 'text', text }],
    source: { kind: 'plugin', plugin: name },
  })
}

export function apply(ctx, config) {
  if (typeof config?.url !== 'string' || config.url.length === 0) {
    throw new TypeError('memory context url is required')
  }
  if (typeof config?.token !== 'string' || config.token.length === 0) {
    throw new TypeError('memory context token is required')
  }
  const maxContextBytes = config.maxContextBytes ?? 16384
  const timeoutMs = config.timeoutMs ?? 10000
  if (!Number.isInteger(maxContextBytes) || maxContextBytes < 1024) {
    throw new TypeError('maxContextBytes must be an integer of at least 1024')
  }
  if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) {
    throw new TypeError('timeoutMs must be a positive integer')
  }

  ctx.on('agent/pre-step', async ({ agent, step, signal }, next) => {
    const decision = await next()
    // A DSH Turn may contain later Tool-continuation steps. Memory is retrieved
    // once for the initial user step so the same payload is not repeatedly added.
    if (
      decision.kind !== 'enter'
      || signal.aborted
      || step !== 1
      || decision.messages.length === 0
    ) {
      return decision
    }
    try {
      const response = await fetch(config.url, {
        method: 'POST',
        headers: {
          authorization: `Bearer ${config.token}`,
          'content-type': 'application/json',
        },
        body: JSON.stringify({ session_id: agent.session.header.id }),
        signal: AbortSignal.any([signal, AbortSignal.timeout(timeoutMs)]),
      })
      if (!response.ok) {
        ctx.logger.warn('memory pre-step returned HTTP %d', response.status)
        return decision
      }
      const payload = await response.json()
      if (payload.context === null) return decision
      if (typeof payload.context !== 'string') {
        ctx.logger.warn('memory pre-step returned an invalid context payload')
        return decision
      }
      if (new TextEncoder().encode(payload.context).byteLength > maxContextBytes) {
        ctx.logger.warn('memory pre-step context exceeded its configured byte budget')
        return decision
      }
      return {
        ...decision,
        messages: [...decision.messages, userMessage(payload.context)],
      }
    } catch (error) {
      signal.throwIfAborted()
      ctx.logger.warn('memory pre-step retrieval failed: %s', String(error))
      return decision
    }
  }, { prepend: true })
}
