/**
 * Pinned SDK-runtime extension for deterministic, host-started Workflows.
 *
 * DSH 0.1.2-alpha.3 exports WorkflowEngine.start(), but its SDK JSON-RPC
 * dispatcher has no public extension router. TypeScript `private` compiles to
 * an ordinary method in this pinned release, so this adapter wraps the server
 * dispatcher and uses getOrCreateSession() to obtain the required parent Agent.
 */

import { createHash } from 'node:crypto'

import {
  HarnessSdkJsonRpcServer,
} from '@deepseek-ai/dsh-sdk-jsonrpc-server'

export const name = 'base-agent-direct-workflow-rpc'
export const inject = ['workflowEngine']

const METHOD = 'base-agent/workflow.execute'

function object(value, label) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`)
  }
  return value
}

function string(value, label) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${label} must be a non-empty string`)
  }
  return value
}

function sha256(value) {
  return createHash('sha256').update(value, 'utf8').digest('hex')
}

export function apply(ctx) {
  const prototype = HarnessSdkJsonRpcServer.prototype

  ctx.effect(() => {
    const original = prototype.handleRequest

    async function extendedHandleRequest(method, params) {
      if (method !== METHOD) return original.call(this, method, params)

      const request = object(params, 'params')
      const sessionId = string(request.sessionId, 'params.sessionId')
      const script = string(request.script, 'params.script')
      const expectedScriptSha256 = string(
        request.scriptSha256,
        'params.scriptSha256',
      )
      const meta = object(request.meta, 'params.meta')
      string(meta.name, 'params.meta.name')
      string(meta.description, 'params.meta.description')
      const argsJson = string(request.argsJson, 'params.argsJson')
      const expectedArgsSha256 = string(
        request.argsSha256,
        'params.argsSha256',
      )
      if (sha256(argsJson) !== expectedArgsSha256) {
        throw new Error('Workflow args sha256 does not match the approved value')
      }
      const args = object(JSON.parse(argsJson), 'parsed params.argsJson')
      if (!Array.isArray(args.steps) || args.steps.length === 0) {
        throw new TypeError('params.args.steps must be a non-empty array')
      }

      if (sha256(script) !== expectedScriptSha256) {
        throw new Error('Workflow script sha256 does not match the approved value')
      }

      // Explicit compatibility pin to dsh-sdk-jsonrpc-server 0.1.2-alpha.3.
      const getOrCreateSession = this.getOrCreateSession
      if (typeof getOrCreateSession !== 'function') {
        throw new Error(
          'incompatible DSH SDK runtime: getOrCreateSession is unavailable',
        )
      }
      const session = await getOrCreateSession.call(this, sessionId)
      const workflowRequest = {
        parent: session.handle.agent,
        script,
        meta,
        args,
        maxTotalAgents: args.steps.length,
      }

      const run = ctx.workflowEngine.start(workflowRequest)
      try {
        const result = await run.result
        return {
          runId: String(run.id),
          stopReason: result.stopReason,
          agentsStarted: result.agentsStarted,
          value: result.value,
          ...(result.error === undefined ? {} : { error: result.error }),
        }
      } finally {
        await run.dispose()
      }
    }

    prototype.handleRequest = extendedHandleRequest
    return () => {
      if (prototype.handleRequest === extendedHandleRequest) {
        prototype.handleRequest = original
      }
    }
  }, 'base-agent-direct-workflow-rpc.install')
}
