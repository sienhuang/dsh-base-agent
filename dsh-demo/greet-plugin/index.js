import Schema from '@deepseek-ai/schemastery'
import { defineTool } from '@deepseek-ai/dsh-tools'

export const name = 'greet-tool'
export const inject = ['tools']

export const Config = Schema.object({
template: Schema.string().default('Hello, {name}!'),
})

export function apply(ctx, config) {
ctx.tools.register(defineTool({
    name: 'greet',
    description: 'Greet a person by name.',

    parameters: {
    name: {
        type: 'string',
        required: true,
        description: 'The name of the person to greet',
    },
    },

    output: {
    schema: {
        type: 'string',
    },
    render: (_args, value) => [
        {
        type: 'text',
        text: value,
        },
    ],
    },

    async execute(args) {
    return config.template.replaceAll('{name}', args.name)
    },
}))
}