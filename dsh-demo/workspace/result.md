# DeepSeek Harness Python SDK 概念解释

- **workspace**：当前的工作目录。SDK 的所有文件操作（读写文件、创建结果文件等）默认都在这个目录下进行。本例中即 `/Users/huangsien/PycharmProjects/dsh-agent/dsh-demo/workspace`。

- **dsh_home**：DeepSeek Harness 的主目录（home 目录）。它是 SDK 的"根"位置，用于存放配置文件、日志、凭据等与 harness 自身相关的数据，区别于存放用户项目文件的 workspace。harness 运行时的全局状态和配置都从这里读取或写入。

- **session_id**：会话标识符。每次与 harness 建立的会话（session）都有唯一的 session_id，用于标识和追踪一次独立的对话/任务执行过程，使得多条命令和多次交互都能归属到同一个会话上下文中，也便于日志关联与回溯。
