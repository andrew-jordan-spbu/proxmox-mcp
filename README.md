# proxmox-mcp

MCP-сервер для Proxmox VE: даёт LLM-агенту (Claude Desktop, Claude Code и другим MCP-клиентам) читать состояние кластера и управлять контейнерами и виртуальными машинами через REST API Proxmox.

Один файл на Python, работает по stdio. Зависимости — `mcp` и `httpx`.

## Возможности

**Чтение состояния**
- `cluster_overview` — узлы, кворум, загрузка CPU/RAM, гости
- `list_guests`, `guest_details` — контейнеры и ВМ, их конфигурация и потребление ресурсов
- `node_status`, `list_storage` — состояние узла и хранилищ
- `list_templates`, `list_isos` — шаблоны LXC и образы ISO
- `list_tasks`, `task_status` — задачи Proxmox и их журналы
- `list_snapshots`

**Управление**
- `guest_power` — запуск, остановка, перезагрузка
- `create_container`, `update_container_config`, `resize_container_disk`, `download_template`
- `create_vm`, `update_vm_config`
- `create_snapshot`, `backup_guest`
- `wake_node` — включить выключенный узел по Wake-on-LAN через соседний узел
- `container_exec` — выполнить команду внутри контейнера (`pct exec` по SSH), если задан `PROXMOX_SSH_HOSTS`

**Разрушительные операции** — `delete_container`, `delete_vm`, `rollback_snapshot`, `delete_snapshot` — помечены в MCP как destructive. Удаление требует повторить имя гостя (`confirm_hostname` / `confirm_name`), чтобы агент не удалил не тот контейнер по ошибке в ID.

**Прочее**
- Режим только для чтения: `PROXMOX_READ_ONLY=true` — регистрируются только инструменты чтения.
- Отказоустойчивость: можно указать несколько узлов кластера, сервер обратится к первому доступному.
- Длинные задачи (создание, бэкап, удаление) сервер ждёт до 60 секунд: при ошибке возвращает хвост журнала задачи, а если она ещё идёт — её UPID для `task_status`.

## Установка

```bash
uv run proxmox_mcp.py
```

`uv` сам поставит зависимости из шапки скрипта. Без `uv`:

```bash
pip install "mcp>=1.10" "httpx>=0.27"
python proxmox_mcp.py
```

## API-токен Proxmox

Создайте отдельного пользователя и токен с минимально нужной ролью. Для просмотра достаточно `PVEAuditor`:

```bash
pveum user add claude@pve
pveum user token add claude@pve mcp --privsep 0
pveum acl modify / --users claude@pve --roles PVEAuditor
```

Для управления контейнерами и ВМ выдайте более широкую роль (например, `PVEVMAdmin` и права на хранилища) — только если это действительно нужно.

## Переменные окружения

| Переменная | Описание |
|---|---|
| `PROXMOX_HOST` | Обязательна. Узел или список узлов через запятую, например `pve1.lan,192.168.1.11`. Порт 8006 и https подставляются автоматически |
| `PROXMOX_TOKEN_ID` | Обязательна. `user@realm!tokenname`, например `claude@pve!mcp` |
| `PROXMOX_TOKEN_SECRET` | Обязательна. Секрет токена (UUID) |
| `PROXMOX_VERIFY_SSL` | `true`, чтобы проверять сертификат. По умолчанию `false` (у Proxmox самоподписанный) |
| `PROXMOX_READ_ONLY` | `true` — только инструменты чтения |
| `PROXMOX_SSH_HOSTS` | Необязательна. Включает `container_exec`. Формат: `node1=root@192.168.1.10,node2=root@192.168.1.11`; несколько адресов для узла — через `\|`. Нужен вход по SSH-ключу |

## Подключение к Claude Desktop

`claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "proxmox": {
      "command": "uv",
      "args": ["run", "/path/to/proxmox_mcp.py"],
      "env": {
        "PROXMOX_HOST": "pve1.lan",
        "PROXMOX_TOKEN_ID": "claude@pve!mcp",
        "PROXMOX_TOKEN_SECRET": "<секрет токена>",
        "PROXMOX_READ_ONLY": "true"
      }
    }
  }
}
```

## Безопасность

- Секрет токена хранится только в конфигурации клиента — не коммитьте её.
- Начните с `PROXMOX_READ_ONLY=true` и роли `PVEAuditor`; права на запись выдавайте осознанно.
- `container_exec` выполняет команды от root внутри контейнера — включайте его, только если доверяете агенту и клиенту.
