<!--
 Copyright (c) 2024 Snowflake Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 -->

# `snow dcm init`

`snow dcm init` bootstraps a [DCM Project](https://docs.snowflake.com/en/user-guide/data-catalog-management)
so you can start deploying with `snow dcm plan` / `snow dcm deploy`. In a single
run it:

- writes (or updates) a `manifest.yml`,
- scaffolds a `sources/definitions/raw.sql` placeholder for a brand-new project,
- creates the DCM Project **object** in Snowflake, and
- provisions any missing supporting objects (database, schema, warehouse).

It is designed to be safe: before touching anything it prints a summary of every
action it will take and asks for a single confirmation. **Nothing is created if
you decline.**


## Use-cases

### 1. Start a new project

Use this when you are creating a DCM Project from scratch. Pass a project name
(or pick one interactively) and init creates a subfolder to hold the project:

```bash
snow dcm init --project-name my_project -c my_connection
```

```text
./my_project/
├── manifest.yml
└── sources/
    └── definitions/
        └── raw.sql        # placeholder — replace with your object definitions
```

The new `manifest.yml` is populated with your account identifier, project owner,
and a first target.

### 2. Add a target to an existing manifest

Use this to add another environment (for example a `dev`, `qa`, or `prod`
target) to a project you already have. Run init from a directory that already
contains a `manifest.yml` and **omit** `--project-name`:

```bash
cd path/to/my_project
snow dcm init --target prod -c my_connection
```

A new target block is appended to the existing `manifest.yml`; your
`default_target` and your `sources/` definitions are left untouched and reused.

When run interactively without flags in a directory that has a `manifest.yml`,
init first asks which of these two paths you want.

## What init resolves for you

During a run, init fills in everything a target needs — using sensible defaults,
prompting when it cannot, and never writing a placeholder:

- **Target name** — defaults to your current account's alias; re-prompts if the
  name is invalid or already used in the manifest.
- **DCM Project object name** — defaults to the target name, or you can enter
  another. Identifiers with special characters are automatically wrapped in
  double quotes so they stay valid instead of failing.
- **Database and schema** — if the object name is not fully qualified, init uses
  your connection's defaults (or prompts you when there are none), and **creates
  them if they don't exist**.
- **Project owner** — uses your current role, or prompts for one if it cannot be
  determined.
- **Warehouse** — uses your connection's warehouse, or provisions an X-Small
  `DCM_WH` (or a name you choose) and tells you how to configure it.

## Options

| Option | Description |
| --- | --- |
| `--project-name <name>` | Create a new project in a subfolder of this name. Omit to add a target to an existing `manifest.yml` in the current directory. |
| `--target <name>` | Name of the target to create in the manifest. Defaults to the account alias. **Required with `--force`.** |
| `--project-identifier <id>` | Identifier of the DCM Project object in Snowflake (e.g. `MY_DB.MY_SCHEMA.MY_PROJECT`). Defaults to the target name; unqualified names use the connection's database/schema. |
| `--if-not-exists` | Do nothing if the DCM Project object already exists in Snowflake. |
| `--force` | Approve all changes non-interactively (for automation/CI). Requires `--target`; if that target already exists it is reused as-is rather than failing. |
| `--interactive` / `--no-interactive` | Control whether init prompts. Interactive is the default in a terminal. |

## Interactive vs. non-interactive

- **Interactive (default in a terminal).** Init walks you through the choices
  above and shows the confirmation summary before making any change.
- **Non-interactive (`--force`).** Init approves every change without prompting,
  so all inputs must be resolvable up front. Pass `--target` (required), and
  either a fully-qualified `--project-identifier` or a connection whose default
  database and schema exist:

  ```bash
  snow dcm init \
    --project-name my_project \
    --target dev \
    --project-identifier MY_DB.MY_SCHEMA.MY_PROJECT \
    --force \
    -c my_connection
  ```

---

# `snow dcm init` with an existing project template

This guide covers the case where you **already have a project template** in a
repo folder — a `manifest.yml` and your SQL object definitions under
`sources/definitions/` — and you want to register the DCM Project in Snowflake
and deploy it.

## Prerequisites

- Snowflake CLI installed (`snow --version`).
- A repo folder that already contains:

  ```text
  my-project/
  ├── manifest.yml
  └── sources/
      └── definitions/
          └── ... your .sql files ...
  ```

## Steps

### 1. Navigate to your project folder

```bash
cd path/to/my-project
```

### 2. Set up your Snowflake connection

Create (or reuse) a named connection, then set it as the default:

```bash
snow connection add
snow connection test -c my_connection
```

You can pass `-c my_connection` to every command below, or set it as the
default connection so it is used automatically.

### 3. Initialize the DCM Project

```bash
snow dcm init -c my_connection
```

`snow dcm init` first asks **where** to work. Because the current directory
already contains a `manifest.yml`, it offers to **add a new target** to it
(leaving your existing `default_target` unchanged and reusing your definition
files). Choose _no_ to instead create a brand-new project in a subfolder — you
give it a name, and a fresh `manifest.yml` and `sources/definitions/raw.sql`
placeholder are created inside `./<name>/`.

It then walks you through:

1. **The target name** — defaults to your current account's alias; it
   re-prompts if the name is invalid or already taken.
2. **The DCM Project object name** — defaults to the target name, or enter
   another. Special characters are automatically wrapped in double quotes so the
   identifier stays valid instead of failing.
3. **The database and schema** — if the object name is not fully qualified, it
   confirms your connection's defaults or lets you enter your own; missing ones
   are created.

Finally it creates the DCM Project object in Snowflake.

Before making any change, init prints a summary of everything it will do —
create a warehouse, create the database/schema, edit your manifest — and asks
you to confirm once. **Nothing is created if you decline.**

> Run in a terminal (or pass `--interactive`) so you can approve the summary.
> For automation, pass `--force` to approve all changes non-interactively — this
> requires `--target`, and if that target already exists it is reused as-is
> rather than failing.

During init it will also:

- Create the target database and schema if they don't exist (part of the
  confirmation summary — a missing one is often a typo).
- Prompt you for the database/schema if your connection has no defaults (common
  on a fresh account) and you gave an unqualified name.
- Prompt you for the role that will own the DCM Project if the current role
  cannot be determined (it never writes a placeholder `project_owner`).
- Ensure a warehouse is available: it uses your connection's warehouse, or, if
  there is none, provisions an X-Small `DCM_WH` (or a name you choose) and tells
  you how to configure it.

> If init provisioned a warehouse for you, configure it before `plan`/`deploy`:
> add `warehouse = "<name>"` to your connection in `config.toml`, or pass
> `--warehouse <name>` to those commands.

### 4. Review the plan

Preview the changes DCM will make to match your definition files, without
applying them:

```bash
snow dcm plan -c my_connection
```

### 5. Deploy

Apply the changes to Snowflake:

```bash
snow dcm deploy -c my_connection
```

## Summary

```bash
cd path/to/my-project
snow connection test -c my_connection
snow dcm init --interactive -c my_connection   # add a target, or start a new project
snow dcm plan -c my_connection                 # review changes
snow dcm deploy -c my_connection               # apply changes
```
