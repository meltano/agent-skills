# Meltano agent skills

Agent skills for building and maintaining [Meltano](https://meltano.com) and [Singer](https://www.singer.io) connectors, packaged as a Claude Code plugin marketplace. Each skill is a plain [Agent Skills](https://agentskills.io) folder (`SKILL.md` plus references), so other agents can use them too.

## Install (Claude Code)

In a session:

```
/plugin marketplace add meltano/agent-skills
/plugin install singer@meltano
```

From a terminal, in one command:

```
claude plugin install singer --marketplace meltano/agent-skills
```

## Skills

| Plugin | Skill | Use it to |
|---|---|---|
| `singer` | [`arrow-batch-tap`](plugins/singer/skills/arrow-batch-tap/SKILL.md) | Add Arrow BATCH support to a Singer tap, and benchmark it in melt-bench |

## Layout

```
.claude-plugin/marketplace.json     # lists the plugins
plugins/<plugin>/
  .claude-plugin/plugin.json
  skills/<skill>/SKILL.md           # steps; references/ holds material only some branches need
```

Keep each skill grounded in shipped code: link the reference implementations, and update the skill in the same change that alters the pattern.
