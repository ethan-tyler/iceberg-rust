# Claude Code Project Instructions

## Project Overview

This is the **iceberg-rust** project - a Rust implementation of Apache Iceberg, an open table format for large analytic datasets.

### Key Crates

| Crate | Path | Purpose |
|-------|------|---------|
| `iceberg` | `crates/iceberg/` | Core Iceberg implementation |
| `iceberg-datafusion` | `crates/integrations/datafusion/` | DataFusion integration |
| `iceberg-catalog-*` | `crates/catalog/` | Catalog implementations |

---

## Documentation Discovery

### When Researching or Exploring the Codebase

**ALWAYS check for existing documentation before implementing or modifying features.**

#### 1. Check the `docs/` Folder First

```
docs/
├── datafusion-update/          # UPDATE operation docs
│   ├── README.md               # Index and quick reference
│   ├── DESIGN.md               # Technical architecture
│   ├── SPRINT-SUMMARY.md       # Sprint execution history
│   ├── CHANGELOG.md            # Version changes
│   └── adr/                    # Architecture Decision Records
│       ├── ADR-001-*.md
│       ├── ADR-002-*.md
│       └── ...
└── [future-feature]/           # Other feature docs
```

#### 2. Documentation Lookup Commands

When working on a feature, run these to find relevant docs:

```bash
# Find all documentation for a feature area
find docs/ -name "*.md" | xargs grep -l "UPDATE\|update" 2>/dev/null

# Find ADRs related to a topic
find docs/ -path "*/adr/*.md" | xargs grep -l "partition\|evolution" 2>/dev/null

# Check for design docs
ls docs/*/DESIGN.md 2>/dev/null
```

#### 3. Documentation Priority Order

When researching a problem, check in this order:

1. **ADRs** (`docs/*/adr/`) - Understand WHY decisions were made
2. **DESIGN.md** - Understand HOW the feature works
3. **SPRINT-SUMMARY.md** - Understand WHAT was built and when
4. **CHANGELOG.md** - Understand what changed recently
5. **Source code comments** - Implementation details

#### 4. Related Documentation Signals

Look for these patterns in code that indicate documentation exists:

```rust
//! # Feature Name
//!
//! See [DESIGN.md](../../docs/feature-name/DESIGN.md) for architecture.
//! See [ADR-001](../../docs/feature-name/adr/ADR-001-*.md) for decision rationale.
```

---

## Documentation Creation Framework

### When to Create Documentation

Create documentation when:
- Implementing a new feature (multi-sprint effort)
- Making architectural decisions
- Changing existing behavior significantly
- Adding new integration points

### Documentation Structure Template

For any significant feature, create this structure:

```
docs/{feature-name}/
├── README.md               # Required: Index and overview
├── DESIGN.md               # Required: Technical design
├── SPRINT-SUMMARY.md       # Required for multi-sprint work
├── CHANGELOG.md            # Required: Track changes
└── adr/                    # Required for decisions
    └── ADR-NNN-{title}.md
```

---

## Document Templates

### README.md Template

```markdown
# {Feature Name}

## Summary
One-paragraph description of what this feature does.

## Documentation Index

| Document | Description |
|----------|-------------|
| [DESIGN.md](./DESIGN.md) | Technical architecture |
| [ADR-001-*.md](./adr/ADR-001-*.md) | {Decision title} |

## Quick Links

- **Source Code**: `crates/{path}/`
- **Tests**: `crates/{path}/tests/`

## Current Status

| Feature | Status | Notes |
|---------|--------|-------|
| Core functionality | Complete | |
| Edge case X | Partial | See ADR-002 |

## Usage

\`\`\`rust
// Example code
\`\`\`
```

### DESIGN.md Template

```markdown
# {Feature} Technical Design

## Summary
What this feature does and who it affects.

## Problem Statement

### Before
What existed before this work.

### Pain Points Solved
- Pain point 1
- Pain point 2

## Solution Overview

### Architecture

\`\`\`
[ASCII diagram or description]
\`\`\`

### Data Flow
1. Step 1
2. Step 2
3. Step 3

## Technical Details

### Key Components

| Component | File | Purpose |
|-----------|------|---------|
| `ComponentA` | `path/to/file.rs` | Does X |

### API Reference

\`\`\`rust
// Key function signatures
\`\`\`

## Configuration & Deployment

- Environment variables
- Feature flags
- Dependencies

## Testing & Validation

| Category | Count | Description |
|----------|-------|-------------|
| Unit | N | Component tests |
| Integration | N | End-to-end tests |

## Risks & Trade-offs

### Known Limitations
1. Limitation 1 - Impact and mitigation
2. Limitation 2 - Impact and mitigation

### Performance Characteristics
- Complexity: O(?)
- Best case: X
- Worst case: Y

## How to Extend

### Adding Feature X
1. Step 1
2. Step 2

### Anti-patterns to Avoid
- Don't do X because Y

## References
- [External doc](url)
- Related ADRs
```

### ADR Template

```markdown
# ADR-{NNN}: {Title}

## Status
**{Proposed | Accepted | Deprecated | Superseded}** - {Date or Sprint}

## Context
What is the issue that we're seeing that is motivating this decision?

## Decision
What is the change that we're proposing and/or doing?

### Rationale
1. Reason 1
2. Reason 2

## Alternatives Considered

### Option A: {Name}
**Approach**: Description

**Pros**:
- Pro 1

**Cons**:
- Con 1

### Option B: {Name}
...

## Consequences

### Positive
- Benefit 1
- Benefit 2

### Negative
- Drawback 1 - Mitigation
- Drawback 2 - Mitigation

## References
- [Related code](path/to/file.rs)
- [External resource](url)
```

### SPRINT-SUMMARY.md Template

```markdown
# {Feature} Sprint Summary

## Overview

| Sprint | Focus | Status | Dates |
|--------|-------|--------|-------|
| Sprint 1 | Core | Complete | YYYY-MM |
| Sprint 2 | Tests | Complete | YYYY-MM |

---

## Sprint N: {Title}

### Objective
What this sprint aimed to accomplish.

### Deliverables

| Component | File | Description |
|-----------|------|-------------|
| `Component` | `path/file.rs` | What it does |

### Key Decisions
- ADR-001: Decision made

### Issues Addressed

| Issue | Severity | Resolution |
|-------|----------|------------|
| Bug X | High | Fixed by Y |

### Test Results
\`\`\`
test result: ok. N passed; 0 failed
\`\`\`

---

## Metrics Summary

| Metric | Value |
|--------|-------|
| Files Added | N |
| Tests Added | N |
| Lines Changed | ~N |

## Lessons Learned

### What Worked Well
1. Thing 1

### What Could Improve
1. Thing 1

### Technical Debt

| Item | Priority | Notes |
|------|----------|-------|
| Debt 1 | High | Details |
```

### CHANGELOG.md Template

```markdown
# {Feature} Changelog

## [{Version/Sprint}] - YYYY-MM-DD

### Added
- New feature X

### Changed
- Modified behavior Y

### Fixed
- Bug Z

### Security
- Security fix A

### Deprecated
- Old API B
```

---

## Code Documentation Standards

### Module-Level Documentation

Every significant module should include:

```rust
//! # Module Name
//!
//! Brief description of what this module does.
//!
//! ## Architecture
//!
//! See [DESIGN.md](../../docs/feature/DESIGN.md) for full architecture.
//!
//! ## Usage
//!
//! \`\`\`rust,ignore
//! // Example usage
//! \`\`\`
//!
//! ## Limitations
//!
//! - Limitation 1
//! - Limitation 2 (see [ADR-001](../../docs/feature/adr/ADR-001.md))
```

### Function Documentation

```rust
/// Brief one-line description.
///
/// Longer description if needed, explaining the "why" not just the "what".
///
/// # Arguments
///
/// * `arg1` - Description
/// * `arg2` - Description
///
/// # Returns
///
/// Description of return value.
///
/// # Errors
///
/// * `ErrorType` - When this happens
///
/// # Example
///
/// \`\`\`rust,ignore
/// let result = function(arg1, arg2)?;
/// \`\`\`
pub fn function(arg1: Type, arg2: Type) -> Result<ReturnType> {
    // Implementation
}
```

---

## Workflow Integration

### Before Starting Work

1. Check `docs/` for existing documentation on the feature area
2. Read relevant ADRs to understand past decisions
3. Review DESIGN.md for architecture context

### During Development

1. Update CHANGELOG.md with changes as you make them
2. Create ADRs for significant decisions
3. Keep SPRINT-SUMMARY.md current

### After Completing Work

1. Update DESIGN.md if architecture changed
2. Finalize SPRINT-SUMMARY.md with metrics
3. Verify all ADRs are in accepted/implemented state
4. Cross-link documentation with source code

---

## Quick Reference

### Find Documentation

```bash
# All docs for a feature
ls docs/{feature-name}/

# All ADRs
find docs/ -path "*/adr/*.md"

# Search docs for a term
grep -r "term" docs/
```

### Create New Feature Docs

```bash
# Create structure
mkdir -p docs/{feature-name}/adr

# Create required files
touch docs/{feature-name}/{README,DESIGN,SPRINT-SUMMARY,CHANGELOG}.md
touch docs/{feature-name}/adr/ADR-001-{decision}.md
```

### Documentation Checklist

- [ ] README.md with index and status
- [ ] DESIGN.md with architecture
- [ ] At least one ADR for key decisions
- [ ] CHANGELOG.md tracking changes
- [ ] SPRINT-SUMMARY.md for multi-sprint work
- [ ] Source code cross-references docs
- [ ] Tests reference relevant docs in comments
