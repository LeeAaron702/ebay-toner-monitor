# Mistakes.md Fixes — Design Doc

**Date:** 2026-02-23
**Scope:** All 8 issues from mistakes.md

## Issues

### Quick Fixes
1. **Remove "Note: None"** — Hide the note line in Telegram messages when `notes` is empty/None
2. **Remove leading spaces in Canon "Singles"** — Strip indentation from mixed lot singles formatting
3. **Show Best Offer status** — Display whether listing accepts Best Offer in Telegram alerts

### Medium Fixes
4. **Front-end search without brand** — Product search should work without selecting a brand first
5. **Inconsistent messaging formats** — Standardize Telegram message format across Canon/Xerox/Lexmark engines
6. **Canon Sets profit display** — Show individual profit for each set option (4-color set, 3-color CMY set), not an aggregate

### Larger Features
7. **BSR dual display** — Show `BSR: [current] | [30d avg]` instead of just one value
8. **Lexmark lot parsing** — Detect quantity indicators in titles like "2 qty", "lot of 3"

## Approach

**Phase 1 — Parallel Investigation (Opus subagents):**
Launch 8 Opus subagents simultaneously. Each investigates the relevant files, identifies exact code locations, and returns a fix plan with file:line references.

**Phase 2 — Sequential Implementation (Sonnet subagents):**
Apply fixes in order 1→2→3→4→5→6→7→8 (smallest first). Each fix gets its own commit.

## Ordering Rationale

Small formatting fixes first to reduce blast radius. Canon-related fixes (2, 5, 6) are grouped but spread across the sequence so each builds on a stable base. BSR and Lexmark lot parsing are last since they involve the most logic changes.
