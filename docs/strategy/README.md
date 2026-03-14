# Strategy Docs — Reytech RFQ

Reference these before every sprint and every major decision.

## Files

### INTELLIGENCE_ROADMAP.md
- Full phase status (what's done, what's next)
- Data architecture (tables, frequencies, health thresholds)
- Pricing oracle design (confidence scoring, formula)
- Business context (goals, constraints, timeline)
- Future state scaffolding (states, federal, white-label)

### PHASE_2_PROMPT.md
- Pre-flight checklist (verify before sending to CC)
- Full Phase 2 Claude Code prompt
- Definition of done
- Phase 3 preview

### THINKING_PRINCIPLES.md
- The 3 questions to ask before every decision
- Data principles
- Architecture principles
- Intelligence loop diagram
- Pull health contract
- Most expensive lessons learned

## When to reference each

| Situation | Reference |
|-----------|-----------|
| Starting a new sprint | INTELLIGENCE_ROADMAP.md + THINKING_PRINCIPLES.md |
| Writing a CC prompt | PHASE_X_PROMPT.md + THINKING_PRINCIPLES.md |
| Architecture decision | THINKING_PRINCIPLES.md |
| Data question | INTELLIGENCE_ROADMAP.md |
| Phase complete — what's next | INTELLIGENCE_ROADMAP.md |
| Something feels wrong | THINKING_PRINCIPLES.md |

## Update policy
- After every phase: update INTELLIGENCE_ROADMAP.md phase status
- After every lesson learned: update tasks/lessons.md  
- After every architectural decision: update THINKING_PRINCIPLES.md
- Phase prompts: create PHASE_3_PROMPT.md etc. as phases complete
