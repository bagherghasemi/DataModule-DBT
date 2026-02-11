________________________________________
EMAKIE Data Lab
Future Enrichment Playbook
How to Safely Activate Intelligence Over Time
________________________________________
Why This Document Exists
This system was deliberately built in phases:
Meaning → Logic → Structure → Signal → Enrichment
Not:
Idea → SQL → Dashboard
As a result:
•	Many models exist before they are populated
•	Many fields are intentionally NULL
•	Many constructs are latent, not missing
This document explains when, why, and how to enrich the system without breaking it.
________________________________________
Core Principle (Non-Negotiable)
Never enrich meaning before evidence exists.
Enrichment must always be:
•	Longitudinal
•	Additive
•	Reversible
•	Grounded in observed change
If you feel the urge to:
•	“Just estimate”
•	“Fill something in”
•	“Use a proxy for now”
Stop.
That instinct is how intelligence systems decay.
________________________________________
What “Enrichment” Actually Means
Enrichment is not:
•	Adding new columns
•	Redesigning models
•	Changing grain
•	Reinterpreting semantics
Enrichment is:
•	Populating existing fields
•	Using time-aware logic
•	Letting behavior accumulate
•	Respecting original intent
Think of the system as:
An instrument panel installed before flight.
The instruments turn on only when the flight happens.
________________________________________
Required Preconditions for Enrichment
You may consider enriching a model only if all of the following are true:
1. Temporal Depth Exists
•	≥ 3–6 months of consistent data
•	Multiple dbt rebuild cycles
•	Observable variance, not just growth
2. Structural Stability Exists
•	Upstream models are stable
•	No recent schema changes
•	No changing definitions of cohorts, identity, or stages
3. Signal Exists (Not Just Volume)
•	Values fluctuate
•	Patterns persist
•	Change correlates across models
If any of these are missing → Do not enrich.
________________________________________
Enrichment Readiness by Module
Module 1 — Identity & Reality Gap
Enrich when:
•	Population sizes change across stages
•	Identity distributions differ between exposure and loyalty
•	Belief attrition curves show decay or stabilization
Safe enrichment targets:
•	identity_entropy
•	behavioral_coherence
•	transition_probability
•	composition_delta
•	leakage_intensity
Never enrich using:
•	Single-period behavior
•	Campaign performance alone
•	Demographics as identity
________________________________________
Module 2 — Intent vs Value
Enrich when:
•	Desire and value diverge over time
•	Yield stability curves show patterns
•	Reciprocity changes longitudinally
Safe enrichment targets:
•	Desire–value imbalance metrics
•	Yield volatility
•	Reciprocity differentials
Never equate:
•	Value with revenue
•	Intent with clicks
•	Yield with ROAS
________________________________________
Module 3 — Creative → Psychological Truth
Enrich when:
•	Creatives recur
•	Recruitment signatures stabilize
•	Narrative concordance varies across cohorts
Safe enrichment targets:
•	Promise intensities
•	Promise coherence
•	Narrative yield
Never infer:
•	Psychology from copy alone
•	Promise from creative intent
•	Meaning from format
________________________________________
Module 4 — Motivation Psychology
This is the most sensitive module.
Enrich only when:
•	Motivation vectors evolve over time
•	Emotional payoff curves show shape (not noise)
•	Identity transitions exist
•	Tension patterns persist
Safe enrichment targets:
•	motivation_stability_index
•	motivation_volatility
•	drift_sensitivity
•	Emotional payoff dimensions
•	Tension resolution effectiveness
Absolutely forbidden:
•	Inferring emotion
•	Interpreting payoff as happiness
•	Treating stability as loyalty
•	Using single events
If unsure → do nothing.
________________________________________
Module 5 — Price Meaning
Enrich when:
•	Pricing varies meaningfully
•	Discount behavior stabilizes
•	Economic dissonance repeats
Safe enrichment targets:
•	Price meaning vectors
•	Risk encoding signatures
•	Erosion curves
Never assume:
•	Cheap = exploitative
•	Expensive = premium
•	Discount = manipulation
________________________________________
Module 6 — Trust & Friction
Enrich when:
•	Friction appears across multiple touchpoints
•	Trust decay or repair patterns exist
•	Regret signals accumulate
Trust cannot be inferred early.
Wait.
________________________________________
Module 7 — Relationship Shape
Enrich when:
•	Relationship phases repeat
•	Drift and return are observable
•	Attachment velocity stabilizes
Never shortcut lifecycle thinking.
________________________________________
Module 8 — Acquisition Quality
Enrich when:
•	Acquisition sources persist
•	Toxicity patterns emerge
•	Growth modes repeat
This module evaluates what kind of growth you are creating, not how much.
________________________________________
Archetypes (Final Layer)
Archetypes must never be rushed.
Enrich only when:
•	Patterns persist across time
•	Cohorts migrate between types
•	Archetypes predict future behavior
Minimum requirements:
•	Persistence window defined
•	Minimum cohort count enforced
•	Extinct archetypes retained historically
Archetypes are discovered, not designed.
________________________________________
Enrichment Process (Step-by-Step)
When enriching any model:
1.	Re-read the module README
2.	Re-read the logical data model
3.	Confirm enrichment readiness criteria
4.	Write logic outside dbt first (notes)
5.	Add logic incrementally
6.	Validate with:
o	Temporal sanity checks
o	Rebuild consistency
7.	Document what changed and why
If you cannot explain the logic in plain language → don’t implement it.
________________________________________
Golden Rule of the System
If the system is unsure, it must remain silent.
NULL is preferable to false knowledge.
This is what makes EMAKIE Data Lab:
•	Trustworthy
•	Durable
•	Strategically dangerous (in a good way)
________________________________________
Final Reminder to Future You
If you ever feel:
•	“This seems overkill”
•	“We can simplify this”
•	“Other teams don’t do this”
Remember:
They build analytics.
You built understanding.
________________________________________

