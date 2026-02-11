SELECT
  'anchored_belonger' AS archetype_id,
  'Anchored Belonger' AS archetype_name,
  'Belonger' AS archetype_short_label,
  'Identity coherence and relational continuity' AS core_drive,
  'Desire for stability vs fear of misalignment' AS core_tension,
  ['Module 1', 'Module 6', 'Module 7'] AS dominant_modules,
  'Compounding' AS growth_behavior,
  'Over-serving can calcify the system and suppress adaptation' AS system_risk,
  'This archetype stabilizes the system but must not become the sole reference for growth decisions.' AS strategic_note
UNION ALL
SELECT
  'aspirational_seeker' AS archetype_id,
  'Aspirational Seeker' AS archetype_name,
  'Seeker' AS archetype_short_label,
  'Self-expansion through perceived future identity' AS core_drive,
  'Hopeful projection vs eventual disillusionment' AS core_tension,
  ['Module 1', 'Module 4', 'Module 5'] AS dominant_modules,
  'Destabilizing' AS growth_behavior,
  'Unchecked scaling amplifies expectation violations and regret' AS system_risk,
  'This archetype fuels momentum but requires disciplined expectation management.' AS strategic_note
UNION ALL
SELECT
  'value_skeptic' AS archetype_id,
  'Value Skeptic' AS archetype_name,
  'Skeptic' AS archetype_short_label,
  'Protection against asymmetric exchange' AS core_drive,
  'Need for proof vs resistance to commitment' AS core_tension,
  ['Module 2', 'Module 5', 'Module 6'] AS dominant_modules,
  'Neutral' AS growth_behavior,
  'Misreading skepticism as hostility leads to unnecessary friction' AS system_risk,
  'This archetype sharpens the system''s value clarity when engaged without pressure.' AS strategic_note
UNION ALL
SELECT
  'transactional_optimizer' AS archetype_id,
  'Transactional Optimizer' AS archetype_name,
  'Optimizer' AS archetype_short_label,
  'Maximizing personal efficiency within constraints' AS core_drive,
  'Utility extraction vs relational investment' AS core_tension,
  ['Module 2', 'Module 5', 'Module 8'] AS dominant_modules,
  'Extractive' AS growth_behavior,
  'Over-indexing erodes relational depth and long-term trust' AS system_risk,
  'This archetype reveals system efficiency limits but should not define the relationship core.' AS strategic_note
UNION ALL
SELECT
  'cautious_evaluator' AS archetype_id,
  'Cautious Evaluator' AS archetype_name,
  'Evaluator' AS archetype_short_label,
  'Risk minimization through understanding' AS core_drive,
  'Desire for certainty vs analysis paralysis' AS core_tension,
  ['Module 3', 'Module 6', 'Module 7'] AS dominant_modules,
  'Neutral' AS growth_behavior,
  'Impatience from the system can convert caution into withdrawal' AS system_risk,
  'This archetype rewards consistency and penalizes volatility.' AS strategic_note
UNION ALL
SELECT
  'emotionally_invested' AS archetype_id,
  'Emotionally Invested' AS archetype_name,
  'Invested' AS archetype_short_label,
  'Relational meaning and emotional reinforcement' AS core_drive,
  'Attachment vs vulnerability to disappointment' AS core_tension,
  ['Module 4', 'Module 6', 'Module 7'] AS dominant_modules,
  'Compounding' AS growth_behavior,
  'Misalignment produces outsized trust decay and regret' AS system_risk,
  'This archetype compounds trust rapidly but amplifies failures just as fast.' AS strategic_note
UNION ALL
SELECT
  'detached_explorer' AS archetype_id,
  'Detached Explorer' AS archetype_name,
  'Explorer' AS archetype_short_label,
  'Low-commitment discovery' AS core_drive,
  'Curiosity vs aversion to constraint' AS core_tension,
  ['Module 3', 'Module 5', 'Module 8'] AS dominant_modules,
  'Neutral' AS growth_behavior,
  'Forcing depth prematurely increases friction without return' AS system_risk,
  'This archetype tests the system''s openness without demanding reciprocity.' AS strategic_note
UNION ALL
SELECT
  'returning_reconsiderer' AS archetype_id,
  'Returning Reconsiderer' AS archetype_name,
  'Reconsiderer' AS archetype_short_label,
  'Resolution of past tension through re-evaluation' AS core_drive,
  'Residual regret vs renewed possibility' AS core_tension,
  ['Module 6', 'Module 7', 'Module 8'] AS dominant_modules,
  'Compounding' AS growth_behavior,
  'Ignoring historical context leads to repeat erosion cycles' AS system_risk,
  'This archetype encodes the system''s capacity for repair and learning.' AS strategic_note
