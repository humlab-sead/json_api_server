WITH limited_ae AS (
  SELECT ae.analysis_entity_id, ae.dataset_id, ae.physical_sample_id
  FROM tbl_analysis_entities ae
  ORDER BY ae.analysis_entity_id
  LIMIT 10
  OFFSET 0
)
SELECT
  -- Site
  s.site_id,
  s.site_name,

  -- Sample group
  sg.sample_group_id,
  sg.sample_group_name,

  -- Physical sample
  ps.physical_sample_id,
  ps.sample_name,

  -- Analysis entity (still your driving set)
  lae.analysis_entity_id,
  lae.dataset_id,

  -- Dataset
  d.master_set_id,
  d.data_type_id,
  d.method_id     AS dataset_method_id,
  d.biblio_id     AS dataset_biblio_id,
  d.project_id    AS dataset_project_id,
  d.dataset_name,

  -- Method
  m.method_id                         AS method_id,
  m.biblio_id                         AS method_biblio_id,
  m.date_updated                      AS method_date_updated,
  m.description                       AS method_description,
  m.method_abbrev_or_alt_name         AS method_abbrev_or_alt_name,
  m.method_group_id                   AS method_group_id,
  m.method_name                       AS method_name,
  m.record_type_id                    AS method_record_type_id,
  m.unit_id                           AS method_unit_id,

  -- === Per-abundance row grain starts here ===
  a.abundance_id,
  a.abundance,                         -- adjust if your column name differs
  a.abundance_element_id,
  abe.element_name            AS element_type,

  -- Taxon bits (readable pieces + a preformatted label)
  ttm.taxon_id,
  UPPER(ttf.family_name)      AS family,
  ttg.genus_name              AS genus,
  ttm.species                 AS species,
  CONCAT(
    UPPER(ttf.family_name), ', ',
    ttg.genus_name, ' ',
    ttm.species,
    CASE WHEN tta.author_name IS NOT NULL AND tta.author_name <> '' THEN ' ' || tta.author_name ELSE '' END
  )                           AS full_taxon,
  tto.order_name              AS order_name,
  tto.record_type_id          AS order_record_type_id,

  -- Per-abundance metadata (arrays in JSONB, but 1 row per abundance)
  ident.identification_levels,
  mods.modifications,
  cns.common_names,
  meas.measured_attributes,
  taxn.taxonomy_notes,
  tbio.text_biology,
  tdist.text_distribution,
  seas.seasonality

FROM limited_ae lae
JOIN tbl_physical_samples ps ON ps.physical_sample_id = lae.physical_sample_id
JOIN tbl_sample_groups   sg ON sg.sample_group_id     = ps.sample_group_id
JOIN tbl_sites           s  ON s.site_id              = sg.site_id
JOIN tbl_datasets        d  ON d.dataset_id           = lae.dataset_id
JOIN tbl_methods         m  ON m.method_id            = d.method_id

-- One row per abundance
LEFT JOIN tbl_abundances a
  ON a.analysis_entity_id = lae.analysis_entity_id

-- Element type
LEFT JOIN tbl_abundance_elements abe
  ON abe.abundance_element_id = a.abundance_element_id

-- Taxon lineage
LEFT JOIN tbl_taxa_tree_master   ttm ON ttm.taxon_id  = a.taxon_id
LEFT JOIN tbl_taxa_tree_genera   ttg ON ttg.genus_id  = ttm.genus_id
LEFT JOIN tbl_taxa_tree_families ttf ON ttf.family_id = ttg.family_id
LEFT JOIN tbl_taxa_tree_orders   tto ON tto.order_id  = ttf.order_id
LEFT JOIN tbl_taxa_tree_authors  tta ON tta.author_id = ttm.author_id

-- === LATERAL subqueries so we don't multiply rows ===

-- Identification levels (joined to ident level names per your reference)
LEFT JOIN LATERAL (
  SELECT COALESCE(jsonb_agg(
           jsonb_build_object(
             'abundance_id',               ail.abundance_id,
             'identification_level_id',    ail.identification_level_id,
             'identification_level_abbrev',il.identification_level_abbrev,
             'identification_level_name',  il.identification_level_name,
             'notes',                      il.notes
           )
         ), '[]'::jsonb) AS identification_levels
  FROM tbl_abundance_ident_levels ail
  LEFT JOIN tbl_identification_levels il
    ON il.identification_level_id = ail.identification_level_id
  WHERE ail.abundance_id = a.abundance_id
) ident ON TRUE

-- Abundance modifications (+ type)
LEFT JOIN LATERAL (
  SELECT COALESCE(jsonb_agg(
           jsonb_build_object(
             'abundance_id',         am.abundance_id,
             'modification_type_id', am.modification_type_id,
             'modification_type',    mt.modification_type_name
           )
         ), '[]'::jsonb) AS modifications
  FROM tbl_abundance_modifications am
  LEFT JOIN tbl_modification_types mt
    ON mt.modification_type_id = am.modification_type_id
  WHERE am.abundance_id = a.abundance_id
) mods ON TRUE

-- Common names (+ language)
LEFT JOIN LATERAL (
  SELECT COALESCE(jsonb_agg(
           to_jsonb(tcn) || jsonb_build_object('language', to_jsonb(lang))
         ), '[]'::jsonb) AS common_names
  FROM tbl_taxa_common_names tcn
  LEFT JOIN tbl_languages lang
    ON lang.language_id = tcn.language_id
  WHERE tcn.taxon_id = a.taxon_id
) cns ON TRUE

-- Measured attributes
LEFT JOIN LATERAL (
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'measured_attribute_id', tma.measured_attribute_id,
           'attribute_measure',     tma.attribute_measure,
           'attribute_type',        tma.attribute_type,
           'attribute_units',       tma.attribute_units,
           'data',                  tma.data
         )), '[]'::jsonb) AS measured_attributes
  FROM tbl_taxa_measured_attributes tma
  WHERE tma.taxon_id = a.taxon_id
) meas ON TRUE

-- Taxonomy notes
LEFT JOIN LATERAL (
  SELECT COALESCE(jsonb_agg(to_jsonb(ttn)), '[]'::jsonb) AS taxonomy_notes
  FROM tbl_taxonomy_notes ttn
  WHERE ttn.taxon_id = a.taxon_id
) taxn ON TRUE

-- Biology text
LEFT JOIN LATERAL (
  SELECT COALESCE(jsonb_agg(to_jsonb(tb)), '[]'::jsonb) AS text_biology
  FROM tbl_text_biology tb
  WHERE tb.taxon_id = a.taxon_id
) tbio ON TRUE

-- Distribution text
LEFT JOIN LATERAL (
  SELECT COALESCE(jsonb_agg(to_jsonb(td)), '[]'::jsonb) AS text_distribution
  FROM tbl_text_distribution td
  WHERE td.taxon_id = a.taxon_id
) tdist ON TRUE

-- Seasonality (+ season / activity type like your reference)
LEFT JOIN LATERAL (
  SELECT COALESCE(jsonb_agg(
           to_jsonb(ts)
           || jsonb_build_object(
                'season',        to_jsonb(se),
                'activity_type', to_jsonb(at)
              )
         ), '[]'::jsonb) AS seasonality
  FROM tbl_taxa_seasonality ts
  LEFT JOIN tbl_seasons se
    ON se.season_id = ts.season_id
  LEFT JOIN tbl_activity_types at
    ON at.activity_type_id = ts.activity_type_id
  WHERE ts.taxon_id = a.taxon_id
) seas ON TRUE

-- Order your flat result however you like:
ORDER BY s.site_id, sg.sample_group_id, ps.physical_sample_id, lae.analysis_entity_id, a.abundance_id;
